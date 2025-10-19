# streamlit_app.py

import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import pydeck as pdk
from typing import List, Dict, Any
import json

session = get_active_session()
st.set_page_config(page_title="NYC Pothole Risk", layout="wide")
st.title("NYC Pothole Risk — Predictions & Hotspots (14-day horizon)")

# ----------------- helpers -----------------
def lc(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.lower() for c in df.columns]
    return df

def pyify_number(x):
    if x is None:
        return None
    try:
        xi = int(x); xf = float(x)
        return xi if xi == xf else xf
    except Exception:
        try:
            return float(x)
        except Exception:
            return None

def color_from_prob_rg(p: float, alpha: int) -> List[int]:
    p = 0.0 if p is None else max(0.0, min(1.0, float(p)))
    r = int(round(255 * p))
    g = int(round(255 * (1 - p)))
    b = 60
    a = max(10, min(255, int(alpha)))
    return [r, g, b, a]

def color_from_outcome(actual, alpha: int) -> List[int]:
    a = max(10, min(255, int(alpha)))
    if actual is None:
        return [120, 120, 120, a]
    try:
        return [220, 50, 50, a] if int(actual) == 1 else [160, 160, 160, a]
    except Exception:
        return [120, 120, 120, a]

def to_scatter_records(df: pd.DataFrame, color_mode: str) -> List[Dict[str, Any]]:
    """Pure-Python records for ScatterplotLayer (circles)."""
    recs: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        lon = pyify_number(row.get("lon"))
        lat = pyify_number(row.get("lat"))
        if lon is None or lat is None:
            continue
        prob = pyify_number(row.get("probability"))
        actual = pyify_number(row.get("actual_label"))
        color = color_from_outcome(actual, 180) if color_mode == "Outcome (Actual)" else color_from_prob_rg(prob, 180)
        recs.append({
            "lon": float(lon),
            "lat": float(lat),
            "borough": None if pd.isna(row.get("borough")) else str(row.get("borough")),
            "probability": None if prob is None else float(prob),
            "actual_label": None if actual is None else int(actual),
            "h3_cell": None if row.get("h3_cell") is None else str(row.get("h3_cell")),
            "asof_date": None if row.get("asof_date") is None else str(row.get("asof_date")),
            # ~60–220 m radius for visibility (approx area)
            "radius": int(60 + (0.0 if prob is None else float(prob)) * 160),
            "color": color,
        })
    return recs

def fetch_hex_geojson_for_cells(h3_cells: List[str]) -> pd.DataFrame:
    """Get GeoJSON polygon for each distinct H3 cell via Snowflake H3_CELL_TO_BOUNDARY()."""
    if not h3_cells:
        return pd.DataFrame(columns=["h3_cell","geomjson"])
    uniq = list(dict.fromkeys([c for c in h3_cells if c]))
    values_rows = ", ".join([f"('{c}')" for c in uniq])
    sql = f"""
      with cells(h3_cell) as (
        select column1 as h3_cell from values {values_rows}
      )
      select c.h3_cell,
             st_asgeojson(h3_cell_to_boundary(c.h3_cell)) as geomjson
      from cells c
    """
    df = session.sql(sql).to_pandas()
    return lc(df)

def to_polygon_records(df: pd.DataFrame, color_mode: str, hex_alpha: int, top_n: int) -> List[Dict[str, Any]]:
    """
    Build polygon records for PolygonLayer using Snowflake boundaries.
    Caps to top_n by probability to avoid blanket & perf issues.
    """
    if df.empty:
        return []

    df2 = df.copy()
    if "probability" in df2.columns:
        df2 = df2.sort_values("probability", ascending=False).head(top_n)

    cells = df2["h3_cell"].dropna().astype(str).unique().tolist()
    geo = fetch_hex_geojson_for_cells(cells)
    if geo.empty:
        return []

    poly_map: Dict[str, List[List[float]]] = {}
    for _, row in geo.iterrows():
        cell = str(row.get("h3_cell"))
        gj = row.get("geomjson")
        try:
            g = json.loads(gj) if isinstance(gj, str) else gj
            ring = (g.get("coordinates", [[]])[0]) if g else []
            ring_list = []
            for pt in ring:
                if isinstance(pt, list) and len(pt) >= 2:
                    ring_list.append([float(pt[0]), float(pt[1])])
            if ring_list:
                poly_map[cell] = ring_list
        except Exception:
            continue

    recs: List[Dict[str, Any]] = []
    for _, row in df2.iterrows():
        cell = str(row.get("h3_cell"))
        polygon = poly_map.get(cell)
        if not polygon:
            continue
        prob = pyify_number(row.get("probability"))
        actual = pyify_number(row.get("actual_label"))
        fill = color_from_outcome(actual, hex_alpha) if color_mode == "Outcome (Actual)" else color_from_prob_rg(prob, hex_alpha)
        lon = pyify_number(row.get("lon"))
        lat = pyify_number(row.get("lat"))
        recs.append({
            "polygon": polygon,
            "fill_color": fill,
            "line_color": [40,40,40,200],
            "borough": None if pd.isna(row.get("borough")) else str(row.get("borough")),
            "probability": None if prob is None else float(prob),
            "actual_label": None if actual is None else int(actual),
            "h3_cell": cell,
            "asof_date": None if row.get("asof_date") is None else str(row.get("asof_date")),
            "lon": None if lon is None else float(lon),
            "lat": None if lat is None else float(lat),
        })
    return recs

# ----------------- bounds / guards -----------------
bounds = session.sql("""
  select to_date(min(asof_day)) as min_d, to_date(max(asof_day)) as max_d
  from CITYDW.GOLD.POTHOLE_PREDICTIONS
""").to_pandas()
if bounds.empty or bounds.iloc[0].isnull().any():
    st.warning("No rows found in CITYDW.GOLD.POTHOLE_PREDICTIONS.")
    st.stop()

min_d = pd.to_datetime(bounds.iloc[0]['MIN_D'])
max_d = pd.to_datetime(bounds.iloc[0]['MAX_D'])

# ----------------- sidebar -----------------
st.sidebar.subheader("Filters")
date_range = st.sidebar.date_input("Date range", (min_d, max_d), min_value=min_d, max_value=max_d)

# Try enriched view for boroughs
try:
    boro_df = session.sql("""
        select distinct borough
        from CITYDW.GOLD.VW_POTHOLE_PREDICTIONS_ENRICHED
        where borough is not null
        order by borough
    """).to_pandas()
    boro_df = lc(boro_df)
    borough_choices = ["All"] + boro_df["borough"].dropna().tolist()
    have_enriched = True
except Exception:
    borough_choices = ["All"]
    have_enriched = False

sel_boroughs = st.sidebar.multiselect("Boroughs", borough_choices, default=["All"])
mode = st.sidebar.radio("Hotspot mode", ["Top 5% per day", "By threshold"])
threshold = None
if mode == "By threshold":
    threshold = st.sidebar.slider("Probability threshold", 0.0, 1.0, 0.70, 0.01)
color_mode = st.sidebar.radio("Color by", ["Probability", "Outcome (Actual)"])
shape_mode = st.sidebar.radio("Geometry", ["Circles (approx area)", "Exact H3 Hexagons (precision)"])
hex_alpha = st.sidebar.slider("Hex fill opacity", 20, 180, 70)
hex_top_n = st.sidebar.slider("Max hexes to draw", 100, 5000, 1000, step=100)

date_from = pd.to_datetime(date_range[0]).date()
date_to   = pd.to_datetime(date_range[1]).date()

# ----------------- data builders -----------------
def fetch_hotspots(mode: str, threshold: float | None, date_from, date_to, borough_filter: str) -> pd.DataFrame:
    if have_enriched:
        if mode == "Top 5% per day":
            sql = f"""
              with ranked as (
                select
                  asof_day::date as asof_date, h3_cell, borough, probability, actual_label,
                  st_y(st_centroid(to_geography(h3_cell_to_boundary(h3_cell)))) as lat,
                  st_x(st_centroid(to_geography(h3_cell_to_boundary(h3_cell)))) as lon,
                  ntile(20) over (partition by asof_day order by probability desc) as vintile
                from CITYDW.GOLD.VW_POTHOLE_PREDICTIONS_ENRICHED
                where asof_day::date between ? and ?
                {borough_filter}
              )
              select * from ranked where vintile = 1
            """
            df = session.sql(sql, params=[str(date_from), str(date_to)]).to_pandas()
        else:
            sql = f"""
              select
                asof_day::date as asof_date, h3_cell, borough, probability, actual_label,
                st_y(st_centroid(to_geography(h3_cell_to_boundary(h3_cell)))) as lat,
                st_x(st_centroid(to_geography(h3_cell_to_boundary(h3_cell)))) as lon
              from CITYDW.GOLD.VW_POTHOLE_PREDICTIONS_ENRICHED
              where asof_day::date between ? and ?
                and probability >= ?
                {borough_filter}
            """
            df = session.sql(sql, params=[str(date_from), str(date_to), float(threshold)]).to_pandas()
    else:
        if mode == "Top 5% per day":
            sql = """
              with base as (
                select
                  asof_day::date as asof_date, h3_cell, probability, actual_label,
                  st_y(st_centroid(to_geography(h3_cell_to_boundary(h3_cell)))) as lat,
                  st_x(st_centroid(to_geography(h3_cell_to_boundary(h3_cell)))) as lon
                from CITYDW.GOLD.POTHOLE_PREDICTIONS
                where asof_day::date between ? and ?
              ),
              ranked as (
                select *, ntile(20) over (partition by asof_date order by probability desc) as vintile
                from base
              )
              select asof_date, h3_cell, null as borough, probability, actual_label, lat, lon
              from ranked where vintile=1
            """
            df = session.sql(sql, params=[str(date_from), str(date_to)]).to_pandas()
        else:
            sql = """
              select
                asof_day::date as asof_date, h3_cell, null as borough, probability, actual_label,
                st_y(st_centroid(to_geography(h3_cell_to_boundary(h3_cell)))) as lat,
                st_x(st_centroid(to_geography(h3_cell_to_boundary(h3_cell)))) as lon
              from CITYDW.GOLD.POTHOLE_PREDICTIONS
              where asof_day::date between ? and ?
                and probability >= ?
            """
            df = session.sql(sql, params=[str(date_from), str(date_to), float(threshold)]).to_pandas()
    return lc(df)

borough_filter_sql = ""
if have_enriched and sel_boroughs and "All" not in sel_boroughs:
    within = ", ".join([f"'{b}'" for b in sel_boroughs])
    borough_filter_sql = f" and borough in ({within}) "

hotspots = fetch_hotspots(mode, threshold, date_from, date_to, borough_filter_sql)

# ----------------- KPIs -----------------
st.subheader("Overall Model Quality")
try:
    overall = lc(session.sql("select * from CITYDW.GOLD.VW_METRICS_OVERALL").to_pandas())
    overall = overall.iloc[0] if not overall.empty else None
except Exception:
    overall = None

if overall is None:
    allpred = lc(session.sql("select predicted_label, actual_label from CITYDW.GOLD.POTHOLE_PREDICTIONS").to_pandas())
    if allpred.empty:
        st.warning("No predictions to compute metrics.")
        st.stop()
    acc = (allpred["predicted_label"] == allpred["actual_label"]).mean()
    tp  = int(((allpred["predicted_label"]==1) & (allpred["actual_label"]==1)).sum())
    fp  = int(((allpred["predicted_label"]==1) & (allpred["actual_label"]==0)).sum())
    fn  = int(((allpred["predicted_label"]==0) & (allpred["actual_label"]==1)).sum())
    tn  = int(((allpred["predicted_label"]==0) & (allpred["actual_label"]==0)).sum())
    prec = tp/(tp+fp) if (tp+fp)>0 else None
    rec  = tp/(tp+fn) if (tp+fn)>0 else None
else:
    acc  = float(overall["accuracy"])
    tp   = int(overall["tp"]); fp = int(overall["fp"]); fn  = int(overall["fn"]); tn = int(overall["tn"])
    prec = float(overall["precision"]) if pd.notnull(overall["precision"]) else None
    rec  = float(overall["recall"])    if pd.notnull(overall["recall"])    else None

k1, k2, k3, k4 = st.columns(4)
k1.metric("Accuracy", f"{acc*100:.1f}%")
k2.metric("Precision", "—" if prec is None else f"{prec*100:.1f}%")
k3.metric("Recall",    "—" if rec  is None else f"{rec*100:.1f}%")
k4.metric("Records", f"{tp+fp+fn+tn}")

st.divider()

# ----------------- Map -----------------
st.subheader(f"Hotspot Map — {shape_mode}")
if hotspots.empty:
    st.info("No hotspot rows for the selected filters. Try widening the date range or lowering the threshold.")
else:
    # ensure lon/lat columns; if missing, derive once
    if "lon" not in hotspots.columns or "lat" not in hotspots.columns:
        geodf = session.sql("""
          select h3_cell,
                 st_y(st_centroid(to_geography(h3_cell_to_boundary(h3_cell)))) as lat,
                 st_x(st_centroid(to_geography(h3_cell_to_boundary(h3_cell)))) as lon
          from (select distinct h3_cell from CITYDW.GOLD.POTHOLE_PREDICTIONS)
        """).to_pandas()
        geodf = lc(geodf)
        hotspots = hotspots.merge(geodf, on="h3_cell", how="left")

    try:
        if shape_mode.startswith("Exact H3"):
            poly_records = to_polygon_records(hotspots, color_mode, hex_alpha, hex_top_n)
            if len(poly_records) == 0:
                st.info("No polygon geometries available for current filters.")
            else:
                lons = [r["lon"] for r in poly_records if r.get("lon") is not None]
                lats = [r["lat"] for r in poly_records if r.get("lat") is not None]
                if not lons or not lats:
                    lons = [r["polygon"][0][0] for r in poly_records if r.get("polygon")]
                    lats = [r["polygon"][0][1] for r in poly_records if r.get("polygon")]
                center_lon = float(sum(lons)/len(lons))
                center_lat = float(sum(lats)/len(lats))

                poly_layer = pdk.Layer(
                    "PolygonLayer",
                    data=poly_records,
                    get_polygon="polygon",
                    get_fill_color="fill_color",
                    get_line_color="line_color",
                    line_width_min_pixels=1,
                    stroked=True,
                    filled=True,
                    extruded=False,
                    pickable=True,
                    auto_highlight=True,
                )

                deck = pdk.Deck(
                    layers=[poly_layer],
                    initial_view_state={"longitude": center_lon, "latitude": center_lat, "zoom": 10.0},
                    tooltip={"html": "<b>{borough}</b><br/>Prob: {probability}<br/>H3: {h3_cell}<br/>{asof_date}",
                             "style": {"backgroundColor": "white", "color": "black"}},
                    map_provider="carto",
                    map_style="light"
                )
                st.pydeck_chart(deck)
                st.caption(f"Hexes drawn: {len(poly_records)}. Use 'Hex fill opacity' and 'Max hexes to draw' to control clutter.")
        else:
            scat_records = to_scatter_records(hotspots, color_mode)
            if len(scat_records) == 0:
                st.info("No mappable hotspot rows (missing coordinates).")
            else:
                center_lon = sum([r["lon"] for r in scat_records]) / len(scat_records)
                center_lat = sum([r["lat"] for r in scat_records]) / len(scat_records)

                scatter_layer = pdk.Layer(
                    "ScatterplotLayer",
                    data=scat_records,
                    get_position="[lon, lat]",
                    get_radius="radius",
                    get_fill_color="color",
                    pickable=True,
                    auto_highlight=True,
                )
                deck = pdk.Deck(
                    layers=[scatter_layer],
                    initial_view_state={"longitude": float(center_lon), "latitude": float(center_lat), "zoom": 10.0},
                    tooltip={"html": "<b>{borough}</b><br/>Prob: {probability}<br/>H3: {h3_cell}<br/>{asof_date}",
                             "style": {"backgroundColor": "white", "color": "black"}},
                    map_provider="carto",
                    map_style="light"
                )
                st.pydeck_chart(deck)
                st.caption(f"Points drawn: {len(scat_records)} (circles show approx influence area).")
    except Exception as e:
        st.warning(f"Map renderer had an issue, showing simple map instead. ({e})")
        fallback_df = hotspots.copy()
        if "lat" in fallback_df.columns and "lon" in fallback_df.columns:
            st.map(fallback_df.rename(columns={"lat":"latitude","lon":"longitude"}))
        else:
            st.info("No coordinates available for fallback.")

# ----------------- Leaderboard (+ lat/lon + download) -----------------
st.subheader("Top Risk Cells (with coordinates)")
if not hotspots.empty:
    if "lon" not in hotspots.columns or "lat" not in hotspots.columns:
        geodf = session.sql("""
          select h3_cell,
                 st_y(st_centroid(to_geography(h3_cell_to_boundary(h3_cell)))) as lat,
                 st_x(st_centroid(to_geography(h3_cell_to_boundary(h3_cell)))) as lon
          from (select distinct h3_cell from CITYDW.GOLD.POTHOLE_PREDICTIONS)
        """).to_pandas()
        geodf = lc(geodf)
        hotspots = hotspots.merge(geodf, on="h3_cell", how="left")

    view_cols = [c for c in ["asof_date","borough","h3_cell","lat","lon","probability","actual_label"] if c in hotspots.columns]
    top_tbl = hotspots.sort_values("probability", ascending=False).head(200)[view_cols]
    st.dataframe(top_tbl, use_container_width=True)

    csv = top_tbl.to_csv(index=False).encode("utf-8")
    st.download_button("Download hotspots (CSV)", data=csv, file_name="pothole_hotspots.csv", mime="text/csv")



# ----------------- Drilldown -----------------
st.subheader("Drilldown by H3 Cell")
cell_id = st.text_input("Enter H3 cell ID (optional)")
if cell_id:
    cell_hist = lc(session.sql("""
      select asof_day::date as asof_date, h3_cell, probability, actual_label, predicted_label
      from CITYDW.GOLD.POTHOLE_PREDICTIONS
      where h3_cell = ?
      order by asof_day
    """, params=[cell_id]).to_pandas())
    if cell_hist.empty:
        st.info("No data for that H3 cell.")
    else:
        st.dataframe(cell_hist.tail(200), use_container_width=True)
        st.line_chart(cell_hist.set_index("asof_date")["probability"])
