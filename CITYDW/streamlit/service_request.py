
import math
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session
def run():
    st.header("test")

st.set_page_config(page_title="City Service Prioritization", page_icon="🚦", layout="wide")

session = get_active_session()

VIEW_QUEUE = "GOLD.V_SERVICE_REQUEST_PRIORITY_QUEUE"
VIEW_SILVER = "SILVER.V_SERVICE_REQUEST_WITH_SEVERITY"

def pct(n, d):
    return 0.0 if not d else (100.0 * n / d)

def sql_in_list(col, values):
    vals = [v for v in values if v]
    if not vals:
        return ""
    safe_values = ["'" + str(v).replace("'", "''") + "'" for v in vals]
    return f"{col} in ({','.join(safe_values)})"

def safe_default(options, desired):
    opt_set = set(options or [])
    return [d for d in (desired or []) if d in opt_set]

def _to_date(x):
    if x is None:
        return None
    if isinstance(x, (date, datetime)):
        return x.date() if isinstance(x, datetime) else x
    return pd.to_datetime(x).date()

@st.cache_data(ttl=300, show_spinner=False)
def load_distinct(col):
    sql = f"select distinct {col} as v from {VIEW_QUEUE} where {col} is not null order by {col}"
    df = session.sql(sql).to_pandas()
    return [str(x).strip() for x in df["V"].tolist() if str(x).strip() != ""]

@st.cache_data(ttl=300, show_spinner=False)
def load_bucket_options():
    sql = f"select distinct priority_bucket as v from {VIEW_QUEUE} where priority_bucket is not null order by priority_bucket"
    df = session.sql(sql).to_pandas()
    vals = [str(x).strip().upper() for x in df["V"].tolist() if str(x).strip() != ""]
    return sorted(set(vals))

@st.cache_data(ttl=300, show_spinner=False)
def load_date_bounds():
    sql = f"select to_date(min(created_ts)) as min_d, to_date(max(created_ts)) as max_d from {VIEW_QUEUE}"
    df = session.sql(sql).to_pandas()
    if df.empty or df.iloc[0].isna().any():
        today = datetime.utcnow().date()
        return today, today
    return _to_date(df.at[0, "MIN_D"]), _to_date(df.at[0, "MAX_D"])

@st.cache_data(ttl=180, show_spinner=False)
def load_queue(filters):
    where = []
    if filters["agencies"]:
        w = sql_in_list("agency_name", filters["agencies"])
        if w: where.append(w)
    if filters["boroughs"]:
        w = sql_in_list("borough", filters["boroughs"])
        if w: where.append(w)
    if filters["types"]:
        w = sql_in_list("complaint_type", filters["types"])
        if w: where.append(w)
    if filters["buckets"]:
        w = sql_in_list("priority_bucket", filters["buckets"])
        if w: where.append(w)
    if not filters.get("ignore_date", False):
        if filters["created_from"]:
            where.append("created_ts >= to_timestamp_ntz('" + filters["created_from"].strftime("%Y-%m-%d") + "')")
        if filters["created_to"]:
            end_next = filters["created_to"] + timedelta(days=1)
            where.append("created_ts < to_timestamp_ntz('" + end_next.strftime("%Y-%m-%d") + "')")
    sql = f"""
      select
        unique_key, agency_name, complaint_type, descriptor, borough,
        created_ts, status,
        age_hours, target_hours, breach_risk,
        severity, recent_similar_count,
        priority_bucket, priority_score, inferred_due_ts,
        latitude, longitude, h3_cell
      from {VIEW_QUEUE}
    """
    if where:
        sql += " where " + " and ".join(where)
    sql += " order by priority_score desc, age_hours desc"
    return session.sql(sql).to_pandas()

@st.cache_data(ttl=300, show_spinner=False)
def load_trend():
    sql = f"""
      with days as (
        select date_trunc('day', created_ts) as d, count(*) as new_cnt
        from {VIEW_SILVER}
        group by 1
      ), closed as (
        select date_trunc('day', closed_ts) as d, count(*) as closed_cnt
        from {VIEW_SILVER}
        where closed_ts is not null
        group by 1
      )
      select coalesce(days.d, closed.d) as day,
             coalesce(new_cnt,0) as new_cnt,
             coalesce(closed_cnt,0) as closed_cnt
      from days full outer join closed on days.d=closed.d
      order by day desc
      limit 90
    """
    return session.sql(sql).to_pandas()

st.title("City Service Request Prioritization")
st.caption("Improve response time by ranking and routing the right issues first")

agencies  = sorted(load_distinct("agency_name"))
boroughs  = sorted([b for b in load_distinct("borough") if b.upper() != "UNSPECIFIED"])
types     = sorted(load_distinct("complaint_type"))
buckets   = load_bucket_options()
min_d, max_d = load_date_bounds()

desired_buckets = ["P1","P2","P3","P4"]
default_buckets = safe_default(buckets, desired_buckets) or buckets

with st.sidebar:
    st.header("Filters")
    sel_agency = st.multiselect("Agency", agencies, default=agencies)
    sel_boro   = st.multiselect("Borough", boroughs, default=boroughs)
    sel_types  = st.multiselect("Complaint Type", types, default=[])
    sel_bucket = st.multiselect("Priority Bucket", buckets, default=default_buckets)

    st.markdown("---")
    st.subheader("Date Range")
    ignore_date = st.checkbox("Ignore date filter", value=False)
    default_from = max(min_d, (max_d - timedelta(days=365))) if min_d and max_d else (datetime.utcnow().date() - timedelta(days=365))
    default_to   = max_d or datetime.utcnow().date()
    created_from = st.date_input("From", value=default_from, min_value=min_d, max_value=max_d) if not ignore_date else None
    created_to   = st.date_input("To",   value=default_to,   min_value=min_d, max_value=max_d) if not ignore_date else None

filters = {
    "agencies": sel_agency,
    "boroughs": sel_boro,
    "types": sel_types,
    "buckets": sel_bucket,
    "created_from": datetime.combine(created_from, datetime.min.time()) if created_from else None,
    "created_to": datetime.combine(created_to, datetime.min.time()) if created_to else None,
    "ignore_date": ignore_date,
}

with st.spinner("Loading data..."):
    df = load_queue(filters)

if df.empty:
    st.warning("No data found. Try clearing filters or expanding date range.")
    st.stop()

for col in ["AGE_HOURS","TARGET_HOURS","BREACH_RISK","SEVERITY","RECENT_SIMILAR_COUNT","PRIORITY_SCORE"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

total_open = len(df)
p1p2 = int((df["PRIORITY_BUCKET"].isin(["P1","P2"])).sum()) if "PRIORITY_BUCKET" in df.columns else 0
overdue = int((df["BREACH_RISK"] > 1).sum()) if "BREACH_RISK" in df.columns else 0
med_age = float(np.nanmedian(df["AGE_HOURS"])) if "AGE_HOURS" in df.columns else np.nan
med_target = float(np.nanmedian(df["TARGET_HOURS"])) if "TARGET_HOURS" in df.columns else np.nan

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Open", f"{total_open:,}")
c2.metric("P1/P2", f"{p1p2:,}", f"{pct(p1p2, total_open):.1f}%")
c3.metric("Overdue", f"{pct(overdue, total_open):.1f}%")
c4.metric("Median Age (hrs)", f"{med_age:.0f}" if not math.isnan(med_age) else "—")
c5.metric("Median Target (hrs)", f"{med_target:.0f}" if not math.isnan(med_target) else "—")

# Tabs: Overview, Citywide, Queue, Map, Agency, Explain
tab_overview, tab_citywide, tab_queue, tab_map, tab_agency, tab_explain = st.tabs(
    ["Overview", "Citywide", "Queue", "Map", "Agency", "Explain"]
)

# -------- Overview --------
with tab_overview:
    st.caption("Overview: quick summary + where to find what.")
    st.subheader("Overview")
    st.write("""
This dashboard prioritizes open service requests using breach risk (age vs. target SLA),
severity, and recurrence. Use the tabs below to explore citywide insights, the live queue,
a hotspot map, agency views, and a short explainer.
    """)
    nav = pd.DataFrame({
        "Tab": ["Overview", "Citywide", "Queue", "Map", "Agency", "Explain"],
        "What you see": [
            "Summary and guide to other tabs",
            "Citywide metrics by borough and top complaint types",
            "Ranked list of open requests with scores and due times",
            "Geospatial hotspots for urgent (P1/P2) requests",
            "Breakdown by agency with top items to dispatch",
            "How the scoring and buckets are computed"
        ]
    })
    st.table(nav)

    with st.expander("Request trend (last 90 days)"):
        try:
            trend = load_trend()
            if not trend.empty:
                st.line_chart(trend.set_index("DAY")[["NEW_CNT","CLOSED_CNT"]].sort_index())
            else:
                st.info("Trend data unavailable.")
        except Exception:
            st.info("Trend data unavailable.")

# -------- Citywide --------
with tab_citywide:
    st.caption("Citywide: borough-level metrics and top complaint types.")
    st.subheader("Citywide Summary")

    # Borough-level metrics
    have_cols = {"BOROUGH","UNIQUE_KEY","PRIORITY_BUCKET","BREACH_RISK","AGE_HOURS","TARGET_HOURS"}.issubset(df.columns)
    if have_cols:
        grp = (
            df.assign(P1P2=df["PRIORITY_BUCKET"].isin(["P1","P2"]),
                      OVERDUE=(df["BREACH_RISK"] > 1))
              .groupby("BOROUGH", as_index=False)
              .agg(
                  open_count=("UNIQUE_KEY","count"),
                  p1p2=("P1P2","sum"),
                  overdue=("OVERDUE","sum"),
                  median_age_hr=("AGE_HOURS","median"),
                  median_target_hr=("TARGET_HOURS","median")
              )
        )
        grp["p1p2_pct"] = (grp["p1p2"] / grp["open_count"] * 100).round(1).fillna(0)
        grp["overdue_pct"] = (grp["overdue"] / grp["open_count"] * 100).round(1).fillna(0)
        cols = ["BOROUGH","open_count","p1p2","p1p2_pct","overdue","overdue_pct","median_age_hr","median_target_hr"]
        st.dataframe(grp[cols].sort_values("p1p2", ascending=False), use_container_width=True, hide_index=True)

        st.bar_chart(grp.set_index("BOROUGH")[["p1p2"]])
    else:
        st.info("Borough metrics unavailable in current data.")

    # Top complaint types (overall)
    if "COMPLAINT_TYPE" in df.columns:
        top_types = (
            df.groupby("COMPLAINT_TYPE", as_index=False)
              .agg(count=("UNIQUE_KEY","count"))
              .sort_values("count", ascending=False)
              .head(10)
        )
        st.subheader("Top Complaint Types (Overall)")
        st.dataframe(top_types, use_container_width=True, hide_index=True)
        st.bar_chart(top_types.set_index("COMPLAINT_TYPE"))
    else:
        st.info("Complaint type not available.")

# -------- Queue --------
with tab_queue:
    st.caption("Queue: prioritized list of open requests.")
    st.subheader("Priority Queue")
    show_cols = [
        "UNIQUE_KEY","AGENCY_NAME","BOROUGH","COMPLAINT_TYPE","DESCRIPTOR",
        "PRIORITY_BUCKET","PRIORITY_SCORE","AGE_HOURS","TARGET_HOURS",
        "BREACH_RISK","SEVERITY","RECENT_SIMILAR_COUNT","CREATED_TS",
        "INFERRED_DUE_TS","STATUS"
    ]
    existing = [c for c in show_cols if c in df.columns]
    st.dataframe(df[existing], use_container_width=True, hide_index=True)
    st.download_button("Download CSV", df[existing].to_csv(index=False).encode("utf-8"), "priority_queue.csv")

# -------- Map --------
with tab_map:
    st.caption("Map: hotspots for P1/P2 requests.")
    st.subheader("Priority Hotspots")
    if {"PRIORITY_BUCKET","LATITUDE","LONGITUDE"}.issubset(df.columns):
        df_map = df[df["PRIORITY_BUCKET"].isin(["P1","P2"])].dropna(subset=["LATITUDE","LONGITUDE"])
        if df_map.empty:
            st.info("No P1/P2 complaints with coordinates.")
        else:
            st.map(df_map.rename(columns={"LATITUDE":"latitude","LONGITUDE":"longitude"})[["latitude","longitude"]])
    else:
        st.info("Location data not available for map view.")

# -------- Agency --------
with tab_agency:
    st.caption("Agency: P1/P2 counts and top items by agency.")
    st.subheader("Agency View")
    if "AGENCY_NAME" in df.columns:
        grp = (
            df.assign(P1P2=df["PRIORITY_BUCKET"].isin(["P1","P2"]))
              .groupby("AGENCY_NAME", as_index=False)
              .agg(open_count=("UNIQUE_KEY","count"), p1p2=("P1P2","sum"))
              .sort_values("p1p2", ascending=False)
        )
        if not grp.empty:
            st.bar_chart(grp.set_index("AGENCY_NAME")[["p1p2"]])
            sel = st.selectbox("Select Agency", grp["AGENCY_NAME"].unique())
            topN = df[df["AGENCY_NAME"] == sel].head(10)
            cols = ["UNIQUE_KEY","BOROUGH","COMPLAINT_TYPE","DESCRIPTOR",
                    "PRIORITY_BUCKET","PRIORITY_SCORE","AGE_HOURS","TARGET_HOURS","BREACH_RISK"]
            existing_cols = [c for c in cols if c in topN.columns]
            st.dataframe(topN[existing_cols], use_container_width=True, hide_index=True)
        else:
            st.info("No agency data found.")
    else:
        st.info("Agency information unavailable.")

# -------- Explain --------
with tab_explain:
    st.caption("Explain: how scores and buckets are built.")
    st.subheader("How Prioritization Works")
    st.markdown("""
**Priority Score**
- 60% Breach risk = Age ÷ Target SLA  
- 30% Severity = Complaint urgency  
- 10% Recurrence = Repeated complaints nearby  

**Buckets**
- **P1:** Overdue or highest severity  
- **P2:** Close to SLA breach  
- **P3:** Moderate urgency  
- **P4:** Low urgency  

All scores and updates are computed in Snowflake; this app provides a live operational view.
    """)

st.caption("© Smart City Data Platform – Powered by Snowflake Streamlit")
