import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import plotly.express as px

# ------------------------------
# App Configuration
# ------------------------------
st.set_page_config(
    page_title="NYC Traffic Dashboard",
    layout="wide",
    page_icon="🚦"
)
session = get_active_session()

# ------------------------------
# Header
# ------------------------------
st.markdown("<h1 style='text-align:center; color:#0B3D91;'>🚦 NYC Traffic & Pothole Dashboard</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align:center; color:#555;'>Monitor traffic, predict potholes, and optimize city services across all NYC boroughs.</p>", unsafe_allow_html=True)
st.markdown("---")

# ------------------------------
# Horizontal Tabs for Pages
# ------------------------------
tabs = st.tabs([
    "📖 Overview", 
    "Top Rush Hours", 
    "Holiday vs Regular Traffic",
    "Traffic Map"
])

# ------------------------------
# PAGE 1: Overview
# ------------------------------
with tabs[0]:
    st.title("NYC Traffic & Pothole Dashboard Overview")
    st.markdown("""
        <p style='color:#555; font-size:14px;'>
        Welcome to the NYC Traffic  Dashboard. This tool helps city planners, traffic analysts, and public service teams make informed decisions by providing:
        </p>
        <ul style='color:#555; font-size:14px;'>
            <li> Traffic congestion hotspots across all NYC boroughs
            <li> Comparative traffic analysis across boroughs, rush periods, and holidays vs regular days</li>
            <li> Traffic Growth Analysis over the years</li>
        </ul>
        <p style='color:#555; font-size:14px;'>
        The dashboard allows filtering by boroughs and streets, provides interactive charts and heatmaps, and helps teams take timely action.
        </p>
        <p style='color:#999; font-size:12px;'><strong>Team:</strong> Snowbuilders</p>
    """, unsafe_allow_html=True)

# ------------------------------
# PAGE 2: Top Rush Hours + Heatmap
# ------------------------------
with tabs[1]:
    st.subheader("Top Rush Hours by Borough & Street")
    st.markdown("<p style='color:#555; font-size:14px;'><strong>Rush Hour Definitions:</strong> Morning: 07:00–13:00 | Evening: 14:00–18:00</p>", unsafe_allow_html=True)

    df = session.sql("""
        SELECT BORO, STREET, RUSH_PERIOD AS HOUR, AVG_RUSH_VOLUME
        FROM CITYDW.SILVER.STREAM_TAB
        WHERE LOAD_ID = 3
    """).to_pandas()

    # Filters
    with st.expander("Filters"):
        select_all_boro = st.checkbox("Select All Boroughs", value=True)
        selected_boro_list = df['BORO'].unique().tolist() if select_all_boro else st.multiselect("Select Borough(s):", options=df['BORO'].unique())
        select_all_street = st.checkbox("Select All Streets", value=True)
        selected_street_list = df['STREET'].unique().tolist() if select_all_street else st.multiselect("Select Street(s):", options=df['STREET'].unique())

    filtered_df = df[(df['BORO'].isin(selected_boro_list)) & (df['STREET'].isin(selected_street_list))]

    # Bar Chart
    top_hours = filtered_df.sort_values(by='AVG_RUSH_VOLUME', ascending=False).groupby(['BORO','STREET']).head(5)
    bar_chart = px.bar(
        top_hours,
        x='HOUR',
        y='AVG_RUSH_VOLUME',
        color='BORO',
        hover_data=['STREET', 'AVG_RUSH_VOLUME'],
        labels={'AVG_RUSH_VOLUME':'Avg Volume', 'HOUR':'Hour'},
        title="Top Rush Hours by Borough & Street"
    )
    st.plotly_chart(bar_chart, use_container_width=True)
    st.markdown("<p style='color:#999;font-size:12px;'>Top 5 rush hours per street and borough. Peak traffic periods are highlighted.</p>", unsafe_allow_html=True)

    st.markdown("---")

    # Heatmap
    st.subheader("Rush Hour Intensity Heatmap")
    heatmap_data = filtered_df.groupby(['BORO','HOUR']).AVG_RUSH_VOLUME.mean().reset_index()
    heatmap_data['HOUR'] = heatmap_data['HOUR'].astype(str)
    heatmap_fig = px.density_heatmap(
        heatmap_data,
        x='HOUR',
        y='BORO',
        z='AVG_RUSH_VOLUME',
        color_continuous_scale='Viridis',
        labels={'AVG_RUSH_VOLUME':'Avg Volume', 'HOUR':'Hour', 'BORO':'Borough'},
        title='Rush Hour Intensity by Borough'
    )
    st.plotly_chart(heatmap_fig, use_container_width=True)
    st.markdown("<p style='color:#999;font-size:12px;'>Darker colors indicate higher traffic volume across boroughs and hours.</p>", unsafe_allow_html=True)

    with st.expander("Show Raw Data"):
        st.dataframe(filtered_df)


# ------------------------------
# PAGE 3: Holiday vs Regular Traffic
# ------------------------------
with tabs[2]:
    st.subheader("Holiday vs Regular Season Traffic")

    # --- Load data ---
    df3 = session.sql("""
        SELECT BORO, STREET, RUSH_PERIOD, AVG_RUSH_VOLUME
        FROM CITYDW.SILVER.STREAM_TAB
        WHERE LOAD_ID = 5
    """).to_pandas()

    # --- Filters ---
    with st.expander("🔧 Filters", expanded=True):
        boroughs = sorted(df3['BORO'].dropna().unique().tolist())
        streets = sorted(df3['STREET'].dropna().unique().tolist())

        selected_boros = st.multiselect(
            "Select Borough(s):",
            options=boroughs,
            default=boroughs,
            key="boro_filter_new"
        )

        selected_streets = st.multiselect(
            "Select Street(s):",
            options=streets,
            default=[],
            help="Leave empty to see borough-level averages",
            key="street_filter_new"
        )

    # --- Filter data ---
    filtered_df3 = df3[df3['BORO'].isin(selected_boros)]

    if selected_streets:
        filtered_df3 = filtered_df3[filtered_df3['STREET'].isin(selected_streets)]
        title_suffix = " (Selected Streets)"
    else:
        # Borough-level aggregation if no streets selected
        filtered_df3 = (
            filtered_df3.groupby(["BORO", "RUSH_PERIOD"], as_index=False)
            .agg({"AVG_RUSH_VOLUME": "mean"})
        )
        title_suffix = " (Borough-Level Average)"

    if filtered_df3.empty:
        st.warning("No data found for the selected filters.")
        st.stop()

    # --- Plotly line chart (cleaner view) ---
    fig = px.line(
        filtered_df3,
        x="BORO",
        y="AVG_RUSH_VOLUME",
        color="RUSH_PERIOD",
        markers=True,
        line_dash="RUSH_PERIOD",
        hover_data=["AVG_RUSH_VOLUME"],
        title=f"Traffic Pattern: Holiday vs Regular Rush Hours {title_suffix}",
        labels={
            "BORO": "Borough",
            "AVG_RUSH_VOLUME": "Average Traffic Volume",
            "RUSH_PERIOD": "Traffic Type"
        },
    )

    fig.update_traces(marker=dict(size=10, line=dict(width=1, color='DarkSlateGrey')))
    fig.update_layout(
        template="simple_white",
        height=500,
        legend_title_text="Rush Period",
        yaxis_title="Average Vehicle Volume",
        xaxis_title="Borough",
        hovermode="x unified",
        margin=dict(t=80, b=80)
    )

    st.plotly_chart(fig, use_container_width=True)

    # --- Insights Section ---
    st.markdown("""
    **Insights:**
    - Each point represents **average rush-hour traffic** for a borough.
    - The **lines connect Holiday vs Regular** values, showing relative changes.
    - Helps you spot where holidays **increase** (or decrease) congestion.
    """, unsafe_allow_html=True)

    # --- Raw Data ---
    with st.expander("📋 Show Underlying Data"):
        st.dataframe(filtered_df3, use_container_width=True)
# ------------------------------
# -----------------------------
# 5th Page: Traffic Growth Map



# -----------------------------------------------------------
# Page Config
# -----------------------------------------------------------
with tabs[3]:
    st.set_page_config(page_title="Traffic Growth Map", layout="wide")
    st.title("Traffic Growth Analysis — Map View")
    
    session = get_active_session()
    
    # -----------------------------------------------------------
    # 1️⃣ Yearly Aggregated Query (2021–2025)
    # -----------------------------------------------------------
    query = """
    WITH yearly_traffic AS (
        SELECT 
            EXTRACT(YEAR FROM TS) AS traffic_year,
            SUM(VOLUME_VEH) AS total_volume
        FROM CITYDW.SILVER.SV_TRAFFIC
        WHERE EXTRACT(YEAR FROM TS) BETWEEN 2021 AND 2025
        GROUP BY 1
        ORDER BY 1
    )
    SELECT
        traffic_year,
        total_volume,
        LAG(total_volume) OVER (ORDER BY traffic_year) AS prev_year_volume,
        ROUND(
            CASE 
                WHEN LAG(total_volume) OVER (ORDER BY traffic_year) IS NULL THEN NULL
                ELSE ((total_volume - LAG(total_volume) OVER (ORDER BY traffic_year)) / LAG(total_volume) OVER (ORDER BY traffic_year)) * 100
            END, 2
        ) AS pct_change
    FROM yearly_traffic;
    """
    df_year = session.sql(query).to_pandas()
    
    if df_year.empty:
        st.warning("No traffic data found for 2021–2025.")
        st.stop()
    
    # -----------------------------------------------------------
    # 2️⃣ Map Query (H3 grid aggregation)
    # -----------------------------------------------------------
    map_query = """
    SELECT
        EXTRACT(YEAR FROM TS) AS traffic_year,
        H3_CELL,
        SUM(VOLUME_VEH) AS total_volume,
        AVG(ST_Y(GEOG)) AS lat,
        AVG(ST_X(GEOG)) AS lon
    FROM CITYDW.SILVER.SV_TRAFFIC
    WHERE EXTRACT(YEAR FROM TS) BETWEEN 2021 AND 2025
    GROUP BY 1, H3_CELL
    ORDER BY 1, H3_CELL
    """
    df_map = session.sql(map_query).to_pandas()
    df_map.columns = [c.lower() for c in df_map.columns]
    
    # -----------------------------------------------------------
    # 3️⃣ Year Selector (Multi-select Dropdown)
    # -----------------------------------------------------------
    years = sorted(df_year["TRAFFIC_YEAR"].unique())
    selected_years = st.multiselect(
        "Select Year(s) for Analysis:",
        options=years,
        default=years,
    )
    
    if not selected_years:
        selected_years = years
    
    df_year_filtered = df_year[df_year["TRAFFIC_YEAR"].isin(selected_years)]
    df_map_filtered = df_map[df_map["traffic_year"].isin(selected_years)]
    
    # -----------------------------------------------------------
    # 4️⃣ KPIs — Simple Text Summary
    # -----------------------------------------------------------
    st.subheader("Summary Statistics")
    
    total_volume = int(df_year_filtered["TOTAL_VOLUME"].sum())
    avg_growth = df_year_filtered["PCT_CHANGE"].mean(skipna=True)
    max_growth = df_year_filtered["PCT_CHANGE"].max(skipna=True)
    min_growth = df_year_filtered["PCT_CHANGE"].min(skipna=True)
    
    c1, c2, c3, c4 = st.columns(4)
    
    c1.metric("Total Vehicle Volume", f"{total_volume:,}")
    c2.metric("Average Yearly Growth", f"{avg_growth:.2f}%")
    c3.metric("Maximum Yearly Growth", f"{max_growth:.2f}%")
    c4.metric("Minimum Yearly Growth", f"{min_growth:.2f}%")
    # -----------------------------------------------------------
    # 5️⃣ Map Section
    # -----------------------------------------------------------
    st.subheader("Traffic Volume Map")
    
    st.markdown("""
    **Map Description**  
    - Each point represents an H3 grid cell with aggregated vehicle volumes.  
    - Larger clusters indicate higher vehicle flow.  
    - Data shown for selected years.
    """)
    
    if {"lat", "lon"}.issubset(df_map_filtered.columns) and not df_map_filtered.empty:
        df_map_plot = df_map_filtered.rename(columns={"lat": "latitude", "lon": "longitude"})
        st.map(df_map_plot[["latitude", "longitude"]])
    else:
        st.info("No coordinates available for map view.")
    
    # -----------------------------------------------------------
    # 6️⃣ Data Download
    # -----------------------------------------------------------
    st.subheader("Download Data")
    
    csv = df_year_filtered.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download Yearly Growth Data (CSV)",
        data=csv,
        file_name="traffic_growth_summary.csv",
        mime="text/csv",
    )
