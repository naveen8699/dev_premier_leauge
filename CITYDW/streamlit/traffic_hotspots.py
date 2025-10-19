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
    "Borough Rush Analysis", 
    "Holiday vs Regular Traffic"
])

# ------------------------------
# PAGE 1: Overview
# ------------------------------
with tabs[0]:
    st.title("NYC Traffic & Pothole Dashboard Overview")
    st.markdown("""
        <p style='color:#555; font-size:14px;'>
        Welcome to the NYC Traffic & Pothole Dashboard. This tool helps city planners, traffic analysts, and public service teams make informed decisions by providing:
        </p>
        <ul style='color:#555; font-size:14px;'>
            <li>🚦 Real-time traffic congestion hotspots across all NYC boroughs</li>
            <li>🛠️ Service request monitoring to improve response times</li>
            <li>⚡ Pothole prediction to prioritize maintenance proactively</li>
            <li>📊 Comparative traffic analysis: rush hours, boroughs, holidays vs regular days</li>
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
# PAGE 3: Borough Rush Analysis
# ------------------------------
with tabs[2]:
    st.subheader("Rush Period Traffic Analysis by Borough")
    df2 = session.sql("""
        SELECT BORO, RUSH_PERIOD, AVG_RUSH_VOLUME_PER_BORO
        FROM CITYDW.SILVER.STREAM_TAB
        WHERE LOAD_ID = 2
    """).to_pandas()

    with st.expander("🔧 Filters"):
        select_all_boro2 = st.checkbox("Select All Boroughs", value=True, key="boro2")
        selected_boro_list2 = df2['BORO'].unique().tolist() if select_all_boro2 else st.multiselect("Select Borough(s):", options=df2['BORO'].unique(), key="multi_boro2")

    filtered_df2 = df2[df2['BORO'].isin(selected_boro_list2)]

    line_chart = px.line(
        filtered_df2,
        x='RUSH_PERIOD',
        y='AVG_RUSH_VOLUME_PER_BORO',
        color='BORO',
        markers=True,
        title="Average Rush Volume per Borough"
    )
    st.plotly_chart(line_chart, use_container_width=True)
    st.markdown("<p style='color:#999;font-size:12px;'>Shows average traffic volume for each borough over different rush periods, identifying peak and off-peak times.</p>", unsafe_allow_html=True)

    with st.expander("Show Raw Data"):
        st.dataframe(filtered_df2)

# ------------------------------
# PAGE 4: Holiday vs Regular Traffic
# ------------------------------
with tabs[3]:
    st.subheader("Holiday vs Regular Season Traffic")
    df3 = session.sql("""
        SELECT BORO, STREET, RUSH_PERIOD, AVG_RUSH_VOLUME
        FROM CITYDW.SILVER.STREAM_TAB
        WHERE LOAD_ID = 5
    """).to_pandas()

    df3['AVG_RUSH_VOLUME_PER_BORO'] = df3.groupby('BORO')['AVG_RUSH_VOLUME'].transform('mean')

    with st.expander("🔧 Filters"):
        select_all_boro3 = st.checkbox("Select All Boroughs", value=True, key="boro3")
        selected_boro_list3 = df3['BORO'].unique().tolist() if select_all_boro3 else st.multiselect("Select Borough(s):", options=df3['BORO'].unique(), key="multi_boro3")
        select_all_street3 = st.checkbox("Select All Streets", value=True, key="street3")
        selected_street_list3 = df3['STREET'].unique().tolist() if select_all_street3 else st.multiselect("Select Street(s):", options=df3['STREET'].unique(), key="multi_street3")

    filtered_df3 = df3[(df3['BORO'].isin(selected_boro_list3)) & (df3['STREET'].isin(selected_street_list3))]

    fig = px.bar(
        filtered_df3,
        x='STREET',
        y='AVG_RUSH_VOLUME',
        color='RUSH_PERIOD',
        facet_col='BORO',
        labels={'AVG_RUSH_VOLUME':'Avg Volume', 'STREET':'Street'},
        title="Traffic Volume Comparison per Borough"
    )
    st.plotly_chart(fig, use_container_width=True)
    st.markdown("<p style='color:#999;font-size:12px;'>Compares traffic volumes during holidays vs regular days across streets and boroughs. Helps identify congestion trends.</p>", unsafe_allow_html=True)

    with st.expander("Show Raw Data"):
        st.dataframe(filtered_df3)
