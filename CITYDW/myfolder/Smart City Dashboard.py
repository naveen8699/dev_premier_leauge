import streamlit as st

# ------------------------------
# Page Configuration
# ------------------------------
st.set_page_config(
    page_title="Smart City Dashboard",
    page_icon="🚦",
    layout="wide"
)

# ------------------------------
# Set Dark Background
# ------------------------------
st.markdown("""
    <style>
    /* Set entire app background to black */
    .stApp {
        background-color: #000000;
    }
    /* Make columns use flex to align cards equally */
    .card-row {
        display: flex;
        gap: 20px;
    }
    .card {
        flex: 1;
        border:1px solid #444;
        border-radius:10px;
        padding:20px;
        text-align:center;
        background-color:#f0f4f8;
        color:#000000;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        min-height:220px;
    }
    .card button {
        background-color:#0B3D91; 
        color:white; 
        padding:10px 20px; 
        border:none; 
        border-radius:5px; 
        cursor:pointer;
    }
    .card button:hover {
        background-color:#094080;
    }
    </style>
""", unsafe_allow_html=True)

# ------------------------------
# Dashboard Header
# ------------------------------
st.markdown("<h1 style='text-align: center; color: #ffffff;'>🚦 Smart City Dashboard</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #cccccc;'>Monitor traffic, predict potholes, and optimize city services in real-time across all boroughs of New York City.</p>", unsafe_allow_html=True)
st.markdown("---")

# ------------------------------
# App Cards (Side by Side, Reordered)
# ------------------------------
st.markdown("""
<div class="card-row">
    <div class="card">
        <h3>Service Requests</h3>
        <p>Track, monitor, and improve city service response times for all boroughs efficiently.</p>
        <a href="https://app.snowflake.com/jtjgole/yv14541/#/streamlit-apps/CITYDW.GOLD.E8N1UGEBU34OZHOF" target="_blank">
            <button>Open App 1</button>
        </a>
    </div>
    <div class="card">
        <h3>Pothole Prediction</h3>
        <p>Predict and prioritize potholes before issues occur, covering all NYC boroughs.</p>
        <a href="https://app.snowflake.com/jtjgole/yv14541/#/streamlit-apps/SMART_CITY.TRAFFIC.YRRHV3R1I4D5P585" target="_blank">
            <button>Open App 2</button>
        </a>
    </div>
    <div class="card">
        <h3>Traffic Hotspots</h3>
        <p>Quickly visualize congestion points and traffic intensity across NYC streets and boroughs.</p>
        <a href="https://app.snowflake.com/jtjgole/yv14541/#/streamlit-apps/CITYDW.SILVER.KLO96NM4G7_7XWHP" target="_blank">
            <button>Open App 3</button>
        </a>
    </div>
</div>
""", unsafe_allow_html=True)

st.markdown("---")

# ------------------------------
# Footer / Notes
# ------------------------------
st.markdown("<p style='text-align: center; color: #aaaaaa;'>Click on the buttons above to open each app. Each app provides real-time insights and actionable intelligence for New York City.</p>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #ffffff; font-weight:bold;'>Team: SnowBuilders</p>", unsafe_allow_html=True)
