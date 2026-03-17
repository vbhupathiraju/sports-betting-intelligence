import streamlit as st
import snowflake.connector
import pandas as pd

st.set_page_config(
    page_title="Sports Betting Intelligence",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Sports Betting Intelligence Platform")
st.caption("Detecting divergence between Kalshi prediction markets and sportsbook odds")

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        database=st.secrets["snowflake"]["database"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        schema="PUBLIC"
    )

@st.cache_data(ttl=60)
def load_divergence_signals():
    conn = get_snowflake_connection()
    df = pd.read_sql("SELECT * FROM SPORTS_BETTING.PUBLIC.v_divergence_latest", conn)
    conn.close()
    return df

@st.cache_data(ttl=60)
def load_sharp_money_signals():
    conn = get_snowflake_connection()
    df = pd.read_sql("SELECT * FROM SPORTS_BETTING.PUBLIC.v_sharp_money_latest", conn)
    conn.close()
    return df

@st.cache_data(ttl=60)
def load_signals_summary():
    conn = get_snowflake_connection()
    df = pd.read_sql("SELECT * FROM SPORTS_BETTING.PUBLIC.v_signals_summary", conn)
    conn.close()
    return df

tab1, tab2, tab3 = st.tabs(["🔀 Divergence Signals", "💰 Sharp Money Signals", "📈 Summary"])

with tab1:
    st.subheader("Divergence Signals")
    st.caption("Kalshi mid-price vs sportsbook implied probability — gaps ≥5%")
    try:
        df = load_divergence_signals()
        st.metric("Total Signals", len(df))
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading divergence signals: {e}")

with tab2:
    st.subheader("Sharp Money Signals")
    st.caption("Moneyline movement ≥10 American odds points between polls")
    try:
        df = load_sharp_money_signals()
        st.metric("Total Signals", len(df))
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading sharp money signals: {e}")

with tab3:
    st.subheader("Signals Summary")
    st.caption("Aggregated signal counts by sport")
    try:
        df = load_signals_summary()
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading signals summary: {e}")

st.divider()
st.caption("Data refreshes every 60 seconds. Last loaded at: " + pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S UTC"))

# Auto-refresh every 60 seconds
import time
time.sleep(60)
st.rerun()
