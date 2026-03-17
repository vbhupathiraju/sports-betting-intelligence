import streamlit as st
import snowflake.connector
import pandas as pd

st.set_page_config(
    page_title="Sports Betting Intelligence",
    page_icon="📊",
    layout="wide"
)

# ── Snowflake connection ──────────────────────────────────────────────────────

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
    df.columns = [c.lower() for c in df.columns]
    return df

@st.cache_data(ttl=60)
def load_sharp_money_signals():
    conn = get_snowflake_connection()
    df = pd.read_sql("SELECT * FROM SPORTS_BETTING.PUBLIC.v_sharp_money_latest", conn)
    conn.close()
    df.columns = [c.lower() for c in df.columns]
    return df

@st.cache_data(ttl=60)
def load_signals_summary():
    conn = get_snowflake_connection()
    df = pd.read_sql("SELECT * FROM SPORTS_BETTING.PUBLIC.v_signals_summary", conn)
    conn.close()
    df.columns = [c.lower() for c in df.columns]
    return df

# ── Formatting helpers ────────────────────────────────────────────────────────

SPORT_LABELS = {
    "basketball_nba": "NBA",
    "basketball_ncaab": "NCAAB",
}

def fmt_sport(key):
    return SPORT_LABELS.get(key, key)

def fmt_pct(val):
    try:
        return f"{float(val):.1%}"
    except:
        return val

def fmt_odds(val):
    try:
        v = int(float(val))
        return f"+{v}" if v > 0 else str(v)
    except:
        return val

def fmt_ts(val):
    try:
        return pd.Timestamp(val).strftime("%b %d %Y  %H:%M UTC")
    except:
        return val

def game_label(row):
    return f"{row['away_team']} @ {row['home_team']}"

# ── Load data ─────────────────────────────────────────────────────────────────

try:
    div_df = load_divergence_signals()
    sharp_df = load_sharp_money_signals()
    summary_df = load_signals_summary()
    load_error = None
except Exception as e:
    div_df = pd.DataFrame()
    sharp_df = pd.DataFrame()
    summary_df = pd.DataFrame()
    load_error = str(e)

# ── Header ────────────────────────────────────────────────────────────────────

st.title("📊 Sports Betting Intelligence Platform")
st.caption("Detecting divergence between Kalshi prediction markets and sportsbook odds")

if load_error:
    st.error(f"Failed to load data from Snowflake: {load_error}")
    st.stop()

latest_ts = None
if not div_df.empty and "computed_at" in div_df.columns:
    latest_ts = div_df["computed_at"].max()
elif not sharp_df.empty and "computed_at" in sharp_df.columns:
    latest_ts = sharp_df["computed_at"].max()

col_ts, col_refresh = st.columns([3, 1])
with col_ts:
    if latest_ts:
        st.info(f"🕐 Last signal computed: **{fmt_ts(latest_ts)}**")
    else:
        st.info("🕐 No signals computed yet")
with col_refresh:
    if st.button("🔄 Refresh now"):
        st.cache_data.clear()
        st.rerun()

st.divider()

# ── Global filters ────────────────────────────────────────────────────────────

all_sports = sorted(set(
    list(div_df["sport_key"].unique() if not div_df.empty else []) +
    list(sharp_df["sport_key"].unique() if not sharp_df.empty else [])
))
sport_options = ["All Sports"] + [fmt_sport(s) for s in all_sports]
selected_sport_label = st.selectbox("Filter by sport", sport_options)

if selected_sport_label == "All Sports":
    selected_sports = all_sports
else:
    selected_sports = [k for k, v in SPORT_LABELS.items() if v == selected_sport_label]

# Filter by sport first to build game list
div_filtered = div_df[div_df["sport_key"].isin(selected_sports)].copy() if not div_df.empty else div_df
sharp_filtered = sharp_df[sharp_df["sport_key"].isin(selected_sports)].copy() if not sharp_df.empty else sharp_df

all_games = sorted(set(
    list(div_filtered.apply(game_label, axis=1).unique() if not div_filtered.empty else []) +
    list(sharp_filtered.apply(game_label, axis=1).unique() if not sharp_filtered.empty else [])
))
game_options = ["All Games"] + all_games
selected_game = st.selectbox("Filter by game", game_options)

if selected_game != "All Games":
    if not div_filtered.empty:
        div_filtered = div_filtered[div_filtered.apply(game_label, axis=1) == selected_game]
    if not sharp_filtered.empty:
        sharp_filtered = sharp_filtered[sharp_filtered.apply(game_label, axis=1) == selected_game]

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab1, tab2, tab3 = st.tabs(["🔀 Divergence Signals", "💰 Sharp Money Signals", "📈 Summary"])

# ── Tab 1: Divergence Signals ─────────────────────────────────────────────────

with tab1:
    st.subheader("Divergence Signals")
    st.caption("Kalshi mid-price vs sportsbook implied probability — gaps ≥5%")

    if div_filtered.empty:
        st.info("No divergence signals for the selected filters.")
    else:
        st.metric("Total Signals", len(div_filtered))

        # Chart: divergence by game
        chart_data = (
            div_filtered.groupby(div_filtered.apply(game_label, axis=1))["divergence"]
            .mean()
            .reset_index()
        )
        chart_data.columns = ["game", "avg_divergence"]
        chart_data["avg_divergence_pct"] = (chart_data["avg_divergence"] * 100).round(1)
        chart_data = chart_data.sort_values("avg_divergence_pct", ascending=False)
        st.bar_chart(chart_data.set_index("game")["avg_divergence_pct"], height=250)
        st.caption("Average divergence % per game")

        st.divider()

        # Group by game
        games = div_filtered.apply(game_label, axis=1).unique()
        for game in sorted(games):
            game_data = div_filtered[div_filtered.apply(game_label, axis=1) == game].copy()
            sport = fmt_sport(game_data["sport_key"].iloc[0])
            n = len(game_data)
            max_div = game_data["divergence"].max()
            with st.expander(f"**{game}** — {sport} · {n} signal(s) · max divergence {fmt_pct(max_div)}", expanded=False):
                display = game_data[[
                    "market_ticker", "kalshi_implied_prob", "sportsbook_home_prob",
                    "divergence", "signal_direction", "num_bookmakers", "commence_time"
                ]].copy()
                display["kalshi_implied_prob"] = display["kalshi_implied_prob"].apply(fmt_pct)
                display["sportsbook_home_prob"] = display["sportsbook_home_prob"].apply(fmt_pct)
                display["divergence"] = display["divergence"].apply(fmt_pct)
                display["commence_time"] = display["commence_time"].apply(fmt_ts)
                display.columns = ["Market Ticker", "Kalshi Prob", "Sportsbook Prob", "Divergence", "Direction", "# Bookmakers", "Game Time"]
                st.dataframe(display, use_container_width=True, hide_index=True)

# ── Tab 2: Sharp Money Signals ────────────────────────────────────────────────

with tab2:
    st.subheader("Sharp Money Signals")
    st.caption("Moneyline movement ≥10 American odds points between polls")

    if sharp_filtered.empty:
        st.info("No sharp money signals for the selected filters.")
    else:
        st.metric("Total Signals", len(sharp_filtered))

        # Chart: avg prob movement by game
        chart_data2 = (
            sharp_filtered.groupby(sharp_filtered.apply(game_label, axis=1))["prob_movement"]
            .mean()
            .reset_index()
        )
        chart_data2.columns = ["game", "avg_prob_movement"]
        chart_data2["avg_prob_movement_pct"] = (chart_data2["avg_prob_movement"] * 100).round(1)
        chart_data2 = chart_data2.sort_values("avg_prob_movement_pct", ascending=False)
        st.bar_chart(chart_data2.set_index("game")["avg_prob_movement_pct"], height=250)
        st.caption("Average probability movement % per game")

        st.divider()

        # Group by game
        games2 = sharp_filtered.apply(game_label, axis=1).unique()
        for game in sorted(games2):
            game_data = sharp_filtered[sharp_filtered.apply(game_label, axis=1) == game].copy()
            sport = fmt_sport(game_data["sport_key"].iloc[0])
            n = len(game_data)
            max_move = game_data["prob_movement"].max()
            with st.expander(f"**{game}** — {sport} · {n} signal(s) · max prob movement {fmt_pct(max_move)}", expanded=False):
                display = game_data[[
                    "team", "bookmaker_key", "prev_odds", "american_odds",
                    "prob_movement", "movement_direction", "commence_time"
                ]].copy()
                display["prev_odds"] = display["prev_odds"].apply(fmt_odds)
                display["american_odds"] = display["american_odds"].apply(fmt_odds)
                display["prob_movement"] = display["prob_movement"].apply(fmt_pct)
                display["commence_time"] = display["commence_time"].apply(fmt_ts)
                display.columns = ["Team", "Bookmaker", "Prev Odds", "Current Odds", "Prob Movement", "Direction", "Game Time"]
                st.dataframe(display, use_container_width=True, hide_index=True)

# ── Tab 3: Summary ────────────────────────────────────────────────────────────

with tab3:
    st.subheader("Signals Summary")
    st.caption("Aggregated signal counts by sport")

    if summary_df.empty:
        st.info("No summary data available.")
    else:
        display_summary = summary_df.copy()
        display_summary["sport_key"] = display_summary["sport_key"].apply(fmt_sport)
        display_summary["latest_computed_at"] = display_summary["latest_computed_at"].apply(fmt_ts)
        display_summary.columns = ["Sport", "Total Signals", "Latest Computed At"]
        st.dataframe(display_summary, use_container_width=True, hide_index=True)

        st.bar_chart(display_summary.set_index("Sport")["Total Signals"], height=250)

# ── Auto-refresh ──────────────────────────────────────────────────────────────

import time
time.sleep(60)
st.rerun()
