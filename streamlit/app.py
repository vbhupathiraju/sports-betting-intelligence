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

def fmt_date(val):
    try:
        return pd.Timestamp(val).strftime("%b %d")
    except:
        return ""

def game_label(row):
    date_str = fmt_date(row['commence_time'])
    return f"{row['away_team']} @ {row['home_team']} ({date_str})"

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

# Date filter — default to today
if not div_df.empty:
    available_dates = sorted(
        div_df["commence_time"].apply(lambda x: pd.Timestamp(x).date()).unique(),
        reverse=True
    )
elif not sharp_df.empty:
    available_dates = sorted(
        sharp_df["commence_time"].apply(lambda x: pd.Timestamp(x).date()).unique(),
        reverse=True
    )
else:
    available_dates = []

today = pd.Timestamp.utcnow().date()
default_date = available_dates[0] if available_dates else today

selected_date = st.selectbox(
    "Filter by game date",
    options=available_dates,
    index=0,
    format_func=lambda d: d.strftime("%B %d, %Y") + (" (today)" if d == today else "")
)

# Filter by date
def filter_by_date(df, date):
    if df.empty:
        return df
    return df[df["commence_time"].apply(lambda x: pd.Timestamp(x).date()) == date].copy()

div_date_filtered = filter_by_date(div_df, selected_date)
sharp_date_filtered = filter_by_date(sharp_df, selected_date)

# Sport filter
all_sports = sorted(set(
    list(div_date_filtered["sport_key"].unique() if not div_date_filtered.empty else []) +
    list(sharp_date_filtered["sport_key"].unique() if not sharp_date_filtered.empty else [])
))
sport_options = ["All Sports"] + [fmt_sport(s) for s in all_sports]
selected_sport_label = st.selectbox("Filter by sport", sport_options)

if selected_sport_label == "All Sports":
    selected_sports = all_sports
else:
    selected_sports = [k for k, v in SPORT_LABELS.items() if v == selected_sport_label]

div_filtered = div_date_filtered[div_date_filtered["sport_key"].isin(selected_sports)].copy() if not div_date_filtered.empty else div_date_filtered
sharp_filtered = sharp_date_filtered[sharp_date_filtered["sport_key"].isin(selected_sports)].copy() if not sharp_date_filtered.empty else sharp_date_filtered

# Game filter
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
    st.caption("Total signals by sport and game")

    if summary_df.empty:
        st.info("No summary data available.")
    else:
        summary_filtered = summary_df[summary_df["sport_key"].isin(selected_sports)].copy()
        summary_filtered["game_date"] = summary_filtered.apply(
            lambda r: pd.Timestamp(r["latest_computed_at"]).date(), axis=1
        )
        summary_filtered = summary_filtered[
            summary_filtered["home_team"].apply(
                lambda ht: any(
                    (div_date_filtered["home_team"] == ht).any() if not div_date_filtered.empty else False
                )
            )
        ] if not div_date_filtered.empty else summary_filtered

        sport_rollup = (
            summary_filtered.groupby("sport_key")["signal_count"]
            .sum()
            .reset_index()
        )
        sport_rollup["sport_key"] = sport_rollup["sport_key"].apply(fmt_sport)
        sport_rollup.columns = ["Sport", "Total Signals"]

        st.bar_chart(sport_rollup.set_index("Sport")["Total Signals"], height=200)
        st.caption("Total signals by sport")

        st.divider()

        for sport_key in sorted(summary_filtered["sport_key"].unique()):
            sport_data = summary_filtered[summary_filtered["sport_key"] == sport_key].copy()
            sport_total = sport_data["signal_count"].sum()
            st.markdown(f"#### {fmt_sport(sport_key)} — {sport_total} total signals")

            display = sport_data[["away_team", "home_team", "signal_count", "latest_computed_at"]].copy()
            display["game"] = display.apply(lambda r: f"{r['away_team']} @ {r['home_team']}", axis=1)
            display["latest_computed_at"] = display["latest_computed_at"].apply(fmt_ts)
            display = display[["game", "signal_count", "latest_computed_at"]]
            display.columns = ["Game", "Signal Count", "Latest Computed At"]
            display = display.sort_values("Signal Count", ascending=False)
            st.dataframe(display, use_container_width=True, hide_index=True)

# ── Auto-refresh ──────────────────────────────────────────────────────────────

import time
time.sleep(60)
st.rerun()
