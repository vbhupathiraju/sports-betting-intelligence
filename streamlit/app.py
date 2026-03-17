import streamlit as st
import snowflake.connector
import pandas as pd
import requests
from datetime import timezone

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
def load_odds_history(home_team, away_team):
    conn = get_snowflake_connection()
    query = f"""
        SELECT team, bookmaker_key, computed_at, american_odds,
               prev_implied_prob, current_implied_prob
        FROM SPORTS_BETTING.PUBLIC.sharp_money_signals
        WHERE home_team = '{home_team}' AND away_team = '{away_team}'
        ORDER BY computed_at ASC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    df.columns = [c.lower() for c in df.columns]
    return df

@st.cache_data(ttl=30)
def load_espn_scores(sport_key):
    espn_paths = {
        "basketball_nba": "basketball/nba",
        "basketball_ncaab": "basketball/mens-college-basketball",
    }
    path = espn_paths.get(sport_key)
    if not path:
        return {}
    try:
        url = f"https://site.api.espn.com/apis/site/v2/sports/{path}/scoreboard"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        scores = {}
        for event in data.get("events", []):
            for comp in event.get("competitions", []):
                competitors = comp.get("competitors", [])
                if len(competitors) < 2:
                    continue
                home = next((c for c in competitors if c.get("homeAway") == "home"), None)
                away = next((c for c in competitors if c.get("homeAway") == "away"), None)
                if not home or not away:
                    continue
                home_name = home["team"]["displayName"]
                away_name = away["team"]["displayName"]
                status = comp.get("status", {})
                status_type = status.get("type", {})
                game_key = f"{away_name}|{home_name}"
                scores[game_key] = {
                    "home_team": home_name,
                    "away_team": away_name,
                    "home_score": home.get("score", "0"),
                    "away_score": away.get("score", "0"),
                    "status": status_type.get("description", "Scheduled"),
                    "state": status_type.get("state", "pre"),
                    "period": status.get("period", 0),
                    "display_clock": status.get("displayClock", ""),
                }
        return scores
    except Exception:
        return {}

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

def period_label(sport_key, period):
    if period == 0:
        return ""
    if sport_key == "basketball_nba":
        if period <= 4:
            return f"Q{period}"
        return f"OT{period - 4}"
    if sport_key == "basketball_ncaab":
        if period <= 2:
            return f"H{period}"
        return f"OT{period - 2}"
    return f"P{period}"

# ── Scoreboard widget ─────────────────────────────────────────────────────────

def render_scoreboard(away_team, home_team, sport_key, scores_cache):
    game_key = f"{away_team}|{home_team}"
    score = scores_cache.get(game_key)
    if not score:
        st.caption("📡 Live score unavailable")
        return

    state = score["state"]
    status = score["status"]
    period = score["period"]
    clock = score["display_clock"]
    away_score = score["away_score"]
    home_score = score["home_score"]

    if state == "in":
        period_str = period_label(sport_key, period)
        status_display = f"🔴 LIVE — {period_str} {clock}".strip()
    elif state == "post":
        status_display = f"✅ Final"
    else:
        status_display = f"🕐 {status}"

    col1, col2, col3 = st.columns([2, 1, 2])
    with col1:
        st.metric(away_team, away_score)
    with col2:
        st.markdown(f"<div style='text-align:center;padding-top:20px;font-size:12px;color:gray'>{status_display}</div>", unsafe_allow_html=True)
    with col3:
        st.metric(home_team, home_score)

# ── Odds movement charts ──────────────────────────────────────────────────────

def render_odds_charts(home_team, away_team):
    history = load_odds_history(home_team, away_team)
    if history.empty:
        st.caption("No odds movement history available yet.")
        return

    history["computed_at"] = pd.to_datetime(history["computed_at"], utc=True)

    # American odds chart
    st.markdown("**American Odds Movement**")
    odds_pivot = history.pivot_table(
        index="computed_at",
        columns=["team", "bookmaker_key"],
        values="american_odds",
        aggfunc="mean"
    )
    odds_pivot.columns = [f"{t} ({b})" for t, b in odds_pivot.columns]
    st.line_chart(odds_pivot, height=200)

    # Implied probability chart
    st.markdown("**Implied Probability Movement**")
    prob_pivot = history.pivot_table(
        index="computed_at",
        columns=["team", "bookmaker_key"],
        values="current_implied_prob",
        aggfunc="mean"
    )
    prob_pivot.columns = [f"{t} ({b})" for t, b in prob_pivot.columns]
    st.line_chart(prob_pivot, height=200)

# ── Load data ─────────────────────────────────────────────────────────────────

try:
    div_df = load_divergence_signals()
    sharp_df = load_sharp_money_signals()
    load_error = None
except Exception as e:
    div_df = pd.DataFrame()
    sharp_df = pd.DataFrame()
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

selected_date = st.selectbox(
    "Filter by game date",
    options=available_dates,
    index=0,
    format_func=lambda d: d.strftime("%B %d, %Y") + (" (today)" if d == today else "")
)

def filter_by_date(df, date):
    if df.empty:
        return df
    return df[df["commence_time"].apply(lambda x: pd.Timestamp(x).date()) == date].copy()

div_date_filtered = filter_by_date(div_df, selected_date)
sharp_date_filtered = filter_by_date(sharp_df, selected_date)

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

# ── Pre-load ESPN scores for all relevant sports ──────────────────────────────

espn_scores = {}
for sport in selected_sports:
    espn_scores.update(load_espn_scores(sport))

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab1, tab2 = st.tabs(["🔀 Divergence Signals", "💰 Sharp Money Signals"])

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
            sport_key = game_data["sport_key"].iloc[0]
            home = game_data["home_team"].iloc[0]
            away = game_data["away_team"].iloc[0]
            sport = fmt_sport(sport_key)
            n = len(game_data)
            max_div = game_data["divergence"].max()
            with st.expander(f"**{game}** — {sport} · {n} signal(s) · max divergence {fmt_pct(max_div)}", expanded=False):
                render_scoreboard(away, home, sport_key, espn_scores)
                st.divider()
                render_odds_charts(home, away)
                st.divider()
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
            sport_key = game_data["sport_key"].iloc[0]
            home = game_data["home_team"].iloc[0]
            away = game_data["away_team"].iloc[0]
            sport = fmt_sport(sport_key)
            n = len(game_data)
            max_move = game_data["prob_movement"].max()
            with st.expander(f"**{game}** — {sport} · {n} signal(s) · max prob movement {fmt_pct(max_move)}", expanded=False):
                render_scoreboard(away, home, sport_key, espn_scores)
                st.divider()
                render_odds_charts(home, away)
                st.divider()
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

# ── Auto-refresh ──────────────────────────────────────────────────────────────

import time
time.sleep(60)
st.rerun()
