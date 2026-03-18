import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.graph_objects as go
import requests

st.set_page_config(
    page_title="Sports Betting Intelligence",
    page_icon="📊",
    layout="wide"
)

# ── Dark chart theme ──────────────────────────────────────────────────────────

DARK_BG = "#0d1117"
GRID_COLOR = "#21262d"
BORDER_COLOR = "#30363d"
TEXT_COLOR = "#e6edf3"
MUTED_COLOR = "#8b949e"

def dark_layout(title="", height=400, y_title="", x_tickangle=0, y_tickformat=""):
    layout = dict(
        plot_bgcolor=DARK_BG,
        paper_bgcolor=DARK_BG,
        font=dict(color=TEXT_COLOR, family="monospace"),
        height=height,
        margin=dict(l=50, r=20, t=40, b=60),
        hovermode="x unified",
        legend=dict(
            bgcolor="#161b22",
            bordercolor=BORDER_COLOR,
            borderwidth=1,
            font=dict(color=TEXT_COLOR),
        ),
        xaxis=dict(
            gridcolor=GRID_COLOR,
            linecolor=BORDER_COLOR,
            tickfont=dict(color=MUTED_COLOR),
            tickangle=x_tickangle,
        ),
        yaxis=dict(
            gridcolor=GRID_COLOR,
            linecolor=BORDER_COLOR,
            tickfont=dict(color=MUTED_COLOR),
            title=dict(text=y_title, font=dict(color=MUTED_COLOR)),
            tickformat=y_tickformat,
        ),
    )
    if title:
        layout["title"] = dict(text=title, font=dict(color=TEXT_COLOR, size=14), x=0)
    return layout

TEAM_COLORS = [
    "#58a6ff", "#3fb950", "#f78166", "#d2a8ff",
    "#ffa657", "#79c0ff", "#56d364", "#ff7b72",
]

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

@st.cache_data(ttl=60)
def load_divergence_history(home_team, away_team):
    conn = get_snowflake_connection()
    query = f"""
        SELECT market_ticker, computed_at, kalshi_implied_prob, sportsbook_home_prob, divergence
        FROM SPORTS_BETTING.PUBLIC.divergence_signals
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
        return f"Q{period}" if period <= 4 else f"OT{period - 4}"
    if sport_key == "basketball_ncaab":
        return f"H{period}" if period <= 2 else f"OT{period - 2}"
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
        badge = f"🔴 LIVE — {period_str} {clock}".strip()
        badge_color = "#f85149"
    elif state == "post":
        badge = "✅ Final"
        badge_color = "#3fb950"
    else:
        badge = f"🕐 {status}"
        badge_color = "#8b949e"

    st.markdown(
        f"""
        <div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px 24px;margin-bottom:12px">
          <div style="text-align:center;margin-bottom:12px">
            <span style="background:#21262d;color:{badge_color};padding:4px 12px;border-radius:12px;font-size:12px;font-weight:600">{badge}</span>
          </div>
          <div style="display:flex;justify-content:space-around;align-items:center">
            <div style="text-align:center">
              <div style="font-size:13px;color:#8b949e;margin-bottom:4px">{away_team}</div>
              <div style="font-size:36px;font-weight:700;color:#e6edf3;font-family:monospace">{away_score}</div>
            </div>
            <div style="color:#30363d;font-size:24px">—</div>
            <div style="text-align:center">
              <div style="font-size:13px;color:#8b949e;margin-bottom:4px">{home_team}</div>
              <div style="font-size:36px;font-weight:700;color:#e6edf3;font-family:monospace">{home_score}</div>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )

# ── Odds movement charts ──────────────────────────────────────────────────────

def render_divergence_chart(home_team, away_team, key_prefix=""):
    history = load_divergence_history(home_team, away_team)
    if history.empty:
        return
    history["computed_at"] = pd.to_datetime(history["computed_at"], utc=True)
    history["computed_at_pst"] = history["computed_at"].dt.tz_convert("America/Los_Angeles")

    chart_config = {"displayModeBar": False}
    fig = go.Figure()
    layout = dark_layout(title="Kalshi vs Sportsbook Implied Probability (PST)", height=350, y_title="Implied Prob", y_tickformat=".0%")
    layout["xaxis"]["rangeslider"] = dict(visible=False)
    layout["xaxis"]["title"] = ""
    layout["margin"] = dict(l=50, r=150, t=50, b=40)
    layout["legend"] = dict(
        bgcolor="#161b22", bordercolor="#30363d", borderwidth=1,
        font=dict(color="#e6edf3"),
        itemclick="toggle", itemdoubleclick="toggleothers",
        title=dict(text="click to toggle", font=dict(color="#8b949e", size=10)),
        orientation="v", x=1.01, xanchor="left", y=1, yanchor="top",
    )
    fig.update_layout(**layout)

    # One line per market_ticker for Kalshi prob
    for i, (ticker, grp) in enumerate(history.groupby("market_ticker")):
        grp = grp.sort_values("computed_at_pst")
        short = ticker.split("-")[-1] if "-" in ticker else ticker
        fig.add_trace(go.Scatter(
            x=grp["computed_at_pst"],
            y=grp["kalshi_implied_prob"],
            mode="lines+markers",
            name=f"Kalshi {short}",
            line=dict(color=TEAM_COLORS[i % len(TEAM_COLORS)], width=2, dash="solid"),
            marker=dict(size=5),
            hovertemplate=f"<b>Kalshi {short}</b><br>Prob: %{{y:.1%}}<br>%{{x|%b %d %H:%M PST}}<extra></extra>",
        ))

    # Sportsbook avg (deduplicated per computed_at)
    sb = history.drop_duplicates(subset=["computed_at_pst"]).sort_values("computed_at_pst")
    fig.add_trace(go.Scatter(
        x=sb["computed_at_pst"],
        y=sb["sportsbook_home_prob"],
        mode="lines+markers",
        name="Sportsbook avg",
        line=dict(color="#f78166", width=2, dash="dot"),
        marker=dict(size=5),
        hovertemplate="<b>Sportsbook avg</b><br>Prob: %{y:.1%}<br>%{x|%b %d %H:%M PST}<extra></extra>",
    ))

    # Divergence as filled area
    fig.add_trace(go.Scatter(
        x=sb["computed_at_pst"],
        y=sb["divergence"],
        mode="lines",
        name="Divergence",
        line=dict(color="#d2a8ff", width=1, dash="dash"),
        fill="tozeroy",
        fillcolor="rgba(210,168,255,0.08)",
        hovertemplate="<b>Divergence</b><br>%{y:.1%}<br>%{x|%b %d %H:%M PST}<extra></extra>",
        yaxis="y2",
    ))
    fig.update_layout(
        yaxis2=dict(
            overlaying="y",
            side="right",
            title="Divergence",
            tickformat=".0%",
            gridcolor="#21262d",
            tickfont=dict(color="#8b949e"),
            title_font=dict(color="#8b949e"),
            showgrid=False,
        )
    )
    st.plotly_chart(fig, width="stretch", key=f"{key_prefix}_div", config=chart_config)

def render_odds_charts(home_team, away_team, key_prefix=""):
    history = load_odds_history(home_team, away_team)
    if history.empty:
        st.caption("No odds movement history available yet — data accumulates as the pipeline runs.")
        return

    history["computed_at"] = pd.to_datetime(history["computed_at"], utc=True)
    combos = list(history.groupby(["team", "bookmaker_key"]))

    # Time range buttons for Plotly
    rangeselector = dict(
        buttons=[
            dict(label="Game Start", step="all"),
            dict(count=6,  label="6h",  step="hour", stepmode="backward"),
            dict(count=24, label="24h", step="hour", stepmode="backward"),
        ],
        bgcolor="#21262d",
        activecolor="#58a6ff",
        bordercolor="#30363d",
        font=dict(color="#e6edf3", size=11),
        x=0,
        y=1.08,
    )

    chart_config = {"displayModeBar": False}

    # American odds chart
    fig_odds = go.Figure()
    layout_odds = dark_layout(title="American Odds Movement", height=400, y_title="American Odds")
    layout_odds["xaxis"]["rangeselector"] = rangeselector
    layout_odds["xaxis"]["rangeslider"] = dict(visible=False)
    layout_odds["legend"] = dict(
        bgcolor="#161b22", bordercolor="#30363d", borderwidth=1,
        font=dict(color="#e6edf3"),
        itemclick="toggle", itemdoubleclick="toggleothers",
        title=dict(text="Click to toggle · Double-click to isolate", font=dict(color="#8b949e", size=10)),
    )
    fig_odds.update_layout(**layout_odds)
    for i, ((team, book), grp) in enumerate(combos):
        grp = grp.sort_values("computed_at")
        color = TEAM_COLORS[i % len(TEAM_COLORS)]
        fig_odds.add_trace(go.Scatter(
            x=grp["computed_at"],
            y=grp["american_odds"],
            mode="lines+markers",
            name=f"{team} ({book})",
            line=dict(color=color, width=2),
            marker=dict(size=6, color=color),
            hovertemplate=f"<b>{team} ({book})</b><br>Odds: %{{y}}<br>Time: %{{x}}<extra></extra>",
        ))
    st.plotly_chart(fig_odds, width="stretch", key=f"{key_prefix}_odds", config=chart_config)

    # Implied probability chart
    fig_prob = go.Figure()
    layout_prob = dark_layout(title="Implied Probability Movement", height=400, y_title="Implied Probability", y_tickformat=".0%")
    layout_prob["xaxis"]["rangeselector"] = rangeselector
    layout_prob["xaxis"]["rangeslider"] = dict(visible=False)
    layout_prob["legend"] = dict(
        bgcolor="#161b22", bordercolor="#30363d", borderwidth=1,
        font=dict(color="#e6edf3"),
        itemclick="toggle", itemdoubleclick="toggleothers",
        title=dict(text="Click to toggle · Double-click to isolate", font=dict(color="#8b949e", size=10)),
    )
    fig_prob.update_layout(**layout_prob)
    for i, ((team, book), grp) in enumerate(combos):
        grp = grp.sort_values("computed_at")
        color = TEAM_COLORS[i % len(TEAM_COLORS)]
        fig_prob.add_trace(go.Scatter(
            x=grp["computed_at"],
            y=grp["current_implied_prob"],
            mode="lines+markers",
            name=f"{team} ({book})",
            line=dict(color=color, width=2),
            marker=dict(size=6, color=color),
            hovertemplate=f"<b>{team} ({book})</b><br>Prob: %{{y:.1%}}<br>Time: %{{x}}<extra></extra>",
        ))
    st.plotly_chart(fig_prob, width="stretch", key=f"{key_prefix}_prob", config=chart_config)

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

# ── Pre-load ESPN scores ──────────────────────────────────────────────────────

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
        chart_data = (
            div_filtered.groupby(div_filtered.apply(game_label, axis=1))["divergence"]
            .mean()
            .reset_index()
        )
        chart_data.columns = ["game", "avg_divergence"]
        chart_data["avg_divergence_pct"] = (chart_data["avg_divergence"] * 100).round(1)
        chart_data = chart_data.sort_values("avg_divergence_pct", ascending=False)

        fig_summary = go.Figure(go.Bar(
            x=chart_data["game"],
            y=chart_data["avg_divergence_pct"],
            marker_color="#58a6ff",
            hovertemplate="%{x}<br>Avg Divergence: %{y:.1f}%<extra></extra>",
        ))
        fig_summary.update_layout(**dark_layout(
            title="Average Divergence % by Game",
            height=300,
            y_title="Avg Divergence %",
            x_tickangle=-20,
        ))
        st.plotly_chart(fig_summary, width="stretch", key="div_summary")
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
                render_divergence_chart(home, away, key_prefix="div_" + game.replace(" ", "_").replace("@", "at").replace("(", "").replace(")", "").replace(",", ""))
                render_odds_charts(home, away, key_prefix="div_" + game.replace(" ", "_").replace("@", "at").replace("(", "").replace(")", "").replace(",", ""))
                with st.expander("Show raw data", expanded=False):
                    display = game_data[[
                        "market_ticker", "kalshi_implied_prob", "sportsbook_home_prob",
                        "divergence", "signal_direction", "num_bookmakers", "commence_time"
                    ]].copy()
                    display["kalshi_implied_prob"] = display["kalshi_implied_prob"].apply(fmt_pct)
                    display["sportsbook_home_prob"] = display["sportsbook_home_prob"].apply(fmt_pct)
                    display["divergence"] = display["divergence"].apply(fmt_pct)
                    display["commence_time"] = display["commence_time"].apply(fmt_ts)
                    display.columns = ["Market Ticker", "Kalshi Prob", "Sportsbook Prob", "Divergence", "Direction", "# Bookmakers", "Game Time"]
                    st.dataframe(display, width="stretch", hide_index=True)

# ── Tab 2: Sharp Money Signals ────────────────────────────────────────────────

with tab2:
    st.subheader("Sharp Money Signals")
    st.caption("Moneyline movement ≥10 American odds points between polls")

    if sharp_filtered.empty:
        st.info("No sharp money signals for the selected filters.")
    else:
        chart_data2 = (
            sharp_filtered.groupby(sharp_filtered.apply(game_label, axis=1))["prob_movement"]
            .mean()
            .reset_index()
        )
        chart_data2.columns = ["game", "avg_prob_movement"]
        chart_data2["avg_prob_movement_pct"] = (chart_data2["avg_prob_movement"] * 100).round(1)
        chart_data2 = chart_data2.sort_values("avg_prob_movement_pct", ascending=False)

        fig_summary2 = go.Figure(go.Bar(
            x=chart_data2["game"],
            y=chart_data2["avg_prob_movement_pct"],
            marker_color="#3fb950",
            hovertemplate="%{x}<br>Avg Prob Movement: %{y:.1f}%<extra></extra>",
        ))
        fig_summary2.update_layout(**dark_layout(
            title="Average Probability Movement % by Game",
            height=300,
            y_title="Avg Prob Movement %",
            x_tickangle=-20,
        ))
        st.plotly_chart(fig_summary2, width="stretch", key="sharp_summary")
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
                render_divergence_chart(home, away, key_prefix="sharp_" + game.replace(" ", "_").replace("@", "at").replace("(", "").replace(")", "").replace(",", ""))
                render_odds_charts(home, away, key_prefix="sharp_" + game.replace(" ", "_").replace("@", "at").replace("(", "").replace(")", "").replace(",", ""))
                with st.expander("Show raw data", expanded=False):
                    display = game_data[[
                        "team", "bookmaker_key", "prev_odds", "american_odds",
                        "prob_movement", "movement_direction", "commence_time"
                    ]].copy()
                    display["prev_odds"] = display["prev_odds"].apply(fmt_odds)
                    display["american_odds"] = display["american_odds"].apply(fmt_odds)
                    display["prob_movement"] = display["prob_movement"].apply(fmt_pct)
                    display["commence_time"] = display["commence_time"].apply(fmt_ts)
                    display.columns = ["Team", "Bookmaker", "Prev Odds", "Current Odds", "Prob Movement", "Direction", "Game Time"]
                    st.dataframe(display, width="stretch", hide_index=True)

# ── Auto-refresh ──────────────────────────────────────────────────────────────

import time
time.sleep(60)
st.rerun()
