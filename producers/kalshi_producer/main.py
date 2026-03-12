"""
Kalshi Producer
---------------
Polls Kalshi every 60 seconds for NBA and NCAAB game winner markets
and publishes raw records to the 'raw-kalshi-markets' Kafka topic.

Strategy:
  1. Fetch today's games from The Odds API (same data as odds producer)
  2. Construct Kalshi event tickers from team names + game date
  3. Fetch each game's markets directly from Kalshi by event ticker
  4. Publish to Kafka

Ticker format: KXNBAGAME-{YY}{MON}{DD}{AWAY_ABBR}{HOME_ABBR}
  e.g. Philadelphia at Detroit on Mar 12 2026 -> KXNBAGAME-26MAR12PHIDET
       Brooklyn at Atlanta on Mar 12 2026    -> KXNBAGAME-26MAR12BKNATL
"""

import logging
import sys
import time
from datetime import datetime, timezone

import requests

sys.path.insert(0, "/app")
from msk_producer import create_producer, send_message
from secrets_helper import get_kalshi_credentials, get_odds_api_key

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("kalshi_producer")

# --- Config ---
TOPIC = "raw-kalshi-markets"
POLL_INTERVAL_SECONDS = 60
KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
ODDS_API_BASE_URL = "https://api.the-odds-api.com/v4/sports"

# Sports to pull games from (winner markets only — Kalshi tracks game winners)
ODDS_API_SPORTS = ["basketball_nba", "basketball_ncaab"]

# Kalshi event ticker prefixes per sport
KALSHI_SPORT_PREFIX = {
    "basketball_nba": "KXNBAGAME",
    "basketball_ncaab": "KXNCAAMBGAME",
}

# Full team name -> Kalshi 3-letter abbreviation
# Confirmed from live Kalshi tickers (PHI, DET, BKN, ATL, OKC, BOS)
NBA_TEAM_ABBR = {
    "Atlanta Hawks": "ATL",
    "Boston Celtics": "BOS",
    "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA",
    "Chicago Bulls": "CHI",
    "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL",
    "Denver Nuggets": "DEN",
    "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW",
    "Houston Rockets": "HOU",
    "Indiana Pacers": "IND",
    "Los Angeles Clippers": "LAC",
    "Los Angeles Lakers": "LAL",
    "Memphis Grizzlies": "MEM",
    "Miami Heat": "MIA",
    "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK",
    "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI",
    "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR",
    "Utah Jazz": "UTA",
    "Washington Wizards": "WAS",
}

# NCAAB: derive from Odds API team name dynamically (see build_ncaab_abbr)
# Kalshi uses first 3-4 letters of school name for NCAAB
# e.g. "Tennessee Volunteers" -> "TENN", "Auburn Tigers" -> "AUB"
NCAAB_TEAM_ABBR_OVERRIDES = {
    "Tennessee Volunteers": "TENN",
    "Auburn Tigers": "AUB",
    "Toledo Rockets": "TOL",
    "Bowling Green Falcons": "BG",
    "Texas Southern Tigers": "TXSO",
    "Alabama A&M Bulldogs": "AAMU",
    "George Mason Patriots": "GMU",
    "St. Bonaventure Bonnies": "SBU",
    "Miami Hurricanes": "MIA",
    "Louisville Cardinals": "LOU",
    "Seton Hall Pirates": "SHU",
    "Creighton Bluejays": "CRE",
    "Wisconsin Badgers": "WIS",
    "Washington Huskies": "WASH",
    "Utah State Aggies": "USU",
    "UNLV Rebels": "UNLV",
    "Arizona Wildcats": "ARIZ",
    "UCF Knights": "UCF",
    "Akron Zips": "AKR",
    "Buffalo Bulls": "BUF",
}


def build_ncaab_abbr(team_name: str) -> str:
    """
    Derive a Kalshi-style abbreviation for an NCAAB team.
    Uses override table first, then falls back to first word uppercased (max 4 chars).
    """
    if team_name in NCAAB_TEAM_ABBR_OVERRIDES:
        return NCAAB_TEAM_ABBR_OVERRIDES[team_name]
    # Fallback: first word of team name, up to 4 chars, uppercase
    first_word = team_name.split()[0].upper()
    return first_word[:4]


def build_kalshi_event_ticker(sport: str, away_team: str, home_team: str, game_date: str) -> str:
    """
    Construct a Kalshi event ticker from game metadata.

    Format: {PREFIX}-{YY}{MON}{DD}{AWAY}{HOME}
    Example: KXNBAGAME-26MAR12PHIDET

    game_date: ISO date string e.g. '2026-03-12'
    """
    prefix = KALSHI_SPORT_PREFIX.get(sport)
    if not prefix:
        return None

    dt = datetime.strptime(game_date, "%Y-%m-%d")
    date_str = dt.strftime("%y%b%d").upper()  # e.g. 26MAR12

    if sport == "basketball_nba":
        away_abbr = NBA_TEAM_ABBR.get(away_team)
        home_abbr = NBA_TEAM_ABBR.get(home_team)
    else:
        away_abbr = build_ncaab_abbr(away_team)
        home_abbr = build_ncaab_abbr(home_team)

    if not away_abbr or not home_abbr:
        logger.warning("Could not build abbr for %s @ %s (%s)", away_team, home_team, sport)
        return None

    return f"{prefix}-{date_str}{away_abbr}{home_abbr}"


def fetch_todays_games(api_key: str, sport: str) -> list:
    """Fetch today's games from Odds API to drive Kalshi lookups."""
    url = f"{ODDS_API_BASE_URL}/{sport}/odds/"
    params = {
        "apiKey": api_key,
        "regions": "us",
        "markets": "h2h",
        "oddsFormat": "american",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error("Failed to fetch Odds API games for %s: %s", sport, e)
        return []


def fetch_kalshi_markets(event_ticker: str, kalshi_api_key: str) -> list:
    """
    Fetch Kalshi winner markets for a specific game event ticker.
    Returns list of market dicts, empty list on failure/not found.
    """
    url = f"{KALSHI_BASE_URL}/markets"
    headers = {"Authorization": f"Bearer {kalshi_api_key}"}
    params = {"limit": 10, "event_ticker": event_ticker}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        if resp.status_code == 404:
            logger.debug("No Kalshi market found for event: %s", event_ticker)
            return []
        resp.raise_for_status()
        data = resp.json()
        markets = data.get("markets", [])
        logger.debug("Found %d Kalshi markets for %s", len(markets), event_ticker)
        return markets
    except Exception as e:
        logger.error("Error fetching Kalshi markets for %s: %s", event_ticker, e)
        return []


def main():
    logger.info("Starting Kalshi producer")
    odds_api_key = get_odds_api_key()
    kalshi_creds = get_kalshi_credentials()
    kalshi_api_key = kalshi_creds["api_key"]

    producer = create_producer()

    try:
        while True:
            total_sent = 0

            for sport in ODDS_API_SPORTS:
                games = fetch_todays_games(odds_api_key, sport)
                logger.info("Processing %d %s games for Kalshi lookup", len(games), sport)

                for game in games:
                    away_team = game.get("away_team")
                    home_team = game.get("home_team")
                    game_date = game.get("commence_time", "")[:10]  # ISO date

                    event_ticker = build_kalshi_event_ticker(
                        sport, away_team, home_team, game_date
                    )
                    if not event_ticker:
                        continue

                    markets = fetch_kalshi_markets(event_ticker, kalshi_api_key)

                    for market in markets:
                        record = {
                            "sport": sport,
                            "event_ticker": event_ticker,
                            "away_team": away_team,
                            "home_team": home_team,
                            "game_date": game_date,
                            "market_ticker": market.get("ticker"),
                            "title": market.get("title"),
                            "yes_ask_dollars": market.get("yes_ask_dollars"),
                            "yes_bid_dollars": market.get("yes_bid_dollars"),
                            "no_ask_dollars": market.get("no_ask_dollars"),
                            "no_bid_dollars": market.get("no_bid_dollars"),
                            "status": market.get("status"),
                            "volume": market.get("volume"),
                            "open_interest": market.get("open_interest"),
                        }
                        key = f"{event_ticker}:{market.get('ticker', 'unknown')}"
                        send_message(producer, TOPIC, record, key=key)
                        total_sent += 1

                    # Small delay to be respectful to Kalshi API
                    time.sleep(0.2)

            producer.flush()
            logger.info(
                "Flushed %d Kalshi market records. Sleeping %ds...",
                total_sent,
                POLL_INTERVAL_SECONDS,
            )
            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Shutting down Kalshi producer")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
