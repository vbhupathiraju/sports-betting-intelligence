"""
Odds API Producer
-----------------
Polls The Odds API every 60 seconds for NBA and NCAAB game odds
and publishes raw records to the 'raw-odds' Kafka topic.

Sports covered:
  - basketball_nba
  - basketball_nba_championship_winner
  - basketball_ncaab
  - basketball_ncaab_championship_winner
  - basketball_wncaab
"""

import logging
import sys
import time

import requests

sys.path.insert(0, "/app")
from msk_producer import create_producer, send_message
from secrets_helper import get_odds_api_key

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("odds_api_producer")

# --- Config ---
TOPIC = "raw-odds"
POLL_INTERVAL_SECONDS = 60
BASE_URL = "https://api.the-odds-api.com/v4/sports"
REGIONS = "us"
MARKETS = "h2h,spreads,totals"
ODDS_FORMAT = "american"

SPORTS = [
    "basketball_nba",
    "basketball_nba_championship_winner",
    "basketball_ncaab",
    "basketball_ncaab_championship_winner",
    "basketball_wncaab",
]


def fetch_odds(api_key: str, sport: str) -> list:
    """Fetch current odds for a sport from The Odds API."""
    url = f"{BASE_URL}/{sport}/odds/"
    params = {
        "apiKey": api_key,
        "regions": REGIONS,
        "markets": MARKETS,
        "oddsFormat": ODDS_FORMAT,
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        logger.info(
            "Fetched %d games for %s | quota remaining: %s",
            len(data),
            sport,
            resp.headers.get("x-requests-remaining", "unknown"),
        )
        return data
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 422:
            # Sport not currently available (e.g. WNCAAB off-season)
            logger.warning("Sport %s not available (422): %s", sport, e)
            return []
        logger.error("HTTP error fetching %s: %s", sport, e)
        return []
    except Exception as e:
        logger.error("Error fetching %s: %s", sport, e)
        return []


def main():
    logger.info("Starting Odds API producer")
    api_key = get_odds_api_key()
    producer = create_producer()

    try:
        while True:
            for sport in SPORTS:
                games = fetch_odds(api_key, sport)
                for game in games:
                    # Add sport key for downstream filtering
                    game["sport_key"] = sport
                    key = f"{sport}:{game.get('id', 'unknown')}"
                    send_message(producer, TOPIC, game, key=key)

            producer.flush()
            logger.info("Flushed all messages. Sleeping %ds...", POLL_INTERVAL_SECONDS)
            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Shutting down Odds API producer")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
