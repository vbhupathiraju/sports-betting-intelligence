"""
Game Events Producer
--------------------
Polls The Odds API every 30 seconds for live game scores/events
and publishes to the 'raw-game-events' Kafka topic.

This producer tracks:
  - Live scores (in-game)
  - Game status changes (scheduled -> in_progress -> completed)
  - Final scores

Used downstream for:
  - Triggering signal generation when games start
  - Settling divergence signals when games complete
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
logger = logging.getLogger("game_events_producer")

# --- Config ---
TOPIC = "raw-game-events"
POLL_INTERVAL_SECONDS = 30  # More frequent than odds — tracks live state changes
BASE_URL = "https://api.the-odds-api.com/v4/sports"

SPORTS = [
    "basketball_nba",
    "basketball_ncaab",
]


def fetch_scores(api_key: str, sport: str, days_from: int = 1) -> list:
    """
    Fetch scores/events for a sport.
    days_from=1 includes today's completed games as well as live.
    """
    url = f"{BASE_URL}/{sport}/scores/"
    params = {
        "apiKey": api_key,
        "daysFrom": days_from,
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        logger.info(
            "Fetched %d score events for %s | quota remaining: %s",
            len(data),
            sport,
            resp.headers.get("x-requests-remaining", "unknown"),
        )
        return data
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 422:
            logger.warning("Sport %s not available (422)", sport)
            return []
        logger.error("HTTP error fetching scores for %s: %s", sport, e)
        return []
    except Exception as e:
        logger.error("Error fetching scores for %s: %s", sport, e)
        return []


def main():
    logger.info("Starting Game Events producer")
    api_key = get_odds_api_key()
    producer = create_producer()

    try:
        while True:
            for sport in SPORTS:
                events = fetch_scores(api_key, sport)
                for event in events:
                    record = {
                        "sport": sport,
                        "game_id": event.get("id"),
                        "sport_key": event.get("sport_key"),
                        "sport_title": event.get("sport_title"),
                        "commence_time": event.get("commence_time"),
                        "completed": event.get("completed", False),
                        "home_team": event.get("home_team"),
                        "away_team": event.get("away_team"),
                        "scores": event.get("scores"),  # None if not started
                        "last_update": event.get("last_update"),
                    }
                    key = f"{sport}:{event.get('id', 'unknown')}"
                    send_message(producer, TOPIC, record, key=key)

            producer.flush()
            logger.info("Flushed game events. Sleeping %ds...", POLL_INTERVAL_SECONDS)
            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Shutting down Game Events producer")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
