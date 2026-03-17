"""
Odds API Producer
-----------------
Polls The Odds API for game odds and publishes raw records to the
'raw-odds' Kafka topic.

Sports, schedules, and poll intervals are driven by sports_config.json.
Edit that file to add/remove sports or adjust schedules — no code change needed.
Config is hot-reloaded each poll cycle so changes take effect within one cycle.

Active sports and their Odds API keys are read from config.
Championship winner markets are not included — game markets only.
"""

import logging
import sys
import time

import requests

sys.path.insert(0, "/app")
from config_loader import (
    get_active_sports,
    get_poll_interval,
    is_within_schedule,
    load_config,
    seconds_until_next_window,
)
from msk_producer import create_producer, send_message
from secrets_helper import get_odds_api_key

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("odds_api_producer")

# --- Config ---
TOPIC = "raw-odds"
BASE_URL = "https://api.the-odds-api.com/v4/sports"
REGIONS = "us"
MARKETS = "h2h,spreads,totals"
ODDS_FORMAT = "american"


def fetch_odds(api_key: str, odds_api_key: str) -> list:
    """Fetch current odds for a sport from The Odds API."""
    url = f"{BASE_URL}/{odds_api_key}/odds/"
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
            odds_api_key,
            resp.headers.get("x-requests-remaining", "unknown"),
        )
        return data
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 422:
            logger.warning("Sport %s not available (422): %s", odds_api_key, e)
            return []
        logger.error("HTTP error fetching %s: %s", odds_api_key, e)
        return []
    except Exception as e:
        logger.error("Error fetching %s: %s", odds_api_key, e)
        return []


def main():
    logger.info("Starting Odds API producer")
    api_key = get_odds_api_key()
    producer = create_producer()

    try:
        while True:
            # Hot-reload config each cycle
            config = load_config()
            active_sports = get_active_sports(config)

            # Determine which sports are within their schedule window
            in_window = [
                s for s in active_sports
                if is_within_schedule(config["sports"][s])
            ]
            out_of_window = [
                s for s in active_sports
                if not is_within_schedule(config["sports"][s])
            ]

            if out_of_window:
                logger.info(
                    "Sports outside schedule window (skipping): %s",
                    out_of_window,
                )

            if not in_window:
                # Smart sleep: find the nearest next window open across all sports
                sleep_secs = min(
                    seconds_until_next_window(config["sports"][s])
                    for s in active_sports
                )
                sleep_secs = min(sleep_secs, 300)  # cap at 5 min
                logger.info(
                    "No active sports in window. Sleeping %ds until next window...",
                    sleep_secs,
                )
                time.sleep(sleep_secs)
                continue

            # Poll each in-window sport
            for sport_key in in_window:
                sport_cfg = config["sports"][sport_key]
                odds_api_key = sport_cfg["odds_api_key"]
                games = fetch_odds(api_key, odds_api_key)
                for game in games:
                    game["sport_key"] = sport_key
                    key = f"{sport_key}:{game.get('id', 'unknown')}"
                    send_message(producer, TOPIC, game, key=key)

            producer.flush()

            # Use the shortest active poll interval among in-window sports
            sleep_secs = min(
                get_poll_interval(config["sports"][s]) for s in in_window
            )
            logger.info(
                "Flushed all messages. Sleeping %ds...", sleep_secs
            )
            time.sleep(sleep_secs)

    except KeyboardInterrupt:
        logger.info("Shutting down Odds API producer")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
