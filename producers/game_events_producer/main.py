"""
Game Events Producer
--------------------
Polls ESPN's public API every 30 seconds for live game scores/events
and publishes to the 'raw-game-events' Kafka topic.

Uses ESPN's undocumented but publicly accessible scoreboard endpoint.
No API key required — zero Odds API quota consumed.

This producer tracks:
  - Live scores (in-game)
  - Game status changes (scheduled -> in_progress -> completed)
  - Final scores

Used downstream for:
  - Driving kalshi_producer game lookups (replaces Odds API dependency)
  - Triggering signal generation when games start
  - Settling divergence signals when games complete

ESPN endpoints:
  - NBA:   https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard
  - NCAAB: https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?groups=50&limit=365
"""

import logging
import sys
import time
from datetime import datetime, timezone

import requests

sys.path.insert(0, "/app")
from msk_producer import create_producer, send_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("game_events_producer")

# --- Config ---
TOPIC = "raw-game-events"
POLL_INTERVAL_SECONDS = 30
ESPN_BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball"

# Maps internal sport key (matches Odds API convention used by kalshi_producer)
# to ESPN league path + any extra query params needed
ESPN_SPORTS = {
    "basketball_nba": {
        "path": "nba/scoreboard",
        "params": {},
    },
    "basketball_ncaab": {
        "path": "mens-college-basketball/scoreboard",
        # groups=50 includes all D1 games; limit=365 ensures we get every game
        "params": {"groups": "50", "limit": "365"},
    },
}


def fetch_espn_scoreboard(sport_key: str) -> list:
    """
    Fetch today's scoreboard from ESPN for a given sport.
    Returns a list of normalized game event dicts, empty list on failure.
    """
    sport_cfg = ESPN_SPORTS.get(sport_key)
    if not sport_cfg:
        logger.warning("Unknown sport key: %s", sport_key)
        return []

    url = f"{ESPN_BASE_URL}/{sport_cfg['path']}"
    params = sport_cfg["params"]

    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        events = data.get("events", [])
        logger.info(
            "Fetched %d ESPN events for %s",
            len(events),
            sport_key,
        )
        return events
    except requests.exceptions.HTTPError as e:
        logger.error("HTTP error fetching ESPN scoreboard for %s: %s", sport_key, e)
        return []
    except Exception as e:
        logger.error("Error fetching ESPN scoreboard for %s: %s", sport_key, e)
        return []


def parse_espn_event(event: dict, sport_key: str) -> dict | None:
    """
    Parse a raw ESPN event dict into a normalized game record.

    ESPN event structure:
      event.id                                    -> game_id
      event.date                                  -> commence_time (ISO)
      event.status.type.completed                 -> completed (bool)
      event.status.type.state                     -> "pre" | "in" | "post"
      event.status.type.description               -> "Scheduled" | "In Progress" | "Final"
      event.competitions[0].competitors[order=0]  -> home team
      event.competitions[0].competitors[order=1]  -> away team
      competitor.team.displayName                 -> full team name
      competitor.score                            -> score string e.g. "108"

    Returns None if the event cannot be parsed.
    """
    try:
        competition = event.get("competitions", [{}])[0]
        competitors = competition.get("competitors", [])

        # ESPN order=0 is home, order=1 is away
        home = next((c for c in competitors if c.get("order") == 0), None)
        away = next((c for c in competitors if c.get("order") == 1), None)

        if not home or not away:
            logger.warning("Could not find home/away for event %s", event.get("id"))
            return None

        status = event.get("status", {})
        status_type = status.get("type", {})

        # Normalize scores: ESPN returns score as string "0" when not started
        home_score = home.get("score", "0")
        away_score = away.get("score", "0")

        # Build scores list in same shape as original Odds API format
        # so downstream consumers (Databricks notebooks) don't need changes
        scores = None
        state = status_type.get("state", "pre")
        if state in ("in", "post"):
            scores = [
                {"name": home.get("team", {}).get("displayName", ""), "score": home_score},
                {"name": away.get("team", {}).get("displayName", ""), "score": away_score},
            ]

        return {
            "sport": sport_key,
            "game_id": event.get("id"),
            "sport_key": sport_key,
            "sport_title": event.get("competitions", [{}])[0]
                           .get("type", {})
                           .get("abbreviation", sport_key),
            "commence_time": event.get("date"),         # ISO 8601 UTC
            "completed": status_type.get("completed", False),
            "status_state": state,                      # "pre" | "in" | "post"
            "status_description": status_type.get("description", ""),
            "home_team": home.get("team", {}).get("displayName", ""),
            "away_team": away.get("team", {}).get("displayName", ""),
            "home_score": home_score,
            "away_score": away_score,
            "scores": scores,                           # None if not started
            "last_update": status_type.get("shortDetail", ""),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "source": "espn",
        }
    except Exception as e:
        logger.error("Error parsing ESPN event %s: %s", event.get("id", "unknown"), e)
        return None


def main():
    logger.info("Starting Game Events producer (ESPN source)")
    producer = create_producer()

    try:
        while True:
            total_sent = 0

            for sport_key in ESPN_SPORTS:
                events = fetch_espn_scoreboard(sport_key)

                for event in events:
                    record = parse_espn_event(event, sport_key)
                    if record is None:
                        continue

                    key = f"{sport_key}:{record['game_id']}"
                    send_message(producer, TOPIC, record, key=key)
                    total_sent += 1

            producer.flush()
            logger.info(
                "Flushed %d game event records. Sleeping %ds...",
                total_sent,
                POLL_INTERVAL_SECONDS,
            )
            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Shutting down Game Events producer")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
