"""
Kalshi Producer
---------------
Polls the 'raw-game-events' Kafka topic every cycle for today's games
(published by game_events_producer from ESPN), then fetches Kalshi winner
markets for each game and publishes to 'raw-kalshi-markets'.

Pipeline:
  ESPN -> game_events_producer -> raw-game-events (Kafka)
       -> kalshi_producer (this) -> Kalshi API -> raw-kalshi-markets (Kafka)

Zero Odds API calls. Kalshi prefix and team abbreviations are read
from sports_config.json — adding a new sport requires no code changes.

Config is hot-reloaded each poll cycle.
"""

import json
import logging
import sys
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaConsumer

sys.path.insert(0, "/app")
from config_loader import (
    get_active_sports,
    get_poll_interval,
    is_within_schedule,
    load_config,
    seconds_until_next_window,
)
from msk_producer import create_producer, send_message
from secrets_helper import get_bootstrap_brokers, get_kalshi_credentials

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("kalshi_producer")

PRODUCE_TOPIC = "raw-kalshi-markets"
CONSUME_TOPIC = "raw-game-events"
KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


def build_kalshi_event_ticker(sport_key, away_team, home_team, game_date, config):
    sport_cfg = config["sports"].get(sport_key)
    if not sport_cfg:
        return None
    prefix = sport_cfg.get("kalshi_prefix")
    if not prefix:
        return None
    team_abbr = sport_cfg.get("team_abbr", {})
    try:
        dt = datetime.strptime(game_date, "%Y-%m-%d")
    except ValueError:
        logger.warning("Invalid game_date format: %s", game_date)
        return None
    date_str = dt.strftime("%y%b%d").upper()
    away_abbr = team_abbr.get(away_team)
    home_abbr = team_abbr.get(home_team)
    if not away_abbr:
        logger.warning("No Kalshi abbr for away team '%s' (%s)", away_team, sport_key)
        return None
    if not home_abbr:
        logger.warning("No Kalshi abbr for home team '%s' (%s)", home_team, sport_key)
        return None
    return f"{prefix}-{date_str}{away_abbr}{home_abbr}"


def fetch_kalshi_markets(event_ticker, kalshi_api_key):
    url = f"{KALSHI_BASE_URL}/markets"
    headers = {"Authorization": f"Bearer {kalshi_api_key}"}
    params = {"limit": 10, "event_ticker": event_ticker}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        if resp.status_code == 404:
            logger.debug("No Kalshi market found for event: %s", event_ticker)
            return []
        resp.raise_for_status()
        return resp.json().get("markets", [])
    except Exception as e:
        logger.error("Error fetching Kalshi markets for %s: %s", event_ticker, e)
        return []


def create_game_events_consumer():
    import socket
    from kafka.errors import NoBrokersAvailable
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    bootstrap_brokers = get_bootstrap_brokers().split(",")
    group_id = f"kalshi-producer-{socket.gethostname()}-{int(time.time())}"
    logger.info("Creating Kafka consumer with group_id: %s", group_id)

    class MSKTokenProvider:
        def token(self):
            token, _ = MSKAuthTokenProvider.generate_auth_token("us-east-1")
            return token

    for attempt in range(1, 6):
        try:
            consumer = KafkaConsumer(
                CONSUME_TOPIC,
                bootstrap_servers=bootstrap_brokers,
                security_protocol="SASL_SSL",
                sasl_mechanism="OAUTHBEARER",
                sasl_oauth_token_provider=MSKTokenProvider(),
                api_version=(3, 5, 1),
                group_id=group_id,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=5000,
            )
            logger.info("Kafka consumer connected to %s", CONSUME_TOPIC)
            return consumer
        except NoBrokersAvailable as e:
            logger.warning("Consumer connection attempt %d/5 failed: %s", attempt, e)
            if attempt < 5:
                time.sleep(10)
            else:
                raise


def drain_game_events(consumer, active_sports):
    games = {}
    try:
        for message in consumer:
            record = message.value
            sport = record.get("sport_key", "")
            if sport not in active_sports:
                continue
            if record.get("completed", False):
                continue
            game_date = (record.get("commence_time") or "")[:10]
            dedup_key = (sport, record.get("home_team", ""), record.get("away_team", ""), game_date)
            games[dedup_key] = record
    except Exception as e:
        if "StopIteration" not in str(type(e).__name__):
            logger.error("Error draining game events: %s", e)
    return games


def main():
    logger.info("Starting Kalshi producer (Kafka source)")
    kalshi_creds = get_kalshi_credentials()
    kalshi_api_key = kalshi_creds["api_key"]
    producer = create_producer()
    consumer = create_game_events_consumer()

    try:
        while True:
            config = load_config()
            active_sports = get_active_sports(config)

            in_window = [s for s in active_sports if is_within_schedule(config["sports"][s])]
            out_of_window = [s for s in active_sports if not is_within_schedule(config["sports"][s])]

            if out_of_window:
                logger.info("Sports outside schedule window (skipping): %s", out_of_window)

            if not in_window:
                sleep_secs = min(
                    seconds_until_next_window(config["sports"][s]) for s in active_sports
                )
                sleep_secs = min(sleep_secs, 300)
                logger.info("No active sports in window. Sleeping %ds until next window...", sleep_secs)
                time.sleep(sleep_secs)
                continue

            games = drain_game_events(consumer, set(in_window))
            logger.info("Drained %d unique games from %s", len(games), CONSUME_TOPIC)

            if not games:
                sleep_secs = min(get_poll_interval(config["sports"][s]) for s in in_window)
                logger.info("No new game events. Sleeping %ds...", sleep_secs)
                time.sleep(sleep_secs)
                continue

            total_sent = 0
            for (sport, home_team, away_team, game_date), game_record in games.items():
                event_ticker = build_kalshi_event_ticker(sport, away_team, home_team, game_date, config)
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
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                    }
                    key = f"{event_ticker}:{market.get('ticker', 'unknown')}"
                    send_message(producer, PRODUCE_TOPIC, record, key=key)
                    total_sent += 1
                time.sleep(0.2)

            producer.flush()
            sleep_secs = min(get_poll_interval(config["sports"][s]) for s in in_window)
            logger.info("Flushed %d Kalshi market records. Sleeping %ds...", total_sent, sleep_secs)
            time.sleep(sleep_secs)

    except KeyboardInterrupt:
        logger.info("Shutting down Kalshi producer")
    finally:
        producer.close()
        consumer.close()


if __name__ == "__main__":
    main()
