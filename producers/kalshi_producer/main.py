"""
Kalshi Producer
---------------
Polls the 'raw-game-events' Kafka topic every 30 seconds for today's games
(published by game_events_producer from ESPN), then fetches Kalshi winner
markets for each game and publishes to the 'raw-kalshi-markets' Kafka topic.

Pipeline:
  ESPN -> game_events_producer -> raw-game-events (Kafka)
       -> kalshi_producer (this file) -> Kalshi API -> raw-kalshi-markets (Kafka)

No Odds API calls are made here. Zero quota consumed.

Poll interval matches game_events_producer (30s) so both producers
stay in sync on the same data.

Ticker format: KXNBAGAME-{YY}{MON}{DD}{AWAY_ABBR}{HOME_ABBR}
  e.g. Philadelphia at Detroit on Mar 12 2026 -> KXNBAGAME-26MAR12PHIDET
       Brooklyn at Atlanta on Mar 12 2026    -> KXNBAGAME-26MAR12BKNATL
"""

import json
import logging
import sys
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaConsumer

sys.path.insert(0, "/app")
from msk_producer import create_producer, send_message
from secrets_helper import get_kalshi_credentials

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("kalshi_producer")

# --- Config ---
PRODUCE_TOPIC = "raw-kalshi-markets"
CONSUME_TOPIC = "raw-game-events"
POLL_INTERVAL_SECONDS = 30
KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Sports supported for Kalshi ticker construction
# Must match sport_key values published by game_events_producer
SUPPORTED_SPORTS = {"basketball_nba", "basketball_ncaab"}

# Kalshi event ticker prefixes per sport
KALSHI_SPORT_PREFIX = {
    "basketball_nba": "KXNBAGAME",
    "basketball_ncaab": "KXNCAAMBGAME",
}

# Full team name -> Kalshi 3-letter abbreviation
# Confirmed from live Kalshi tickers
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
    first_word = team_name.split()[0].upper()
    return first_word[:4]


def build_kalshi_event_ticker(
    sport: str, away_team: str, home_team: str, game_date: str
) -> str | None:
    """
    Construct a Kalshi event ticker from game metadata.

    Format: {PREFIX}-{YY}{MON}{DD}{AWAY}{HOME}
    Example: KXNBAGAME-26MAR12PHIDET

    game_date: ISO date string e.g. '2026-03-12'
    Returns None if the ticker cannot be built.
    """
    prefix = KALSHI_SPORT_PREFIX.get(sport)
    if not prefix:
        return None

    try:
        dt = datetime.strptime(game_date, "%Y-%m-%d")
    except ValueError:
        logger.warning("Invalid game_date format: %s", game_date)
        return None

    date_str = dt.strftime("%y%b%d").upper()  # e.g. 26MAR12

    if sport == "basketball_nba":
        away_abbr = NBA_TEAM_ABBR.get(away_team)
        home_abbr = NBA_TEAM_ABBR.get(home_team)
    else:
        away_abbr = build_ncaab_abbr(away_team)
        home_abbr = build_ncaab_abbr(home_team)

    if not away_abbr or not home_abbr:
        logger.warning(
            "Could not build abbr for %s @ %s (%s)", away_team, home_team, sport
        )
        return None

    return f"{prefix}-{date_str}{away_abbr}{home_abbr}"


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


def create_game_events_consumer() -> KafkaConsumer:
    """
    Create a Kafka consumer that reads from raw-game-events.

    Uses 'latest' auto_offset_reset so we only process messages that arrive
    after startup — we never replay stale game data from hours ago.

    Uses a unique group_id per startup so each run gets a fresh offset
    and doesn't block other consumers on the topic.
    """
    import socket
    from kafka.errors import NoBrokersAvailable
    from secrets_helper import get_bootstrap_brokers

    # Reuse the same MSK bootstrap brokers as the producer
    bootstrap_brokers = get_bootstrap_brokers().split(",")

    # Unique group ID per container instance so restarts don't cause rebalances
    group_id = f"kalshi-producer-{socket.gethostname()}-{int(time.time())}"

    logger.info("Creating Kafka consumer with group_id: %s", group_id)

    # Import the same IAM token provider used by msk_producer
    # (already installed in the container via requirements.txt)
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    class MSKTokenProvider:
        def token(self):
            token, _ = MSKAuthTokenProvider.generate_auth_token("us-east-1")
            return token

    max_retries = 5
    for attempt in range(1, max_retries + 1):
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
                consumer_timeout_ms=5000,  # Stop iteration after 5s of no messages
            )
            logger.info("Kafka consumer connected to %s", CONSUME_TOPIC)
            return consumer
        except NoBrokersAvailable as e:
            logger.warning(
                "Kafka consumer connection attempt %d/%d failed: %s",
                attempt,
                max_retries,
                e,
            )
            if attempt < max_retries:
                time.sleep(10)
            else:
                raise


def drain_game_events(consumer: KafkaConsumer) -> dict:
    """
    Drain all available messages from the raw-game-events topic.

    Returns a dict keyed by (sport, home_team, away_team, game_date) ->
    most recent game record. Deduplicates so we only process each game once
    per poll cycle even if multiple score updates arrived.

    consumer_timeout_ms=5000 on the consumer means this returns after
    5 seconds of silence — no busy loop.
    """
    games = {}

    try:
        for message in consumer:
            record = message.value
            sport = record.get("sport_key", "")

            if sport not in SUPPORTED_SPORTS:
                continue

            # Skip completed games — no point looking up Kalshi markets
            # for games that are already done
            if record.get("completed", False):
                continue

            game_date = (record.get("commence_time") or "")[:10]  # ISO date
            dedup_key = (
                sport,
                record.get("home_team", ""),
                record.get("away_team", ""),
                game_date,
            )

            # Keep the latest message for each game (last write wins)
            games[dedup_key] = record

    except Exception as e:
        # consumer_timeout_ms raises StopIteration internally — that's normal
        # Any other exception we log and continue
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
            # Step 1: drain all new game events from Kafka
            games = drain_game_events(consumer)
            logger.info(
                "Drained %d unique games from %s", len(games), CONSUME_TOPIC
            )

            if not games:
                logger.info(
                    "No new game events in this cycle. Sleeping %ds...",
                    POLL_INTERVAL_SECONDS,
                )
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            # Step 2: for each unique game, build Kalshi ticker and fetch markets
            total_sent = 0

            for (sport, home_team, away_team, game_date), game_record in games.items():
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
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                    }
                    key = f"{event_ticker}:{market.get('ticker', 'unknown')}"
                    send_message(producer, PRODUCE_TOPIC, record, key=key)
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
        consumer.close()


if __name__ == "__main__":
    main()
