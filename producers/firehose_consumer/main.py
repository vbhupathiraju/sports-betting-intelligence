"""
firehose_consumer/main.py

Consumes messages from the 3 raw Kafka topics and forwards each message
to the corresponding Kinesis Firehose delivery stream.

Topic → Firehose stream mapping:
  raw-odds            → sports-betting-odds-stream
  raw-kalshi-markets  → sports-betting-kalshi-stream
  raw-game-events     → sports-betting-game-events-stream
"""

import json
import logging
import os
import sys
import time

import boto3
from kafka import KafkaConsumer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────

AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
BOOTSTRAP_BROKERS = os.environ.get("BOOTSTRAP_BROKERS", "")

TOPIC_TO_STREAM = {
    "raw-odds":           "sports-betting-odds-stream",
    "raw-kalshi-markets": "sports-betting-kalshi-stream",
    "raw-game-events":    "sports-betting-game-events-stream",
}

CONSUMER_GROUP = "firehose-consumer-group"

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# MSK IAM token provider
# Must be a class with a token() method — NOT a plain callable.
# See Phase 3 Issue 8.
# ─────────────────────────────────────────────

class MSKTokenProvider:
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
        return token


# ─────────────────────────────────────────────
# Bootstrap brokers: env var or Secrets Manager
# ─────────────────────────────────────────────

def get_bootstrap_brokers() -> str:
    if BOOTSTRAP_BROKERS:
        logger.info("Using BOOTSTRAP_BROKERS from environment variable")
        return BOOTSTRAP_BROKERS

    logger.info("Fetching bootstrap brokers from Secrets Manager")
    client = boto3.client("secretsmanager", region_name=AWS_REGION)
    response = client.get_secret_value(SecretId="sports-betting/msk-bootstrap-brokers")
    secret = json.loads(response["SecretString"])
    return secret["bootstrap_brokers"]


# ─────────────────────────────────────────────
# Build Kafka consumer
# api_version=(3, 5, 1) is MANDATORY — skips broker version probe which
# times out against MSK IAM auth. See Phase 3 Issue 9.
# ─────────────────────────────────────────────

def build_consumer(brokers: str) -> KafkaConsumer:
    logger.info(f"Connecting to MSK brokers: {brokers}")
    return KafkaConsumer(
        *TOPIC_TO_STREAM.keys(),
        bootstrap_servers=brokers.split(","),
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=MSKTokenProvider(),
        api_version=(3, 5, 1),
        group_id=CONSUMER_GROUP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v,   # raw bytes — we re-encode for Firehose
        consumer_timeout_ms=1000,         # allows the poll loop to be interruptible
    )


# ─────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────

def main():
    logger.info("Starting Firehose consumer")

    brokers = get_bootstrap_brokers()
    firehose = boto3.client("firehose", region_name=AWS_REGION)

    # Retry consumer connection on startup — MSK may need a moment
    consumer = None
    for attempt in range(1, 6):
        try:
            consumer = build_consumer(brokers)
            logger.info("KafkaConsumer connected successfully")
            break
        except Exception as e:
            logger.warning(f"Consumer connect attempt {attempt}/5 failed: {e}")
            time.sleep(10)

    if consumer is None:
        logger.error("Failed to connect to MSK after 5 attempts — exiting")
        sys.exit(1)

    logger.info(f"Subscribed to topics: {list(TOPIC_TO_STREAM.keys())}")

    records_forwarded = 0

    while True:
        try:
            for message in consumer:
                topic = message.topic
                stream_name = TOPIC_TO_STREAM.get(topic)

                if stream_name is None:
                    logger.warning(f"Unknown topic: {topic} — skipping")
                    continue

                # Firehose records must end with a newline for S3 line-delimited JSON
                payload = message.value
                if not payload.endswith(b"\n"):
                    payload = payload + b"\n"

                try:
                    firehose.put_record(
                        DeliveryStreamName=stream_name,
                        Record={"Data": payload},
                    )
                    records_forwarded += 1

                    if records_forwarded % 10 == 0:
                        logger.info(
                            f"Forwarded {records_forwarded} records total | "
                            f"latest: topic={topic} stream={stream_name}"
                        )

                except Exception as e:
                    logger.error(f"Firehose put_record failed for stream {stream_name}: {e}")

        except Exception as e:
            logger.error(f"Consumer poll error: {e} — continuing")
            time.sleep(5)


if __name__ == "__main__":
    main()
