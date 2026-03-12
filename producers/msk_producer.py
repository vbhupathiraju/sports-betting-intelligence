"""
MSK Kafka producer with IAM authentication.
All sport-specific producers inherit from or use this module.
"""

import json
import logging
import socket
from datetime import datetime, timezone

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from kafka import KafkaProducer
from kafka.errors import KafkaError

from secrets_helper import get_bootstrap_brokers

logger = logging.getLogger(__name__)

AWS_REGION = "us-east-1"


class MSKTokenProvider:
    """
    Token provider object required by kafka-python's OAUTHBEARER mechanism.
    Must implement a token() method that returns a valid IAM auth token.
    """
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
        return token


def create_producer() -> KafkaProducer:
    brokers = get_bootstrap_brokers()
    logger.info("Connecting to MSK brokers: %s", brokers)

    producer = KafkaProducer(
        bootstrap_servers=brokers.split(","),
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=MSKTokenProvider(),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        retry_backoff_ms=500,
        linger_ms=100,
        batch_size=16384,
        compression_type="gzip",
        request_timeout_ms=30000,
        connections_max_idle_ms=540000,
        client_id=f"sports-betting-producer-{socket.gethostname()}",
	api_version=(3, 5, 1),
    )

    logger.info("KafkaProducer created successfully")
    return producer


def send_message(
    producer: KafkaProducer,
    topic: str,
    value: dict,
    key: str = None,
) -> None:
    value["ingested_at"] = datetime.now(timezone.utc).isoformat()

    future = producer.send(topic, value=value, key=key)

    try:
        record_metadata = future.get(timeout=10)
        logger.debug(
            "Sent to %s partition=%d offset=%d",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )
    except KafkaError as e:
        logger.error("Failed to send message to %s: %s", topic, e)
        raise
