"""
Secrets Manager helper for sports-betting-intelligence project.
Centralizes all secret retrieval with caching to minimize API calls.
"""

import boto3
import json
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

# Region where secrets are stored
AWS_REGION = "us-east-1"


def _get_client():
    return boto3.client("secretsmanager", region_name=AWS_REGION)


@lru_cache(maxsize=None)
def _get_secret_raw(secret_id: str) -> dict:
    """Fetch and parse a secret from Secrets Manager (cached)."""
    client = _get_client()
    response = client.get_secret_value(SecretId=secret_id)
    return json.loads(response["SecretString"])


def get_odds_api_key() -> str:
    secret = _get_secret_raw("sports-betting/odds-api-key")
    return secret["api_key"]


def get_kalshi_credentials() -> dict:
    """Returns dict with 'email' and 'api_key' keys."""
    return _get_secret_raw("sports-betting/kalshi-credentials")


def get_snowflake_credentials() -> dict:
    """Returns dict with account, username, password, database, warehouse."""
    return _get_secret_raw("sports-betting/snowflake-credentials")


def get_bootstrap_brokers() -> str:
    """Returns the MSK bootstrap broker string for IAM auth (TLS endpoints)."""
    secret = _get_secret_raw("sports-betting/msk-bootstrap-brokers")
    return secret["bootstrap_brokers"]
