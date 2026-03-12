import boto3
import json

def get_secret(secret_name: str, region: str = "us-east-1") -> dict:
    """
    Fetch a secret from AWS Secrets Manager.
    
    Usage:
        from secrets_helper import get_secret
        creds = get_secret("sports-betting/kalshi-credentials")
        api_key = creds["api_key"]
    """
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


if __name__ == "__main__":
    # Quick test — run with: python secrets_helper.py
    secret = get_secret("sports-betting/odds-api-key")
    print(f"✅ Secrets Manager connection working. Key starts with: {secret['api_key'][:6]}...")
