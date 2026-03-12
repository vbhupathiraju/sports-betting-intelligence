# ARM-compatible base image (EC2 is t4g.small / aarch64 — see Phase 3 Issue 15)
FROM python:3.11-slim

WORKDIR /app

# Copy shared modules first
COPY requirements.txt .
COPY secrets_helper.py .

# Copy this consumer's code
COPY firehose_consumer/main.py .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-u", "main.py"]
