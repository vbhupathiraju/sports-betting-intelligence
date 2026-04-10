# SharpEdge — Real-Time Sports Betting Intelligence Platform

A production-grade data engineering platform that streams live sports odds and prediction market data to detect divergence between Kalshi prediction markets and traditional sportsbook lines, surfacing sharp money signals in real time.

**Live Dashboard → [sharpedge-betting.vercel.app](https://sharpedge-betting.vercel.app)**
&nbsp;·&nbsp;
**Repo → [github.com/vbhupathiraju/sports-betting-intelligence](https://github.com/vbhupathiraju/sports-betting-intelligence)**

---

## What It Does

Traditional sportsbooks and prediction markets like Kalshi price the same game outcomes independently. When informed ("sharp") money moves one market before the other catches up, a measurable divergence appears in the implied probabilities. SharpEdge ingests odds data from both sources every 15–120 seconds, computes divergence signals across all active games, and surfaces them on a live dashboard updated every 60 seconds.

---

## Architecture

```
The Odds API ──┐
Kalshi API    ─┼──▶  Apache Kafka (AWS MSK)  ──▶  Amazon Kinesis Firehose  ──▶  S3
ESPN API      ─┘                                                                  │
                                                                                  ▼
                                                                  Databricks (Auto Loader + Spark)
                                                                                  │
                                                         ┌────────────────────────┘
                                                         ▼
                                                      Snowflake
                                                         │
                                                         ▼
                                              Next.js Dashboard (Vercel)
```

**Orchestration:** Apache Airflow (self-hosted on EC2) triggers Databricks every 2 minutes during active game windows, then fires Snowflake `COPY INTO` automatically after each run.

**End-to-end latency:** ~3–5 minutes from a real-world odds movement to dashboard update (dominated by Firehose 60s buffer + ~2 min Databricks runtime).

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Sources | The Odds API, Kalshi REST API, ESPN API |
| Message Bus | Apache Kafka on AWS MSK (IAM auth, port 9098) |
| Stream Transport | Amazon Kinesis Data Firehose (60s / 1MB buffer) |
| Storage | Amazon S3 (partitioned by sport + date) |
| Stream Processing | Databricks (Auto Loader → Bronze Delta, Spark → Silver signals) |
| Orchestration | Apache Airflow 2.8.1 (self-hosted on EC2, SequentialExecutor) |
| Data Warehouse | Snowflake (REST API, custom client to avoid OOM on Vercel) |
| Compute | AWS EC2 `t4g.small` (ARM, private subnet, SSM-only access) |
| Secrets | AWS Secrets Manager |
| Security & Audit | AWS IAM, CloudTrail, KMS, VPC with private subnets + NAT Gateway |
| Alerting | CloudWatch + SNS + Gmail SMTP |
| Infrastructure | Terraform |
| Dashboard | Next.js 16 (App Router) + React 19 + Tailwind CSS v4, deployed on Vercel |
| Charts | Recharts |

---

## Signal Types

### Divergence Signals
For each active game, the platform joins the latest Kalshi implied probability with the sportsbook consensus implied probability (derived from American moneyline odds across DraftKings, FanDuel, BetMGM, BetRivers, Caesars, and others). The difference is the **divergence score**, a proxy for informed money in one market not yet reflected in the other.

### Sharp Money Signals
For each game and sportsbook, the platform tracks line movement over time. A significant shift in implied probability, especially when correlated across multiple books simultaneously, is a **sharp money signal** indicating large professional wagers moved the line.

---

## Pipeline Details

### Ingestion (EC2 Docker containers)
Three producer containers run continuously:
- **`odds-api-producer`**: polls The Odds API every 120s during game windows, publishes moneyline odds for all active NBA and NCAAB games to `raw-odds` Kafka topic
- **`game-events-producer`**: polls ESPN every 15s, publishes live game state (score, clock, status) to `raw-game-events` Kafka topic
- **`kalshi-producer`**: consumes from `raw-game-events`, constructs Kalshi event tickers using team abbreviation mappings, fetches market prices, publishes to `raw-kalshi-markets` Kafka topic. Kalshi data is always synchronized with active ESPN games.
- **`firehose-consumer`**: reads all three Kafka topics and forwards to Kinesis Firehose for S3 delivery

### Processing (Databricks)
Two notebooks run in sequence on every Airflow trigger:
1. **`01_ingest_raw.py`**: Databricks Auto Loader reads new S3 files into Bronze Delta tables (`raw_odds`, `raw_kalshi`, `raw_game_events`), tracking processed files via S3 checkpoints
2. **`02_compute_signals.py`**: Joins Bronze tables, converts odds to implied probabilities, computes divergence and sharp money signals, writes results to `s3://processed/divergence_signals/` and `s3://processed/sharp_money_signals/`

### Warehousing (Snowflake)
Airflow triggers `COPY INTO` on both signal tables after each Databricks run. Four views handle deduplication:
- `v_divergence_latest` / `v_sharp_money_latest`: latest signal per game using 10-minute window + `ROW_NUMBER()` dedup
- `v_divergence_signals_clean` / `v_sharp_money_signals_clean`: full history deduped by minute-level bucketing for chart queries

### Dashboard (Next.js + Vercel)
- On page load, fires `/api/warmup` to pre-warm the Snowflake warehouse, then fetches all signal data + ESPN scores in parallel
- Sport/tab/date filtering is client-side with no additional Snowflake queries on filter changes
- Chart data is lazy-loaded per game on first card expand, with auto-retry on Snowflake cold starts
- Auto-refreshes every 60 seconds in the background without resetting UI state
- Sportsbook lines displayed with consistent color encoding (DraftKings teal, FanDuel blue, BetMGM orange, etc.), solid lines for home team, dashed for away

---

## Repository Structure

```
sports-betting-intelligence/
├── producers/
│   ├── odds_api_producer/main.py
│   ├── kalshi_producer/main.py
│   ├── game_events_producer/main.py
│   ├── firehose_consumer/main.py
│   ├── sports_config.json          # Team abbreviation mappings (ESPN → Kalshi)
│   ├── msk_producer.py             # Shared Kafka producer helper
│   └── secrets_helper.py           # Secrets Manager helper
├── databricks/notebooks/
│   ├── 01_ingest_raw.py            # Auto Loader → Bronze Delta
│   └── 02_compute_signals.py       # Signal computation → Silver + S3
├── snowflake/
│   ├── 01_setup.sql
│   ├── 05_views.sql                # All 4 dedup views
│   └── snowflake_views_backup.sql  # Exported DDLs
├── nextjs-dashboard/
│   ├── app/api/                    # divergence, sharp, odds/history, scores, warmup
│   ├── components/                 # Dashboard, GameCard, OddsChart, DivergenceChart, etc.
│   └── lib/
│       ├── snowflake.ts            # Custom Snowflake REST client (replaces snowflake-sdk)
│       └── marchMadness2026.ts     # Tournament team filter set
├── infrastructure/                 # Terraform — VPC, MSK, EC2, Firehose, S3, IAM
├── docker-compose.yml
└── README.md
```

---

## Infrastructure (Terraform)

All AWS infrastructure is defined in `infrastructure/` and provisioned via Terraform:
- VPC with private subnets (EC2 + MSK brokers), public subnets, NAT Gateway, Internet Gateway
- MSK cluster (`kafka.t3.small`, 2 brokers, IAM auth)
- EC2 `t4g.small` in private subnet (SSM-only access, no public IP)
- Three Kinesis Firehose delivery streams → S3
- S3 buckets with SSE-S3 encryption and SSL-only bucket policies
- IAM roles and policies for EC2 instance, MSK access, Firehose delivery
- Secrets Manager secrets for API keys and credentials
- CloudWatch alarms + SNS topic for EC2 health alerting
- CloudTrail for full API audit logging

---

## Key Engineering Decisions

**Custom Snowflake REST client:** `snowflake-sdk` uses native C++ binaries that exceed Vercel Hobby's 2048MB serverless function memory limit. Built a custom HTTPS client against Snowflake's REST API that handles chunked result sets (gzip-compressed S3 presigned URLs) correctly.

**Kalshi producer as a Kafka consumer:** Rather than polling Kalshi on a fixed schedule, the Kalshi producer consumes from the `raw-game-events` topic. This ensures Kalshi data is always synchronized with games ESPN is actively tracking, without needing a separate game discovery mechanism.

**Minute-level dedup views:** Databricks occasionally rewrites S3 signal files, causing Snowflake `COPY INTO` to ingest duplicate rows. Rather than fighting the write pattern, clean views truncate `computed_at` to the minute and use `ROW_NUMBER()` to keep only the latest row per partition, collapsing all duplicates within each 2-minute poll cycle.

**UTC→ET timezone conversion for Kalshi tickers:** Kalshi event tickers encode the game date in ET (e.g. `KXNBAGAME-26MAR24DENPHX`). NBA games tip off at ~7pm ET = midnight UTC, so extracting the date from the UTC `commence_time` produces the wrong date. The Kalshi producer converts to ET before building the ticker.

---

## Running Locally

```bash
# Clone and set up Python venv
git clone https://github.com/vbhupathiraju/sports-betting-intelligence.git
cd sports-betting-intelligence
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Run the Next.js dashboard
cd nextjs-dashboard
npm install
npm run dev
# Visit http://localhost:3000
```

The dashboard requires Snowflake credentials in `nextjs-dashboard/.env.local` (see Environment Variables section above).

---

## Cost (AWS, during active development)

| Service | Monthly Cost |
|---|---|
| MSK (2x `kafka.t3.small`) | ~$20–30 |
| EC2 (`t4g.small`) | ~$8–10 |
| Kinesis Firehose | ~$5–8 |
| S3 | ~$2–3 |
| Secrets Manager | ~$3 |
| CloudTrail | ~$5 |
| SNS + CloudWatch | ~$1 |
| Odds API | ~$30 |
| **Total** | **~$74–90/mo** |

Databricks and Snowflake ran on free trials during development.

---

## License

MIT
