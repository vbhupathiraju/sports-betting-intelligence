# Databricks notebook source
# Cell 1 — Configuration
S3_BUCKET = "s3://sports-betting-raw-data-974482386805"

RAW_PATHS = {
    "odds":        f"{S3_BUCKET}/odds/",
    "kalshi":      f"{S3_BUCKET}/kalshi/",
    "game_events": f"{S3_BUCKET}/game-events/",
}

CHECKPOINT_BASE = f"{S3_BUCKET}/checkpoints/bronze"

CATALOG = "sports_betting_intelligence"
SCHEMA  = "bronze"

print("Config loaded")
print(f"S3 bucket: {S3_BUCKET}")
print(f"Catalog:   {CATALOG}")
print(f"Schema:    {SCHEMA}")

# COMMAND ----------

# Cell 2 — Create Bronze schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} MANAGED LOCATION '{S3_BUCKET}/unity-catalog/'")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Catalog and schema ready: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# Cell 3 — Auto Loader: raw odds → Bronze Delta table
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/odds_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(RAW_PATHS["odds"])
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/odds")
    .option("mergeSchema", "true")
    .toTable(f"{CATALOG}.{SCHEMA}.raw_odds")
)

print("raw_odds stream started")

# COMMAND ----------

# Cell 4 — Auto Loader: raw kalshi → Bronze Delta table
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/kalshi_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(RAW_PATHS["kalshi"])
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/kalshi")
    .option("mergeSchema", "true")
    .toTable(f"{CATALOG}.{SCHEMA}.raw_kalshi")
)

print("raw_kalshi stream started")

# COMMAND ----------

# Cell 5 — Auto Loader: raw game events → Bronze Delta table
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/game_events_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(RAW_PATHS["game_events"])
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/game_events")
    .option("mergeSchema", "true")
    .toTable(f"{CATALOG}.{SCHEMA}.raw_game_events")
)

print("raw_game_events stream started")

# COMMAND ----------

# Cell 6 — Verify Bronze tables have data
for table in ["raw_odds", "raw_kalshi", "raw_game_events"]:
    count = spark.table(f"{CATALOG}.{SCHEMA}.{table}").count()
    print(f"{CATALOG}.{SCHEMA}.{table}: {count} rows")

# COMMAND ----------

# Cell 7 — Inspect schemas
print("=== raw_odds schema ===")
spark.table(f"{CATALOG}.{SCHEMA}.raw_odds").printSchema()

print("\n=== raw_kalshi schema ===")
spark.table(f"{CATALOG}.{SCHEMA}.raw_kalshi").printSchema()

# COMMAND ----------

