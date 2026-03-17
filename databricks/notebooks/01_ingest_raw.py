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
# trigger(availableNow=True): process all pending files then stop (no infinite loop)
# schemaEvolutionMode addNewColumns: accept new fields without crashing
# awaitTermination: block until this stream finishes before moving to next cell
print("Starting odds ingest...")
odds_query = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/odds_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(RAW_PATHS["odds"])
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/odds")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.raw_odds")
)
odds_query.awaitTermination()
print("raw_odds ingest complete")

# COMMAND ----------

# Cell 4 — Auto Loader: raw kalshi → Bronze Delta table
print("Starting kalshi ingest...")
kalshi_query = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/kalshi_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(RAW_PATHS["kalshi"])
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/kalshi")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.raw_kalshi")
)
kalshi_query.awaitTermination()
print("raw_kalshi ingest complete")

# COMMAND ----------

# Cell 5 — Auto Loader: raw game events → Bronze Delta table
print("Starting game_events ingest...")
game_events_query = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/game_events_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(RAW_PATHS["game_events"])
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/game_events")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.raw_game_events")
)
game_events_query.awaitTermination()
print("raw_game_events ingest complete")

# COMMAND ----------

# Cell 6 — Verify Bronze tables have data
# This now runs AFTER all three streams have fully completed
for table in ["raw_odds", "raw_kalshi", "raw_game_events"]:
    count = spark.table(f"{CATALOG}.{SCHEMA}.{table}").count()
    print(f"{CATALOG}.{SCHEMA}.{table}: {count} rows")

# COMMAND ----------

# Cell 7 — Inspect schemas
print("=== raw_odds schema ===")
spark.table(f"{CATALOG}.{SCHEMA}.raw_odds").printSchema()

print("\n=== raw_kalshi schema ===")
spark.table(f"{CATALOG}.{SCHEMA}.raw_kalshi").printSchema()

print("\n=== raw_game_events schema ===")
spark.table(f"{CATALOG}.{SCHEMA}.raw_game_events").printSchema()

# COMMAND ----------
