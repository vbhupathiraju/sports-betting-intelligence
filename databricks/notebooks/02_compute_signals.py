# Databricks notebook source
# Cell 1 — Configuration
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

S3_BUCKET = "s3://sports-betting-raw-data-974482386805"
CHECKPOINT_BASE = f"{S3_BUCKET}/checkpoints/silver"

CATALOG = "sports_betting_intelligence"
BRONZE  = "bronze"
SILVER  = "silver"

# Divergence threshold — flag when Kalshi and sportsbook differ by more than this
DIVERGENCE_THRESHOLD = 0.05  # 5 percentage points

# Sharp money threshold — flag when line moves more than this in one poll cycle
SHARP_MONEY_THRESHOLD = 10   # 10 American odds points

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")

print("Config loaded")
print(f"Divergence threshold: {DIVERGENCE_THRESHOLD * 100}%")
print(f"Sharp money threshold: {SHARP_MONEY_THRESHOLD} odds points")

# COMMAND ----------

# Cell 2 — Helper: convert American odds to implied probability
# American odds: -150 means bet 150 to win 100 (implied prob = 150/250 = 60%)
# American odds: +130 means bet 100 to win 130 (implied prob = 100/230 = 43.5%)

def american_to_implied_prob(odds):
    """Convert American odds to implied probability (0.0 to 1.0)"""
    if odds is None:
        return None
    if odds < 0:
        return (-odds) / (-odds + 100)
    else:
        return 100 / (odds + 100)

american_to_implied_prob_udf = F.udf(american_to_implied_prob, DoubleType())

# Test it
test_cases = [(-150, 0.6), (+130, 0.435), (-110, 0.524)]
for odds, expected in test_cases:
    result = american_to_implied_prob(odds)
    print(f"odds={odds:+d} → implied prob={result:.3f} (expected ~{expected})")

# COMMAND ----------

# Cell 3 — Build odds implied probabilities from Bronze
# Explode bookmakers and markets to get h2h (moneyline) odds per team per game

odds_df = (spark.table(f"{CATALOG}.{BRONZE}.raw_odds")
    .select(
        "id",
        "sport_key",
        "sport_title",
        "home_team",
        "away_team",
        "commence_time",
        "ingested_at",
        F.explode("bookmakers").alias("bookmaker")
    )
    .select(
        "id",
        "sport_key",
        "sport_title",
        "home_team",
        "away_team",
        "commence_time",
        "ingested_at",
        F.col("bookmaker.key").alias("bookmaker_key"),
        F.col("bookmaker.title").alias("bookmaker_title"),
        F.explode("bookmaker.markets").alias("market")
    )
    .filter(F.col("market.key") == "h2h")   # moneyline only
    .select(
        "id",
        "sport_key",
        "sport_title",
        "home_team",
        "away_team",
        "commence_time",
        "ingested_at",
        "bookmaker_key",
        "bookmaker_title",
        F.explode("market.outcomes").alias("outcome")
    )
    .select(
        "id",
        "sport_key",
        "sport_title",
        "home_team",
        "away_team",
        "commence_time",
        "ingested_at",
        "bookmaker_key",
        "bookmaker_title",
        F.col("outcome.name").alias("team"),
        F.col("outcome.price").alias("american_odds"),
        american_to_implied_prob_udf(F.col("outcome.price")).alias("implied_prob")
    )
)

# Average implied probability across all bookmakers per game per team
avg_odds_df = (odds_df
    .groupBy("id", "sport_key", "sport_title", "home_team", "away_team", 
             "commence_time", "ingested_at", "team")
    .agg(
        F.avg("implied_prob").alias("avg_implied_prob"),
        F.count("bookmaker_key").alias("num_bookmakers")
    )
)

print(f"Total odds rows after explode: {odds_df.count()}")
print(f"Averaged odds rows: {avg_odds_df.count()}")
avg_odds_df.show(5, truncate=False)

# COMMAND ----------

# Cell 4 — Build Kalshi implied probabilities from Bronze (fixed: status = active)
kalshi_df = (spark.table(f"{CATALOG}.{BRONZE}.raw_kalshi")
    .filter(F.col("status") == "active")
    .select(
        "event_ticker",
        "market_ticker",
        "sport",
        "home_team",
        "away_team",
        "game_date",
        "ingested_at",
        "title",
        F.col("yes_ask_dollars").cast(DoubleType()).alias("yes_ask"),
        F.col("yes_bid_dollars").cast(DoubleType()).alias("yes_bid"),
    )
    .withColumn(
        "kalshi_implied_prob",
        (F.col("yes_ask") + F.col("yes_bid")) / 2
    )
    .filter(F.col("kalshi_implied_prob").isNotNull())
    .filter(F.col("kalshi_implied_prob") > 0)
)

print(f"Kalshi rows: {kalshi_df.count()}")
kalshi_df.show(5, truncate=False)

# COMMAND ----------

# Cell 5 — Compute divergence signals
# Join Kalshi and sportsbook odds on home_team + away_team
# Compare Kalshi implied prob vs average sportsbook implied prob

# Get home team implied prob from avg_odds_df
home_odds = (avg_odds_df
    .filter(F.col("team") == F.col("home_team"))
    .select(
        "id",
        "sport_key",
        "home_team",
        "away_team",
        "commence_time",
        "ingested_at",
        F.col("avg_implied_prob").alias("sportsbook_home_prob"),
        "num_bookmakers"
    )
)

# Join Kalshi home team markets with sportsbook home team odds
divergence_df = (kalshi_df
    .join(
        home_odds,
        on=["home_team", "away_team"],
        how="inner"
    )
    .withColumn(
        "divergence",
        F.abs(F.col("kalshi_implied_prob") - F.col("sportsbook_home_prob"))
    )
    .withColumn(
        "signal_direction",
        F.when(
            F.col("kalshi_implied_prob") > F.col("sportsbook_home_prob"),
            "KALSHI_HIGHER"   # Kalshi thinks home team more likely than sportsbooks
        ).otherwise("SPORTSBOOK_HIGHER")
    )
    .withColumn(
        "is_divergence_signal",
        F.col("divergence") >= DIVERGENCE_THRESHOLD
    )
    .withColumn("signal_type", F.lit("divergence"))
    .withColumn("computed_at", F.current_timestamp())
    .select(
        "signal_type",
        "computed_at",
        "sport_key",
        "home_team",
        "away_team",
        "commence_time",
        "event_ticker",
        "market_ticker",
        "kalshi_implied_prob",
        "sportsbook_home_prob",
        "divergence",
        "signal_direction",
        "is_divergence_signal",
        "num_bookmakers",
    )
)

total = divergence_df.count()
signals = divergence_df.filter(F.col("is_divergence_signal")).count()
print(f"Total joined rows: {total}")
print(f"Divergence signals (>={DIVERGENCE_THRESHOLD*100}%): {signals}")
divergence_df.filter(F.col("is_divergence_signal")).show(5, truncate=False)

# COMMAND ----------

# Cell 6 — Compute sharp money signals
# Detect when the same game's odds move significantly between poll cycles
# Uses window functions to compare current odds vs previous poll

from pyspark.sql.window import Window

# Get moneyline odds per game per bookmaker over time
odds_time_series = (odds_df
    .filter(F.col("bookmaker_key").isin(
        "fanduel", "draftkings", "betmgm", "betrivers"  # major books only
    ))
    .withColumn(
        "ingested_at_ts",
        F.to_timestamp("ingested_at")
    )
)

# Window: for each game + bookmaker + team, order by time
window = Window.partitionBy(
    "id", "bookmaker_key", "team"
).orderBy("ingested_at_ts")

# Compute line movement between consecutive polls
sharp_money_df = (odds_time_series
    .withColumn("prev_odds", F.lag("american_odds", 1).over(window))
    .withColumn("prev_prob", F.lag("implied_prob", 1).over(window))
    .filter(F.col("prev_odds").isNotNull())
    .withColumn(
        "odds_movement",
        F.abs(F.col("american_odds") - F.col("prev_odds"))
    )
    .withColumn(
        "prob_movement",
        F.abs(F.col("implied_prob") - F.col("prev_prob"))
    )
    .withColumn(
        "is_sharp_signal",
        F.col("odds_movement") >= SHARP_MONEY_THRESHOLD
    )
    .withColumn(
        "movement_direction",
        F.when(F.col("american_odds") > F.col("prev_odds"), "ODDS_LENGTHENING")
        .otherwise("ODDS_SHORTENING")  # shortening = team becoming more favored
    )
    .withColumn("signal_type", F.lit("sharp_money"))
    .withColumn("computed_at", F.current_timestamp())
    .select(
        "signal_type",
        "computed_at",
        "sport_key",
        "home_team",
        "away_team",
        "commence_time",
        "bookmaker_key",
        "team",
        "prev_odds",
        "american_odds",
        "odds_movement",
        F.round("prev_prob", 4).alias("prev_implied_prob"),
        F.round("implied_prob", 4).alias("current_implied_prob"),
        F.round("prob_movement", 4).alias("prob_movement"),
        "movement_direction",
        "is_sharp_signal",
        "ingested_at_ts"
    )
)

total = sharp_money_df.count()
signals = sharp_money_df.filter(F.col("is_sharp_signal")).count()
print(f"Total line movement rows: {total}")
print(f"Sharp money signals (>={SHARP_MONEY_THRESHOLD} pts): {signals}")
sharp_money_df.filter(F.col("is_sharp_signal")).show(5, truncate=False)

# COMMAND ----------

# Cell 7 — Write divergence signals to Silver Delta table
(divergence_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER}.divergence_signals")
)

print(f"Divergence signals written to {CATALOG}.{SILVER}.divergence_signals")
print(f"Row count: {spark.table(f'{CATALOG}.{SILVER}.divergence_signals').count()}")

# COMMAND ----------

# Cell 8 — Write sharp money signals to Silver Delta table
(sharp_money_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER}.sharp_money_signals")
)

print(f"Sharp money signals written to {CATALOG}.{SILVER}.sharp_money_signals")
print(f"Row count: {spark.table(f'{CATALOG}.{SILVER}.sharp_money_signals').count()}")

# COMMAND ----------

# Cell 9 — Write signals to S3 as JSON (for downstream consumption)

PROCESSED_BASE = f"{S3_BUCKET}/processed"

# Write divergence signals to S3
(spark.table(f"{CATALOG}.{SILVER}.divergence_signals")
    .filter(F.col("is_divergence_signal"))  # only actual signals, not all rows
    .write
    .format("json")
    .mode("overwrite")
    .save(f"{PROCESSED_BASE}/divergence_signals/")
)

print(f"Divergence signals written to {PROCESSED_BASE}/divergence_signals/")

# Write sharp money signals to S3
(spark.table(f"{CATALOG}.{SILVER}.sharp_money_signals")
    .filter(F.col("is_sharp_signal"))  # only actual signals
    .write
    .format("json")
    .mode("overwrite")
    .save(f"{PROCESSED_BASE}/sharp_money_signals/")
)

print(f"Sharp money signals written to {PROCESSED_BASE}/sharp_money_signals/")

# COMMAND ----------

