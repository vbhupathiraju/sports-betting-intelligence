-- Phase 6: Load data from S3 into Snowflake tables
-- Run as ACCOUNTADMIN in Snowsight after 03_tables.sql
-- Re-run whenever new data lands in S3 (Phase 7 Airflow will automate this)

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE SPORTS_BETTING;
USE SCHEMA PUBLIC;

COPY INTO sharp_money_signals
FROM @sports_betting_sharp_money_stage
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PURGE = FALSE;

COPY INTO divergence_signals
FROM @sports_betting_divergence_stage
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PURGE = FALSE;
