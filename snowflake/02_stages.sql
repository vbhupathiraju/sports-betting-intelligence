-- Phase 6: External Stages
-- Run as ACCOUNTADMIN in Snowsight after 01_storage_integration.sql

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE SPORTS_BETTING;
USE SCHEMA PUBLIC;

CREATE STAGE sports_betting_divergence_stage
  STORAGE_INTEGRATION = sports_betting_s3_integration
  URL = 's3://sports-betting-raw-data-974482386805/processed/divergence_signals/'
  FILE_FORMAT = (
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
  );

CREATE STAGE sports_betting_sharp_money_stage
  STORAGE_INTEGRATION = sports_betting_s3_integration
  URL = 's3://sports-betting-raw-data-974482386805/processed/sharp_money_signals/'
  FILE_FORMAT = (
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
  );

-- Verify stages can see S3 files
LIST @sports_betting_divergence_stage;
LIST @sports_betting_sharp_money_stage;
