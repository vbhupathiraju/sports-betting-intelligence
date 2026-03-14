-- Phase 6: Snowflake Storage Integration
-- Run as ACCOUNTADMIN in Snowsight
-- Creates the S3 integration and links to IAM role sports-betting-snowflake-role

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS SPORTS_BETTING;
USE DATABASE SPORTS_BETTING;
USE SCHEMA PUBLIC;

CREATE STORAGE INTEGRATION sports_betting_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::974482386805:role/sports-betting-snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://sports-betting-raw-data-974482386805/processed/');

-- After running, execute DESC INTEGRATION to get STORAGE_AWS_IAM_USER_ARN
-- and STORAGE_AWS_EXTERNAL_ID, then update the IAM trust policy
DESC INTEGRATION sports_betting_s3_integration;
