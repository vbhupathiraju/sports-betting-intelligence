-- Phase 6: Tables
-- Run as ACCOUNTADMIN in Snowsight after 02_stages.sql

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE SPORTS_BETTING;
USE SCHEMA PUBLIC;

CREATE OR REPLACE TABLE divergence_signals (
  signal_type            VARCHAR,
  computed_at            TIMESTAMP_TZ,
  sport_key              VARCHAR,
  home_team              VARCHAR,
  away_team              VARCHAR,
  commence_time          TIMESTAMP_TZ,
  event_ticker           VARCHAR,
  market_ticker          VARCHAR,
  kalshi_implied_prob    FLOAT,
  sportsbook_home_prob   FLOAT,
  divergence             FLOAT,
  signal_direction       VARCHAR,
  is_divergence_signal   BOOLEAN,
  num_bookmakers         INTEGER
);

CREATE OR REPLACE TABLE sharp_money_signals (
  signal_type           VARCHAR,
  computed_at           TIMESTAMP_TZ,
  sport_key             VARCHAR,
  home_team             VARCHAR,
  away_team             VARCHAR,
  commence_time         TIMESTAMP_TZ,
  bookmaker_key         VARCHAR,
  team                  VARCHAR,
  prev_odds             INTEGER,
  american_odds         INTEGER,
  odds_movement         INTEGER,
  prev_implied_prob     FLOAT,
  current_implied_prob  FLOAT,
  prob_movement         FLOAT,
  movement_direction    VARCHAR,
  is_sharp_signal       BOOLEAN,
  ingested_at_ts        TIMESTAMP_TZ
);
