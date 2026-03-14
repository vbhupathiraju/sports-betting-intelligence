-- Phase 6: Dashboard-ready views
-- Run as ACCOUNTADMIN in Snowsight after 04_copy_into.sql

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE SPORTS_BETTING;
USE SCHEMA PUBLIC;

CREATE OR REPLACE VIEW v_sharp_money_latest AS
SELECT
  computed_at,
  sport_key,
  home_team,
  away_team,
  commence_time,
  bookmaker_key,
  team,
  american_odds,
  prev_odds,
  odds_movement,
  current_implied_prob,
  prev_implied_prob,
  prob_movement,
  movement_direction,
  ingested_at_ts
FROM sharp_money_signals
WHERE computed_at = (SELECT MAX(computed_at) FROM sharp_money_signals)
ORDER BY prob_movement DESC;

CREATE OR REPLACE VIEW v_divergence_latest AS
SELECT
  computed_at,
  sport_key,
  home_team,
  away_team,
  commence_time,
  event_ticker,
  market_ticker,
  kalshi_implied_prob,
  sportsbook_home_prob,
  divergence,
  signal_direction,
  num_bookmakers
FROM divergence_signals
WHERE computed_at = (SELECT MAX(computed_at) FROM divergence_signals)
ORDER BY divergence DESC;

CREATE OR REPLACE VIEW v_signals_summary AS
SELECT
  sport_key,
  COUNT(*) AS total_sharp_signals,
  AVG(prob_movement) AS avg_prob_movement,
  AVG(ABS(odds_movement)) AS avg_odds_movement,
  COUNT(DISTINCT bookmaker_key) AS num_bookmakers,
  MIN(computed_at) AS first_signal_at,
  MAX(computed_at) AS last_signal_at
FROM sharp_money_signals
GROUP BY sport_key
ORDER BY total_sharp_signals DESC;
