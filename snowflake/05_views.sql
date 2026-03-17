-- v_divergence_latest: latest divergence signals, NBA and NCAAB only, ordered by divergence DESC
CREATE OR REPLACE VIEW SPORTS_BETTING.PUBLIC.v_divergence_latest AS
SELECT * FROM SPORTS_BETTING.PUBLIC.divergence_signals
WHERE sport_key NOT IN ('basketball_wncaab', 'basketball_ncaaw')
  AND computed_at = (SELECT MAX(computed_at) FROM SPORTS_BETTING.PUBLIC.divergence_signals)
ORDER BY divergence DESC;

-- v_sharp_money_latest: latest sharp money signals, NBA and NCAAB only, ordered by prob_movement DESC
CREATE OR REPLACE VIEW SPORTS_BETTING.PUBLIC.v_sharp_money_latest AS
SELECT * FROM SPORTS_BETTING.PUBLIC.sharp_money_signals
WHERE sport_key NOT IN ('basketball_wncaab', 'basketball_ncaaw')
  AND computed_at = (SELECT MAX(computed_at) FROM SPORTS_BETTING.PUBLIC.sharp_money_signals)
ORDER BY prob_movement DESC;

-- v_signals_summary: aggregated signal counts by sport_key, NBA and NCAAB only
CREATE OR REPLACE VIEW SPORTS_BETTING.PUBLIC.v_signals_summary AS
SELECT 
    sport_key,
    COUNT(*) AS signal_count,
    MAX(computed_at) AS latest_computed_at
FROM (
    SELECT sport_key, computed_at FROM SPORTS_BETTING.PUBLIC.divergence_signals
    WHERE sport_key NOT IN ('basketball_wncaab', 'basketball_ncaaw')
    UNION ALL
    SELECT sport_key, computed_at FROM SPORTS_BETTING.PUBLIC.sharp_money_signals
    WHERE sport_key NOT IN ('basketball_wncaab', 'basketball_ncaaw')
)
GROUP BY sport_key;
