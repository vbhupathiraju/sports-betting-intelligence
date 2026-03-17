"""
Sports Betting Pipeline DAG
----------------------------
Runs on a schedule aligned to game windows:
  - Every 15 minutes during active game hours (PST)
  - Every 2 hours outside game hours

Each run:
  Task 1: Trigger Databricks job (ingest_raw -> compute_signals) and wait for completion
  Task 2a: COPY INTO Snowflake divergence_signals table
  Task 2b: COPY INTO Snowflake sharp_money_signals table (runs in parallel with 2a)

Task 2a and 2b only run if Task 1 succeeds.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.dates import days_ago
from zoneinfo import ZoneInfo

# ── Constants ────────────────────────────────────────────────────────────────
DATABRICKS_JOB_ID = 837198367689892
SNOWFLAKE_CONN_ID = "snowflake_default"
DATABRICKS_CONN_ID = "databricks_default"
SNOWFLAKE_DATABASE = "SPORTS_BETTING"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"

# ── Schedule check ───────────────────────────────────────────────────────────
def is_within_game_window() -> bool:
    """
    Returns True if current PST time is within any active game window.
    NBA:   3pm-11pm PST every day
    NCAAB: Tue/Wed 3pm-9pm, Thu/Fri 8am-9pm, Sat/Sun/Mon 3pm-9pm PST
    If within window, DAG proceeds. If not, DAG is short-circuited (skipped).
    """
    tz = ZoneInfo("America/Los_Angeles")
    now = datetime.now(tz)
    current_time = now.strftime("%H:%M")
    current_day = now.strftime("%A").lower()

    # NBA window: 3pm-11pm every day
    if "15:00" <= current_time <= "23:00":
        return True

    # NCAAB Thursday/Friday early window: 8am-3pm
    if current_day in ("thursday", "friday") and "08:00" <= current_time < "15:00":
        return True

    return False


# ── SQL ───────────────────────────────────────────────────────────────────────
# STRIP_OUTER_ARRAY = FALSE because Databricks writes newline-delimited JSON
# (one object per line), not a JSON array
COPY_DIVERGENCE_SQL = """
COPY INTO SPORTS_BETTING.PUBLIC.divergence_signals
FROM @SPORTS_BETTING.PUBLIC.sports_betting_divergence_stage
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';
"""

COPY_SHARP_MONEY_SQL = """
COPY INTO SPORTS_BETTING.PUBLIC.sharp_money_signals
FROM @SPORTS_BETTING.PUBLIC.sports_betting_sharp_money_stage
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';
"""

# ── DAG definition ────────────────────────────────────────────────────────────
default_args = {
    "owner": "vishal",
    "depends_on_past": False,
    "email": ["vbhupathiraju@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="sports_betting_pipeline",
    default_args=default_args,
    description="Trigger Databricks notebooks and load signals into Snowflake",
    schedule_interval="*/15 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["sports-betting", "databricks", "snowflake"],
) as dag:

    # Task 0: Check if we're within a game window — skip entire DAG if not
    check_window = ShortCircuitOperator(
        task_id="check_game_window",
        python_callable=is_within_game_window,
    )

    # Task 1: Trigger Databricks job and wait for completion
    run_databricks = DatabricksRunNowOperator(
        task_id="run_databricks_pipeline",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DATABRICKS_JOB_ID,
        wait_for_termination=True,
        polling_period_seconds=30,
    )

    # Task 2a: COPY INTO divergence_signals
    load_divergence = SnowflakeOperator(
        task_id="load_divergence_signals",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=COPY_DIVERGENCE_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )

    # Task 2b: COPY INTO sharp_money_signals
    load_sharp_money = SnowflakeOperator(
        task_id="load_sharp_money_signals",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=COPY_SHARP_MONEY_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )

    # Pipeline: window check → Databricks → both Snowflake loads in parallel
    check_window >> run_databricks >> [load_divergence, load_sharp_money]
