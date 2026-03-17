"""
Config Loader
-------------
Shared module for loading sports_config.json at runtime.

Hot-reload: called at the top of every poll cycle so changes to
sports_config.json take effect within one cycle — no container restart needed.

Fallback: if the file fails to parse (e.g. mid-edit malformed JSON),
the last valid config is returned and a warning is logged. The producer
keeps running on stale config rather than crashing.

Schedule formats supported:

  Simple (same window every active day):
    "schedule": {
      "timezone": "America/Los_Angeles",
      "days": ["monday", "tuesday", ...],
      "start_time": "15:00",
      "end_time": "23:00"
    }

  Per-day (different window per day):
    "schedule": {
      "timezone": "America/Los_Angeles",
      "day_schedules": {
        "tuesday":  {"start_time": "15:00", "end_time": "21:00"},
        "thursday": {"start_time": "08:00", "end_time": "21:00"},
        ...
      }
    }

  Always on (no time restriction):
    "schedule": null
"""

import json
import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "sports_config.json")

_last_valid_config = None


def load_config() -> dict:
    """
    Load and return sports_config.json.
    Falls back to last valid config if file is missing or malformed.
    """
    global _last_valid_config

    try:
        with open(CONFIG_PATH, "r") as f:
            config = json.load(f)
        _last_valid_config = config
        return config
    except FileNotFoundError:
        logger.error("sports_config.json not found at %s", CONFIG_PATH)
    except json.JSONDecodeError as e:
        logger.warning(
            "sports_config.json is malformed (mid-edit?): %s — using last valid config", e
        )

    if _last_valid_config is not None:
        return _last_valid_config

    raise RuntimeError(
        "sports_config.json could not be loaded and no cached config exists. "
        "Cannot start producer."
    )


def get_active_sports(config: dict) -> list:
    """Return list of sport keys that are currently active."""
    return [
        sport_key
        for sport_key, sport_cfg in config["sports"].items()
        if sport_cfg.get("active", False)
    ]


def _get_todays_window(schedule: dict) -> tuple:
    """
    Return (start_time_str, end_time_str) for today based on the schedule,
    or (None, None) if today is not a scheduled day.

    Handles both simple and per-day schedule formats.
    """
    tz = ZoneInfo(schedule["timezone"])
    now = datetime.now(tz)
    current_day = now.strftime("%A").lower()

    # Per-day format
    if "day_schedules" in schedule:
        day_cfg = schedule["day_schedules"].get(current_day)
        if not day_cfg:
            return None, None
        return day_cfg["start_time"], day_cfg["end_time"]

    # Simple format
    allowed_days = [d.lower() for d in schedule.get("days", [])]
    if current_day not in allowed_days:
        return None, None
    return schedule.get("start_time", "00:00"), schedule.get("end_time", "23:59")


def is_within_schedule(sport_cfg: dict) -> bool:
    """
    Check if the current time falls within the sport's scheduled window.

    Returns True if:
    - schedule is null (always on)
    - current day and time are within the configured window
    """
    schedule = sport_cfg.get("schedule")
    if schedule is None:
        return True

    tz = ZoneInfo(schedule["timezone"])
    now = datetime.now(tz)
    current_time = now.strftime("%H:%M")

    start, end = _get_todays_window(schedule)
    if start is None:
        return False

    return start <= current_time <= end


def get_poll_interval(sport_cfg: dict) -> int:
    """
    Return the appropriate poll interval in seconds based on schedule.

    Inside window:  poll_interval_active_seconds
    Outside window: poll_interval_idle_seconds
    """
    if is_within_schedule(sport_cfg):
        return sport_cfg.get("poll_interval_active_seconds", 30)
    return sport_cfg.get("poll_interval_idle_seconds", 300)


def seconds_until_next_window(sport_cfg: dict) -> int:
    """
    Calculate seconds until the next scheduled window opens.
    Used for smart sleep when all sports are outside their windows.
    Returns 300 (5 min) if schedule is null or no upcoming window found.
    """
    schedule = sport_cfg.get("schedule")
    if schedule is None:
        return 300

    tz = ZoneInfo(schedule["timezone"])
    now = datetime.now(tz)

    for days_ahead in range(8):
        candidate = now + timedelta(days=days_ahead)
        candidate_day = candidate.strftime("%A").lower()

        # Get window for this candidate day
        if "day_schedules" in schedule:
            day_cfg = schedule["day_schedules"].get(candidate_day)
            if not day_cfg:
                continue
            start_time = day_cfg["start_time"]
        else:
            allowed_days = [d.lower() for d in schedule.get("days", [])]
            if candidate_day not in allowed_days:
                continue
            start_time = schedule.get("start_time", "00:00")

        start_h, start_m = map(int, start_time.split(":"))
        window_start = candidate.replace(
            hour=start_h, minute=start_m, second=0, microsecond=0
        )

        # Skip if this window has already started today
        if window_start <= now:
            continue

        diff = int((window_start - now).total_seconds())
        if diff > 0:
            return diff

    return 300
