"""
sensors.py
----------
Custom Airflow sensor that checks whether the NYSE is currently open.
Used as the first task in fetch_market_data DAG to avoid wasted API calls.
"""

import pendulum
from airflow.sensors.base import BaseSensorOperator

try:
    import trading_calendars as tc
    nyse = tc.get_calendar("NYSE")
    CALENDAR_AVAILABLE = True
except Exception:
    CALENDAR_AVAILABLE = False


class MarketOpenSensor(BaseSensorOperator):
    """
    Pokes every `poke_interval` seconds until NYSE is open.
    If the market is closed (weekend/holiday/after hours), the sensor
    reschedules itself and the rest of the DAG does not run.
    """

    def poke(self, context) -> bool:
        now_utc = pendulum.now("UTC")
        now_et  = now_utc.in_timezone("America/New_York")

        # If calendar library unavailable, fall back to simple time check
        if not CALENDAR_AVAILABLE:
            is_weekday    = now_et.weekday() < 5
            market_open   = now_et.replace(hour=9,  minute=30, second=0)
            market_close  = now_et.replace(hour=16, minute=0,  second=0)
            is_open = is_weekday and market_open <= now_et <= market_close
        else:
            try:
                is_open = (
                    nyse.is_session(now_et.date())
                    and nyse.is_open_on_minute(now_utc)
                )
            except Exception:
                is_open = False

        self.log.info(f"Market open check at {now_et.strftime('%Y-%m-%d %H:%M ET')}: {is_open}")
        return is_open
