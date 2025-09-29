# us_treasuries/settings.py
from typing import Dict

# --------------- Polygon base ----------------
POLYGON_API_BASE = "https://api.polygon.io"
POLYGON_TREASURY_YIELDS_PATH = "/fed/v1/treasury-yields"  # Economy → Treasury Yields

# Local market time the inputs are captured for the CMT curve
CMT_OBSERVATION_TZ = "America/New_York"
CMT_OBSERVATION_HOUR = 15   # ≈3:30 p.m. local
CMT_OBSERVATION_MINUTE = 30

# --------------- Tenor map -------------------
UST_CMT_FIELD_BY_TENOR: Dict[str, str] = {
    "1m":  "yield_1_month",
    "3m":  "yield_3_month",
    "6m":  "yield_6_month",
    "1y":  "yield_1_year",
    "2y":  "yield_2_year",
    "3y":  "yield_3_year",
    "5y":  "yield_5_year",
    "7y":  "yield_7_year",
    "10y": "yield_10_year",
    "20y": "yield_20_year",
    "30y": "yield_30_year",
}

# Approximate day counts for curve bootstrapping
UST_TENOR_DAYS: Dict[str, int] = {
    "1m": 30, "3m": 91, "6m": 182,
    "1y": 365, "2y": 730, "3y": 1095, "5y": 1825, "7y": 2555,
    "10y": 3650, "20y": 7300, "30y": 10950,
}

# --------------- Table identifiers ----------------
UST_CMT_YIELDS_TABLE_UID = "polygon_ust_cmt_usd"            # CMT by tenor
UST_CMT_ZERO_CURVE_UID  = "polygon_ust_cmt_zero_curve_usd"  # bootstrapped curve
