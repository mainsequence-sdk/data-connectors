# src/data_nodes/algoseek_nodes_figi.py
from __future__ import annotations
from typing import Dict, Union, List, Optional, Iterable, Tuple
import os, gzip, io, glob, datetime
import pytz, pandas as pd

import mainsequence.client as msc
from mainsequence.tdag import DataNode, APIDataNode
from mainsequence.client.models_tdag import UpdateStatistics, ColumnMetaData

UTC = pytz.utc
US_EASTERN = pytz.timezone("US/Eastern")

# === Adjust these paths for your machine ===
BASE_DIR = os.path.expanduser("~/Downloads/demo_data")
TRADES_DIR = os.path.join(BASE_DIR, "us-equity-1min-trades-adjusted-secid-20220101-20220630")
DAILY_DIR  = os.path.join(BASE_DIR, "us-equity-daily-ohlc-primary-secid-20220101-20220630")
SECMASTER_CSV = os.path.join(BASE_DIR, "sp500_equity_security_master.csv")

# ------------------------------
# Helpers
# ------------------------------
def _yyyymmdd_to_date(s: str) -> datetime.date:
    return datetime.datetime.strptime(s, "%Y%m%d").date()

def _parse_time_hhmm_or_hhmmss(t: str) -> Tuple[int, int, int]:
    parts = [int(x) for x in str(t).split(":")]
    if len(parts) == 2:
        return parts[0], parts[1], 0
    elif len(parts) == 3:
        return parts[0], parts[1], parts[2]
    raise ValueError(f"Unexpected time string: {t}")

def _combine_date_time_est_to_utc(date_str: str, time_str: str) -> datetime.datetime:
    """
    Algoseek minute bars: Date (yyyymmdd) + TimeBarStart ('HH:MM' or 'HH:MM:SS') in EST.
    Convert to timezone-aware UTC for the platform time_index.
    """
    d = _yyyymmdd_to_date(date_str)
    hh, mm, ss = _parse_time_hhmm_or_hhmmss(time_str)
    est_dt = US_EASTERN.localize(datetime.datetime(d.year, d.month, d.day, hh, mm, ss))
    return est_dt.astimezone(UTC)

def _ensure_lowercase_maxlen(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).lower()[:63] for c in df.columns]
    return df

def _iter_gz_csv_files(folder: str) -> Iterable[str]:
    for pat in ("*.csv.gz", "*.csv"):
        for f in glob.iglob(os.path.join(folder, pat)):
            yield f

def _read_csv_auto(file_path: str) -> pd.DataFrame:
    if file_path.endswith(".gz"):
        with gzip.open(file_path, "rb") as f:
            return pd.read_csv(io.BytesIO(f.read()))
    return pd.read_csv(file_path)

def _utc_midnight_today_minus(days: int = 1) -> datetime.datetime:
    now = datetime.datetime.now(UTC)
    return now.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=days)

def _last_token(s: str) -> str:
    if not isinstance(s, str) or not s:
        return ""
    toks = [t.strip() for t in s.split(";") if t.strip()]
    return toks[-1] if toks else ""

def _load_security_master_mapping(csv_path: str) -> pd.DataFrame:
    """
    Returns a DataFrame with one row per SecId containing:
      secid, figi (last/current), ticker (last/current), name (last/current), primaryexchange (last/current)
    Rows with missing FIGI are dropped.
    """
    df = pd.read_csv(csv_path)
    out = pd.DataFrame({
        "secid": df["SecId"].astype(str),
        "figi": df.get("FIGI", "").apply(_last_token),
        "ticker": df.get("Tickers", "").apply(_last_token),
        "name": df.get("Name", "").apply(_last_token),
        "primaryexchange": df.get("PrimaryExchange", "").apply(_last_token),
        "sector": df.get("Sector", ""),
        "industry": df.get("Industry", ""),
        "liststatus": df.get("ListStatus", "")
    })
    out = out[out["figi"].astype(str).str.len() > 0].drop_duplicates(subset=["secid"], keep="last").reset_index(drop=True)
    return _ensure_lowercase_maxlen(out)

def _chunked(seq: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def _get_or_register_assets_from_figis(figis: List[str], batch_size: int = 500) -> List[msc.Asset]:
    """
    1) Find existing via Asset.filter(unique_identifier__in=...)
    2) Register missing via Asset.register_figi_as_asset_in_main_sequence_venue(figi)
    Returns msc.Asset objects (existing + newly registered).
    """
    figis = sorted(set([f for f in figis if f]))
    existing_assets: List[msc.Asset] = []
    existing_figis: set = set()

    # Find existing in batches (avoid unknown per_page arg)
    for chunk in _chunked(figis, batch_size):
        found = msc.Asset.filter(unique_identifier__in=chunk)
        existing_assets.extend(found)
        existing_figis.update([a.unique_identifier for a in found])

    # Register missing
    missing = [f for f in figis if f not in existing_figis]
    newly_registered: List[msc.Asset] = []
    for figi in missing:
        try:
            a = msc.Asset.register_figi_as_asset_in_main_sequence_venue(figi=figi)
            newly_registered.append(a)
        except Exception:
            # Asset may already exist or FIGI invalid – continue with others.
            continue

    return existing_assets + newly_registered



# ------------------------------
# 1) Security Master — FIGI snapshot (MultiIndex: time_index, unique_identifier=FIGI)
# ------------------------------
class AlgoseekSecurityMasterSnapshotFIGI(DataNode):
    """Security Master snapshot using FIGI as unique_identifier (plus secid/ticker/etc. as columns)."""

    _ARGS_IGNORE_IN_STORAGE_HASH = ["csv_path"]

    def __init__(self, csv_path: Optional[str]=None , *args, **kwargs):
        self.csv_path = csv_path
        super().__init__(*args, **kwargs)

    def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:
        return {}

    def get_table_metadata(self) -> Optional[msc.TableMetaData]:
        return msc.TableMetaData(
            identifier="algoseek_security_master_snapshot_figi",
            data_frequency_id=msc.DataFrequency.one_d,
            description="Snapshot of Algoseek Security Master (latest values) with FIGI as unique_identifier."
        )

    def get_column_metadata(self) -> List[ColumnMetaData]:
        return [
            ColumnMetaData(column_name="secid", dtype="string", label="Algoseek SecId"),
            ColumnMetaData(column_name="ticker", dtype="string", label="Ticker"),
            ColumnMetaData(column_name="name", dtype="string", label="Name"),
            ColumnMetaData(column_name="primaryexchange", dtype="string", label="Primary Exchange"),
            ColumnMetaData(column_name="sector", dtype="string", label="Sector"),
            ColumnMetaData(column_name="industry", dtype="string", label="Industry"),
            ColumnMetaData(column_name="liststatus", dtype="string", label="List Status"),
        ]

    def get_asset_list(self) -> List[msc.Asset]:
        mapping = _load_security_master_mapping(self.csv_path)
        figis = mapping["figi"].tolist()
        return _get_or_register_assets_from_figis(figis)

    def update(self) -> pd.DataFrame:
        us: UpdateStatistics = self.update_statistics
        today_utc = datetime.datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        if us.max_time_index_value is not None and us.max_time_index_value >= today_utc:
            return pd.DataFrame()  # already emitted today's snapshot

        mapping = _load_security_master_mapping(self.csv_path)
        mapping = mapping.rename(columns={"figi": "unique_identifier"})
        mapping["time_index"] = today_utc

        out = mapping[
            ["time_index", "unique_identifier", "secid", "ticker", "name", "primaryexchange", "sector", "industry", "liststatus"]
        ]
        out = out.set_index(["time_index", "unique_identifier"]).sort_index()
        return out



if __name__ == "__main__":

    node=AlgoseekSecurityMasterSnapshotFIGI(csv_path=SECMASTER_CSV)
    node.run()

    a=5

