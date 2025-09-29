# us_treasuries/data_nodes.py
from __future__ import annotations

import datetime as dt
import pytz
from typing import Dict, List, Optional, Union, Tuple

import pandas as pd
import mainsequence.client as msc
from mainsequence.tdag import DataNode
from mainsequence.client.models_tdag import ColumnMetaData
from pydantic import BaseModel  # noqa: F401

from .settings import (
    POLYGON_TREASURY_YIELDS_PATH,
    UST_CMT_FIELD_BY_TENOR,
    UST_TENOR_DAYS,
    UST_CMT_YIELDS_TABLE_UID,
)
from .utils import PolygonClient, normalize_cmt_rows

UTC = pytz.utc


# -----------------------------
# DRY base for Polygon Economy
# -----------------------------

class PolygonEconomyNode(DataNode):
    """
    Base DataNode for Polygon Economy endpoints.
    Subclasses must implement:
      - endpoint_path (str)
      - build_assets() -> list of identifiers to register
      - pull_between(start_date, end_date) -> pd.DataFrame with at least:
           ['time_index','unique_identifier', ...metric columns...]
    """
    endpoint_path: str = ""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client: Optional[PolygonClient] = None

    def build_assets(self) -> List[msc.Asset]:
        raise NotImplementedError

    def pull_between(self, start_date: str, end_date: str) -> pd.DataFrame:
        raise NotImplementedError

    def _client_or_new(self) -> PolygonClient:
        if self._client is None:
            self._client = PolygonClient()
        return self._client


# -----------------------------
# UST Constant-Maturity Yields (Polygon only)
# -----------------------------

class PolygonUSTCMTYields(PolygonEconomyNode):
    """
    Daily U.S. Treasury constant-maturity yields by tenor from Polygon Economy API.
    Identifiers: UST_<tenor}_CMT (e.g., UST_2Y_CMT)
    Columns:
      - days_to_maturity (int)
      - par_yield (decimal)
    """
    endpoint_path = POLYGON_TREASURY_YIELDS_PATH

    def get_column_metadata(self) -> List[ColumnMetaData]:
        return [
            ColumnMetaData(column_name="days_to_maturity", dtype="int",
                           description="Days to maturity", label="Days to Maturity"),
            ColumnMetaData(column_name="par_yield", dtype="float",
                           description="Par Yield (decimal)", label="Par Yield"),
        ]

    def get_table_metadata(self) -> Optional[msc.TableMetaData]:
        return msc.TableMetaData(
            identifier=UST_CMT_YIELDS_TABLE_UID,
            data_frequency_id=msc.DataFrequency.one_d,
            description="U.S. Treasury constant-maturity yields (1m–30y) from Polygon Economy API.",
        )

    def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:
        return {}

    def build_assets(self) -> List[msc.Asset]:
        payload = []
        for tenor in UST_CMT_FIELD_BY_TENOR:
            identifier = f"UST_{tenor}_CMT"
            snapshot = {"name": identifier, "ticker": identifier, "exchange_code": "US_TREASURY"}
            payload.append({
                "unique_identifier": identifier,
                "snapshot": snapshot,
                "security_market_sector": msc.MARKETS_CONSTANTS.FIGI_MARKET_SECTOR_GOVT,
                "security_type": msc.MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_DOMESTIC,
                "security_type_2": msc.MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_2_GOVT,
            })
        return msc.Asset.batch_get_or_register_custom_assets(payload)

    def get_asset_list(self) -> List[msc.Asset]:
        return self.build_assets()

    def _update_windows_per_asset(self) -> Dict[str, Tuple[str, str]]:
        """
        Compute [start, end] per asset (YYYY-MM-DD). End = yesterday UTC.
        Skip assets already up to date (start > end).
        """
        assets = self.update_statistics.asset_list or self.get_asset_list()
        yday = (dt.datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
                - dt.timedelta(days=1)).date()
        out: Dict[str, Tuple[str, str]] = {}
        for a in assets:
            last = self.update_statistics.get_last_update_index_2d(uid=a.unique_identifier)
            if last is None:
                start = dt.date(2010, 1, 1)
            else:
                start = (last + dt.timedelta(days=1)).astimezone(UTC).date()
            if start <= yday:
                out[a.unique_identifier] = (start.isoformat(), yday.isoformat())
        return out

    def pull_between(self, start_date: str, end_date: str) -> pd.DataFrame:
        client = self._client_or_new()
        params = {"date.gte": start_date, "date.lte": end_date, "sort": "date.asc", "limit": 50000}
        rows = client.get_all_pages(self.endpoint_path, params=params)
        return normalize_cmt_rows(rows, UST_CMT_FIELD_BY_TENOR, UST_TENOR_DAYS)

    def update(self) -> pd.DataFrame:
        # Per-asset windows to avoid global backfills
        per_asset = self._update_windows_per_asset()
        if not per_asset:
            return pd.DataFrame()

        # Group assets that share the same [start, end] to dedupe API calls
        grouped: Dict[Tuple[str, str], List[str]] = {}
        for uid, win in per_asset.items():
            grouped.setdefault(win, []).append(uid)

        out_parts: List[pd.DataFrame] = []

        for (start, end), uids in grouped.items():
            cmt_df = self.pull_between(start, end)
            if cmt_df.empty:
                continue

            merged = cmt_df.set_index(["time_index", "unique_identifier"]).sort_index()

            # Emit only rows relevant for each asset in this window
            idx_uids = set(merged.index.get_level_values(1))
            for uid in uids:
                if uid not in idx_uids:
                    continue
                sub = merged.xs(uid, level=1, drop_level=False)
                out_parts.append(sub[["days_to_maturity", "par_yield"]])

        if not out_parts:
            return pd.DataFrame()

        return pd.concat(out_parts).sort_index()
