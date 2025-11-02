# src/data_nodes/nodes.py
from __future__ import annotations
from typing import Dict, List, Optional, Iterable, Tuple, Union
import datetime
import pandas as pd
import pytz
import glob
import gzip

import mainsequence.client as msc
from mainsequence.tdag import DataNode
from mainsequence.client.models_tdag import ColumnMetaData
from pathlib import Path

UTC = pytz.utc

# ------------------------------
# Helpers
# ------------------------------
def _ensure_lowercase_maxlen(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).lower()[:63] for c in df.columns]
    return df

def _split_semicolon(s: object) -> List[str]:
    if s is None:
        return []
    return [t.strip() for t in str(s).split(";") if t and t.strip()]

def _unique_preserve_order(items: List[str]) -> List[str]:
    seen, out = set(), []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def _yyyymmdd_to_date(s: str) -> datetime.date:
    return datetime.datetime.strptime(s, "%Y%m%d").date()

def _range_token_to_dates(token: str) -> Tuple[datetime.date, datetime.date]:
    # token is "yyyymmdd:yyyymmdd"; end may be 20991231 (ongoing)
    start_str, end_str = token.split(":")
    return _yyyymmdd_to_date(start_str), _yyyymmdd_to_date(end_str)

# Canonical column list (Algoseek Security Master guide)
CANONICAL_COLS = [
    "SecId", "Tickers", "TickersStartToEndDate", "Name", "NameStartToEndDate", "ISIN",
    "ISINStartToEndDate", "ListStatus", "SecurityDescription", "USIdentifier",
    "USIdentifierStartToEndDate", "PrimaryExchange", "PrimaryExchangeStartToEndDate",
    "SEDOL", "Sic", "Sector", "Industry", "FIGI",
]

def _explode_sm_mapping_by_figi(sm_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert raw Security Master (one row per SecId with semicolon lists) into **one row per FIGI**.

    - Replicates a SecId row for **each FIGI token** (multiple FIGIs per SecId are expected).
    - Preserves **all CSV columns**; they remain semicolon-joined strings.
    """
    cols_present = [c for c in CANONICAL_COLS if c in sm_df.columns]
    base = sm_df[cols_present].copy()

    rows = []
    for _, r in base.iterrows():
        secid = str(r.get("SecId"))
        figis = _unique_preserve_order(_split_semicolon(r.get("FIGI")))
        if not figis:
            continue

        for figi in figis:
            new = r.to_dict()
            new["SecId"] = secid
            new["FIGI"] = figi
            new["latest_figi"]=figis[-1]
            rows.append(new)

    out = pd.DataFrame(rows)

    # Attach any extra columns beyond CANONICAL_COLS (left-merge by SecId)
    extras = [c for c in sm_df.columns if c not in CANONICAL_COLS]
    if extras:
        extras_df = sm_df[["SecId"] + extras].drop_duplicates(subset=["SecId"])
        out = out.merge(extras_df, on="SecId", how="left")

    out = _ensure_lowercase_maxlen(out)
    out = out.rename(columns={"figi": "unique_identifier"})
    out["secid"] = out["secid"].astype(str)
    out = out.drop_duplicates(subset=["unique_identifier", "secid"]).reset_index(drop=True)
    return out

def _load_security_master_mapping_figi_rows(csv_path: str) -> pd.DataFrame:
    """
    Read the Algoseek Security Master CSV and return a DataFrame with **one row per FIGI**,
    keeping all original columns (semicolon lists and yyyymmdd:yyyymmdd ranges) as strings.
    """
    if not csv_path:
        raise ValueError("Security Master csv_path is required")
    raw = pd.read_csv(csv_path, dtype=str).fillna("")
    return _explode_sm_mapping_by_figi(raw)

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


    # Find existing in batches (avoid unknown per_page arg)

    existing_assets = msc.Asset.query(unique_identifier__in=figis, per_page=batch_size)
    existing_figis = {a.unique_identifier: a for a in existing_assets}

    # Register missing
    missing = [f for f in figis if f not in existing_figis]
    newly_registered: List[msc.Asset] = []
    for figi in missing:
        try:
            a = msc.Asset.register_asset_from_figi(figi=figi)
            newly_registered.append(a)

        except Exception as e:
            # Asset may already exist or FIGI invalid – continue with others.
            raise e

    return existing_assets + newly_registered

def _load_figi_to_secid_lookup(lookup_csv_path: str) -> pd.DataFrame:
    """
    Load **FIGI → SecId** lookup with **StartDate/EndDate** and return a frame:
      columns: figi, secid, start_date (date), end_date (date)
    This is the correct, date-aware mapping source per Algoseek docs (Table 7–8). :contentReference[oaicite:2]{index=2}
    """
    if not lookup_csv_path:
        raise ValueError("FIGI to SecId lookup csv_path is required")
    df = pd.read_csv(lookup_csv_path, dtype=str).fillna("")
    df = df.rename(columns={"FIGI": "figi", "SecId": "secid", "StartDate": "startdate", "EndDate": "enddate"})
    # convert to dates; accept both 20991231/29991231 as "ongoing"
    df["start_date"] = df["startdate"].apply(_yyyymmdd_to_date)
    df["end_date"]   = df["enddate"].apply(_yyyymmdd_to_date)
    df["secid"] = df["secid"].astype(str)
    df = df[["figi", "secid", "start_date", "end_date"]]
    return df


# ------------------------------
# DataNode
# ------------------------------
class AlgoSeekSecurityMaster(DataNode):
    """
    Algoseek Security Master flattened by FIGI.

    - `unique_identifier` **is FIGI** (platform-wide identity).
    - The node is a **static snapshot**: every FIGI has exactly **one** `time_index` (`OFFSET_START`).
    - If the source CSV changes, the entire table is re-emitted and the platform upserts by (time_index, unique_identifier).
    """

    _ARGS_IGNORE_IN_STORAGE_HASH = ["csv_path"]
    OPEN_TO_PUBLIC = True

    def __init__(self,is_demo:bool, csv_path: Optional[str] = None, *args, **kwargs):
        self.csv_path = csv_path
        self.is_demo=is_demo
        super().__init__(*args, **kwargs)

    def dependencies(self) -> Dict[str, "DataNode"]:
        return {}

    def get_table_metadata(self) -> Optional[msc.TableMetaData]:
        identifier="algoseek_security_master"
        if self.is_demo:
            identifier=identifier+"_DEMO"
        return msc.TableMetaData(
            identifier=identifier,
            data_frequency_id=msc.DataFrequency.one_d,
            description="Algoseek Security Master flattened by FIGI (one constant snapshot; FIGI is unique_identifier).",
        )

    def get_column_metadata(self) -> List[ColumnMetaData]:
        return [
            ColumnMetaData(
                column_name="unique_identifier",
                dtype="string",
                label="FIGI",
                description="Financial Instrument Global Identifier used as the platform unique_identifier. Algoseek uses the Primary Exchange Composite FIGI when available; otherwise FIGI."
            ),
            ColumnMetaData(column_name="secid", dtype="string", label="Algoseek SecId", description="Algoseek unique security identifier."),
            ColumnMetaData(column_name="tickers", dtype="string", label="Historical tickers", description="List of symbol names used over time; values separated by semicolons."),
            ColumnMetaData(column_name="tickersstarttoenddate", dtype="string", label="Ticker start:end ranges", description="Start and end dates (yyyymmdd:yyyymmdd) for each ticker; far-future end date (e.g., 20991231) means ongoing."),
            ColumnMetaData(column_name="name", dtype="string", label="Historical names", description="List of security names used over time; values separated by semicolons."),
            ColumnMetaData(column_name="namestarttoenddate", dtype="string", label="Name start:end ranges", description="Start and end dates (yyyymmdd:yyyymmdd) for each name; far-future end date means ongoing."),
            ColumnMetaData(column_name="isin", dtype="string", label="Historical ISINs", description="List of ISIN codes used over time; values separated by semicolons (may be blank)."),
            ColumnMetaData(column_name="isinstarttoenddate", dtype="string", label="ISIN start:end ranges", description="Start and end dates (yyyymmdd:yyyymmdd) for each ISIN; far-future end date means ongoing (may be blank)."),
            ColumnMetaData(column_name="liststatus", dtype="string", label="List Status", description="Current list status: A = Announced, D = Delisted, L = Listed (may be blank)."),
            ColumnMetaData(column_name="securitydescription", dtype="string", label="Security Type", description="Current security type (e.g., Equity Shares, ETF, Depository Receipts; may be blank)."),
            ColumnMetaData(column_name="usidentifier", dtype="string", label="Historical US identifiers", description="List of US identifiers used over time; values separated by semicolons (may be blank)."),
            ColumnMetaData(column_name="usidentifierstarttoenddate", dtype="string", label="US Identifier start:end ranges", description="Start and end dates (yyyymmdd:yyyymmdd) for each US identifier; far-future end date means ongoing (may be blank)."),
            ColumnMetaData(column_name="primaryexchange", dtype="string", label="Historical primary exchanges", description="List of primary exchanges used over time; values separated by semicolons."),
            ColumnMetaData(column_name="primaryexchangestarttoenddate", dtype="string", label="Primary exchange start:end ranges", description="Start and end dates (yyyymmdd:yyyymmdd) for each primary exchange; far-future end date means ongoing."),
            ColumnMetaData(column_name="sedol", dtype="string", label="SEDOL (deprecated)", description="Stock Exchange Daily Official List; historical only and no longer updated (may be blank)."),
            ColumnMetaData(column_name="sic", dtype="string", label="SIC code", description="Current Standard Industrial Classification code (may be blank)."),
            ColumnMetaData(column_name="sector", dtype="string", label="Sector", description="Current SIC sector (may be blank)."),
            ColumnMetaData(column_name="industry", dtype="string", label="Industry", description="Current SIC industry (may be blank)."),
            ColumnMetaData(column_name="latest_figi", dtype="string", label="Latest Figi",
                           description="The latest figi in the  sec id row"),
        ]

    def get_asset_list(self) -> List[msc.Asset]:
        mapping = _load_security_master_mapping_figi_rows(self.csv_path)
        figis = mapping["unique_identifier"].tolist()
        self.mapping=mapping
        return _get_or_register_assets_from_figis(figis)

    def update(self) -> pd.DataFrame:
        """
        Special “static snapshot” update.

        Behavior
        --------
        - Emits **one row per FIGI** and sets the **same `time_index` for all rows** to `self.OFFSET_START`.
        - The table is **static per FIGI**: each FIGI has **exactly one timestamp**.
        - On every run, we **rebuild the full snapshot from the CSV** and **return all rows**.
          If the source file is unchanged, the upsert is effectively a no-op. If it changed,
          the platform upserts by MultiIndex (`time_index`, `unique_identifier`).

        Rationale
        ---------
        - Algoseek encodes history inside columns (semicolon lists + yyyymmdd:yyyymmdd ranges),
          not as time-indexed rows. A single snapshot per FIGI is the clearest contract.
        """
        mapping=self.mapping
        mapping["time_index"] = pd.Timestamp(self.OFFSET_START)+datetime.timedelta(minutes=1)
        mapping["time_index"]=mapping["time_index"].astype("datetime64[ns, UTC]")
        mapping=mapping.set_index(["time_index", "unique_identifier"]).sort_index()
        self.overwrite_latest_value=True
        return  mapping

    # --------------------------
    # Date-aware ticker lookup
    # --------------------------
    def lookup_figi_and_secid_by_ticker(self, tickers: List[str],
                                        on_date: Union[datetime.date, datetime.datetime, str]) -> pd.DataFrame:
        """
        Resolve (FIGI, SecId) for the given `tickers` **on a specific date**.

        - Fetch the snapshot with `get_data_between_dates(OFFSET_START, OFFSET_START)`.
        - Pair `tickers` with `tickersstarttoenddate`; keep entries where `on_date` falls in [start, end].
        - Returns lowercase columns: `figi`, `ticker`, `secid`.
        """
        if isinstance(on_date, str):
            on_date = datetime.datetime.strptime(on_date, "%Y-%m-%d").date()
        elif isinstance(on_date, datetime.datetime):
            on_date = on_date.date()

        df = self.get_data_between_dates(start_date=self.OFFSET_START, end_date=self.OFFSET_START)
        if df is None or df.empty:
            return pd.DataFrame(columns=["figi", "ticker", "secid"])

        sm = df.reset_index()  # time_index, unique_identifier, ...
        for col in {"unique_identifier", "secid", "tickers", "tickersstarttoenddate"} - set(sm.columns):
            sm[col] = ""

        want = {t.lower() for t in tickers}
        matches = []

        for _, row in sm.iterrows():
            ticker_tokens = _split_semicolon(row["tickers"])
            range_tokens = _split_semicolon(row["tickersstarttoenddate"])
            for i, tk in enumerate(ticker_tokens):
                if tk.lower() not in want:
                    continue
                if i >= len(range_tokens):
                    continue
                start_d, end_d = _range_token_to_dates(range_tokens[i])
                if start_d <= on_date <= end_d:
                    matches.append({"figi": row["unique_identifier"], "ticker": tk, "secid": str(row["secid"])})

        out = pd.DataFrame(matches).drop_duplicates().reset_index(drop=True)
        return _ensure_lowercase_maxlen(out)





# ------------------------------
# DataNode: Daily OHLC (reads all CSVs, depends on Security Master)
# ------------------------------




class AlgoSeekDailyOHLCFIGI(DataNode):
    """
    Algoseek US Equity **Primary Exchange Daily OHLC** (FIGI-indexed).

    - **Depends on** the Security Master node for DAG ordering and the CSV path.
    - Reads *all* CSV files in `daily_dir` (e.g., `.../us-equity-daily-ohlc-primary-secid-YYYYMMDD-YYYYMMDD/*.csv`).
    - Maps `SecId` → **FIGI** using the Security Master CSV (**last FIGI** per SecId).
    - MultiIndex: (`time_index` UTC midnight of `TradeDate`, `unique_identifier` = FIGI).
    """

    _ARGS_IGNORE_IN_STORAGE_HASH = ["daily_dir", "security_master_path"]
    OPEN_TO_PUBLIC = True
    # ---- one source of truth for columns ----
    DAILY_FIELDS: "Dict[str, Dict[str, str]]" = dict([
        # SRC NAME           → out_name, dtype, label, description
        ("SecId",
         {"name": "secid", "dtype": "string", "label": "Algoseek SecId", "desc": "Algoseek unique Security ID"}),

        ("Ticker", {"name": "ticker", "dtype": "string", "label": "Ticker", "desc": "Symbol name"}),
        ("Name", {"name": "name", "dtype": "string", "label": "Name", "desc": "Name of equity security"}),
        ("PrimaryExchange", {"name": "primaryexchange", "dtype": "string", "label": "Primary Exchange",
                             "desc": "Primary listing exchange on this TradeDate"}),
        ("ISIN", {"name": "isin", "dtype": "string", "label": "ISIN", "desc": "ISIN as of this trade date (optional)"}),
        ("OpenPrice",
         {"name": "open", "dtype": "float", "label": "Open Price", "desc": "Primary exchange opening trade"}),
        # :contentReference[oaicite:4]{index=4}
        ("OpenSize",
         {"name": "opensize", "dtype": "int", "label": "Open Size", "desc": "Primary exchange open trade size"}),
        ("OpenTime", {"name": "open_time", "dtype": "string", "label": "Open Time",
                      "desc": "Time of the Primary exchange opening trade"}),
        ("HighPrice", {"name": "high", "dtype": "float", "label": "High Price",
                       "desc": "Highest trade price from any exchange or TRF"}),
        (
        "HighTime", {"name": "hightime", "dtype": "string", "label": "High Time", "desc": "Time of the highest trade"}),
        ("LowPrice", {"name": "low", "dtype": "float", "label": "Low Price",
                      "desc": "Lowest trade price from any exchange or TRF"}),
        ("LowTime", {"name": "lowtime", "dtype": "string", "label": "Low Time", "desc": "Time of the lowest trade"}),
        ("ClosePrice",
         {"name": "close", "dtype": "float", "label": "Close Price", "desc": "Primary exchange closing trade"}),
        # :contentReference[oaicite:5]{index=5}
        ("CloseSize",
         {"name": "closesize", "dtype": "int", "label": "Close Size", "desc": "Primary exchange close trade size"}),

        ("ListedMarketHoursVolume",
         {"name": "listedmarkethoursvolume", "dtype": "int", "label": "Listed Market Hours Volume",
          "desc": "Public listed exchanges volume during regular hours"}),
        ("ListedMarketHoursTrades",
         {"name": "listedmarkethourstrades", "dtype": "int", "label": "Listed Market Hours Trades",
          "desc": "Number of trades during regular market hours in public listed exchanges"}),
        ("Volume", {"name": "volume", "dtype": "int", "label": "Listed Total Volume",
                               "desc": "Public listed exchanges volume for the whole day"}),
        ("ListedTotalTrades", {"name": "listedtotaltrades", "dtype": "int", "label": "Listed Total Trades",
                               "desc": "Total number of trades for the trade date (listed exchanges)"}),
        ("FinraMarketHoursVolume",
         {"name": "finramarkethoursvolume", "dtype": "int", "label": "FINRA Market Hours Volume",
          "desc": "FINRA/TRF trading volume during regular market hours"}),
        ("FinraMarketHoursTrades",
         {"name": "finramarkethourstrades", "dtype": "int", "label": "FINRA Market Hours Trades",
          "desc": "Number of FINRA/TRF trades during regular market hours"}),
        ("FinraTotalVolume", {"name": "finratotalvolume", "dtype": "int", "label": "FINRA Total Volume",
                              "desc": "FINRA/TRF trading volume for the whole day"}),
        ("FinraTotalTrades", {"name": "finratotaltrades", "dtype": "int", "label": "FINRA Total Trades",
                              "desc": "Total number of FINRA/TRF trades for the trade date"}),
        ("CumulativePriceFactor",
         {"name": "cumulativepricefactor", "dtype": "float", "label": "Cumulative Price Factor",
          "desc": "Cumulative price factor"}),
        ("CumulativeVolumeFactor",
         {"name": "cumulativevolumefactor", "dtype": "float", "label": "Cumulative Volume Factor",
          "desc": "Cumulative volume factor"}),
        ("AdjustmentFactor",
         {"name": "adjustmentfactor", "dtype": "float", "label": "Adjustment Factor", "desc": "Adjustment factor"}),
        ("AdjustmentReason",
         {"name": "adjustmentreason", "dtype": "string", "label": "Adjustment Reason", "desc": "Adjustment reason"}),
        ("MarketVWAP", {"name": "vwap", "dtype": "float", "label": "Market VWAP",
                        "desc": "VWAP during regular hours incl. Open/Close cross"}),
        ("DailyVWAP", {"name": "dailyvwap", "dtype": "float", "label": "Daily VWAP", "desc": "VWAP for the whole day"}),
    ])

    def __init__(self, daily_dir: str, security_master_path, is_demo: bool, *args, **kwargs):
        self.daily_dir = daily_dir
        self.security_master_node = AlgoSeekSecurityMaster(csv_path=security_master_path,is_demo=is_demo)
        self.is_demo = is_demo
        super().__init__(*args, **kwargs)

    def dependencies(self) -> Dict[str, "DataNode"]:
        # Ensure master runs first
        return {"security_master_node": self.security_master_node}

    def get_table_metadata(self) -> Optional[msc.TableMetaData]:
        identifier = "algoseek_daily_ohlc"
        if self.is_demo:
            identifier = identifier + "_DEMO"
        return msc.TableMetaData(
            identifier=identifier,
            data_frequency_id=msc.DataFrequency.one_d,
            description="Algoseek Primary Exchange Daily OHLC (official open/close), FIGI-indexed; scans all CSVs in daily_dir.",
        )

    def get_column_metadata(self) -> List[ColumnMetaData]:
        # Build from one source of truth
        metas = []
        for src, spec in self.DAILY_FIELDS.items():
            # Skip TradeDate – it becomes the time_index, not a column
            if src == "TradeDate":
                continue
            metas.append(ColumnMetaData(
                column_name=spec["name"],
                dtype=spec["dtype"],
                label=spec["label"],
                description=spec["desc"],
            ))
        # plus the mapping column we add
        metas.append(ColumnMetaData(column_name="secid", dtype="string", label="Algoseek SecId used for mapping",
                                    description="SecId left as a data column after mapping to FIGI."))
        return metas

    def get_asset_list(self) -> List[msc.Asset]:
        master_list=self.security_master_node.get_df_between_dates()
        all_sec_ids=[]
        for df_path in self._iter_daily_csvs():
            secid=Path(df_path).stem
            all_sec_ids.append(secid)

        #this will repeat all the figis with the same secid so we have duplicated data per figi, this is better as we can query per figi the full story taht corresponds to the sec id
        available_assets=master_list[master_list.secid.isin(all_sec_ids)].index.get_level_values("unique_identifier").to_list()
        self.security_master_df=master_list
        asset_list=_get_or_register_assets_from_figis(figis=available_assets)


        self.sec_id_to_figi_map =  (
                                        self.security_master_df
                                            .reset_index()
                                            .groupby("secid", sort=False)["unique_identifier"]
                                            .apply(lambda s: list(dict.fromkeys(s.dropna())))
                                            .to_dict()
                                    )



        return asset_list

    def _iter_daily_csvs(self) -> Iterable[str]:
        for path in glob.iglob(os.path.join(self.daily_dir, "*.csv")):
            yield path

    def update(self) -> pd.DataFrame:
        """
        Scan **all CSVs** in `daily_dir`, map `SecId→FIGI` via the Security Master CSV, and emit FIGI-indexed daily bars.

        - `time_index` is **UTC midnight** of `TradeDate` (yyyymmdd).
        - `unique_identifier` is **FIGI** (kept in index); `secid` persists as a data column.
        """
        us = self.update_statistics


        frames=[]
        for df_path in self._iter_daily_csvs():
            secid = Path(df_path).stem
            df = pd.read_csv(df_path)
            for figi in self.sec_id_to_figi_map[secid]:
                tmp_df=df.copy()
                tmp_df["unique_identifier"]=figi
                frames.append(tmp_df)

        if not frames:
            return pd.DataFrame()

        raw = pd.concat(frames, ignore_index=True)



        dates = pd.to_datetime(raw["TradeDate"].astype(str), format="%Y%m%d")  # naive date
        times = pd.to_timedelta(raw["CloseTime"].astype(str))
        open_time=pd.to_timedelta(raw["OpenTime"].astype(str))
        local_dt = (dates + times).dt.tz_localize("America/New_York")
        open_time= (dates + open_time).dt.tz_localize("America/New_York").astype("int64")
        # 4) Build time_index (UTC midnight)
        raw["time_index"] = local_dt.dt.tz_convert("UTC")
        raw["open_time"] = open_time

        raw=raw.drop(columns=["CloseTime","OpenTime"])
        raw=raw.rename(columns={"ListedTotalVolume":"volume","OpenPrice":"open","HighPrice":"high","LowPrice":"low",
                                "ClosePrice":"close","MarketVWAP":"vwap"
                                })
        raw.columns=[c.lower() for c in raw.columns]

        raw[["open", "close", "high", "low", "volume","vwap"]]
        out = raw.set_index(["time_index", "unique_identifier"]).sort_index()
        return out



# ------------------------------
# DataNode: 1‑minute Trades (reads .csv.gz by SecId, depends on Security Master)
# ------------------------------
class AlgoSeek1MinTradesAdjusted(DataNode):
    """
    Algoseek US Equity **Trade Only Adjusted Minute Bars** (FIGI-indexed), reading SecId-aggregated gzipped CSVs.

    - **Depends on** the Security Master node for DAG ordering and the SecId→FIGI mapping.
    - Scans `trades_dir` for files like `18679.csv.gz` (one SecId per file; typically per year).
    - Replicates each SecId's rows **for each mapped FIGI** so you can query the full history by FIGI.
    - `time_index` is the **observation time** at the end of the minute:
        UTC( (Date + TimeBarStart + 1 minute) in America/New_York ).
      See algoseek “Bar Notes”: a one‑minute bar “11:04” is from time >11:04 to <11:05, and times are EST/ET. :contentReference[oaicite:2]{index=2}
    """

    _ARGS_IGNORE_IN_STORAGE_HASH = ["trades_dir", "security_master_path"]
    OPEN_TO_PUBLIC = True

    # one source of truth for columns we keep (lowercase)
    TRADE_FIELDS: "Dict[str, Dict[str, str]]" = dict([
        # SRC NAME                → out_name, dtype, label, description
        ("SecId", {"name": "secid", "dtype": "string", "label": "Algoseek SecId",
                   "desc": "Algoseek unique security identifier used for file naming and mapping."}),
        ("Ticker", {"name": "ticker", "dtype": "string", "label": "Ticker", "desc": "Symbol as of the row's trading date"}),
        ("FirstTradePrice", {"name": "open", "dtype": "float", "label": "First Trade Price", "desc": "Price of the first trade in the minute"}),
        ("HighTradePrice", {"name": "high", "dtype": "float", "label": "High Trade Price", "desc": "Highest trade price in the minute"}),
        ("LowTradePrice", {"name": "low", "dtype": "float", "label": "Low Trade Price", "desc": "Lowest trade price in the minute"}),
        ("LastTradePrice", {"name": "close", "dtype": "float", "label": "Last Trade Price", "desc": "Price of the last trade in the minute"}),
        ("VolumeWeightPrice", {"name": "volumeweightprice", "dtype": "float", "label": "VWAP", "desc": "Dollar-volume weighted average price in the minute"}),
        ("Volume", {"name": "volume", "dtype": "int", "label": "Volume", "desc": "Shares traded in the minute"}),
        ("TotalTrades", {"name": "totaltrades", "dtype": "int", "label": "Total Trades", "desc": "Number of trades in the minute"}),

        # Adjusted fields (backward adjusted for corporate events)
        ("FirstTradePriceAdjusted", {"name": "firsttradepriceadjusted", "dtype": "float", "label": "Adj First Trade Price", "desc": "Backward adjusted"}),
        ("HighTradePriceAdjusted", {"name": "hightradepriceadjusted", "dtype": "float", "label": "Adj High Trade Price", "desc": "Backward adjusted"}),
        ("LowTradePriceAdjusted", {"name": "lowtradepriceadjusted", "dtype": "float", "label": "Adj Low Trade Price", "desc": "Backward adjusted"}),
        ("LastTradePriceAdjusted", {"name": "lasttradepriceadjusted", "dtype": "float", "label": "Adj Last Trade Price", "desc": "Backward adjusted"}),
        ("VolumeWeightPriceAdjusted", {"name": "vwap", "dtype": "float", "label": "Adj VWAP", "desc": "Backward adjusted"}),
        ("VolumeAdjusted", {"name": "volumeadjusted", "dtype": "int", "label": "Adj Volume", "desc": "Backward adjusted"})
    ])

    def __init__(self, trades_dir: str, security_master_path: str, is_demo: bool, *args, **kwargs):
        self.trades_dir = trades_dir
        self.security_master_node = AlgoSeekSecurityMaster(csv_path=security_master_path, is_demo=is_demo)
        self.is_demo = is_demo
        super().__init__(*args, **kwargs)

    # ---- Node wiring ----
    def dependencies(self) -> Dict[str, "DataNode"]:
        return {"security_master_node": self.security_master_node}

    def get_table_metadata(self) -> Optional[msc.TableMetaData]:
        identifier = "algoseek_1min_trades_adjusted"
        if self.is_demo:
            identifier += "_DEMO"
        return msc.TableMetaData(
            identifier=identifier,
            data_frequency_id=msc.DataFrequency.one_min,
            description="Algoseek Trade Only Adjusted Minute Bars (SecId gz files) remapped to FIGI; "
                        "time_index is Date + TimeBarStart + 1m in ET, converted to UTC.",
        )

    def get_column_metadata(self) -> List[ColumnMetaData]:
        # exclude Date/TimeBarStart because they feed time_index, not columns
        metas = []
        for src, spec in self.TRADE_FIELDS.items():
            metas.append(ColumnMetaData(
                column_name=spec["name"],
                dtype=spec["dtype"],
                label=spec["label"],
                description=spec["desc"],
            ))
        return metas

    # ---- Asset universe (SecId -> FIGIs) ----
    def get_asset_list(self) -> List[msc.Asset]:
        master_list = self.security_master_node.get_df_between_dates()
        all_sec_ids = []
        for df_path in self._iter_minute_gz_files():
            secid =self._secid_from_path(df_path)
            all_sec_ids.append(secid)

        # this will repeat all the figis with the same secid so we have duplicated data per figi, this is better as we can query per figi the full story taht corresponds to the sec id
        available_assets = master_list[master_list.secid.isin(all_sec_ids)].index.get_level_values(
            "unique_identifier").to_list()
        self.security_master_df = master_list
        asset_list = _get_or_register_assets_from_figis(figis=available_assets)

        self.sec_id_to_figi_map = (
            self.security_master_df
            .reset_index()
            .groupby("secid", sort=False)["unique_identifier"]
            .apply(lambda s: list(dict.fromkeys(s.dropna())))
            .to_dict()
        )

        return asset_list

    # ---- IO helpers ----
    def _iter_minute_gz_files(self) -> Iterable[str]:
        pattern = os.path.join(self.trades_dir, "*.csv.gz")
        for path in glob.iglob(pattern):
            yield path

    @staticmethod
    def _secid_from_path(path: str) -> Optional[str]:
        # file names like "18679.csv.gz" → "18679"
        name = os.path.basename(path)
        if not name.endswith(".csv.gz"):
            return None
        return name[:-7]  # strip ".csv.gz"

    # ---- Update ----
    def update(self) -> pd.DataFrame:
        """
        Read every `*.csv.gz` in trades_dir, replicate per FIGI (SecId→FIGIs),
        and emit 1‑minute FIGI-indexed bars.

        - `time_index` = UTC( (Date + TimeBarStart + 1 minute) in America/New_York ).
        - No datetime columns in output; `date` and `timebarstart` are consumed to build the index.
        - Incremental: if the platform specifies an asset batch and last times, we filter rows per asset.
        """
        us = self.update_statistics
        # quick lookup for per-asset incremental filtering
        target_assets = {a.unique_identifier: a for a in (us.asset_list or [])}

        frames: List[pd.DataFrame] = []

        for c,gz_path in enumerate(self._iter_minute_gz_files()):
            secid = self._secid_from_path(gz_path)
            if not secid:
                continue

            figis_for_file = self.sec_id_to_figi_map.get(secid, [])
            if not figis_for_file:
                raise Exception("No Sec id Found in Asset master list")
            # Decompress and read
            with gzip.open(gz_path, "rt") as fh:
                df = pd.read_csv(fh)

            if df is None or df.empty:
                raise Exception(f"Error on df in {secid}")

            # Build time_index = (Date + TimeBarStart + 1 minute) in NY → UTC
            local_naive = pd.to_datetime(
                df["Date"].astype(str) + df["TimeBarStart"].astype(str),
                format="%Y%m%d%H:%M",
                errors="coerce",
            ) + pd.Timedelta(minutes=1)
            local = local_naive.dt.tz_localize("America/New_York")
            df["time_index"] = local.dt.tz_convert("UTC")

            # Keep only declared fields + time_index; drop Date/TimeBarStart
            keep_src = list(self.TRADE_FIELDS.keys())
            keep_src += ["Date", "TimeBarStart"]  # present in df but will be dropped
            existing = [c for c in keep_src if c in df.columns]
            df = df[existing + ["time_index"]]

            # standardize names (lowercase ≤63 chars)
            df.columns = [str(c).lower() for c in df.columns]
            # ensure secid is present & string
            df["secid"] = df["secid"].astype(str)

            # consume date/timebarstart; they must not remain as columns in output
            df = df.drop(columns=[c for c in ["date", "timebarstart"] if c in df.columns])

            # Replicate per FIGI and optional per‑asset incremental filtering
            for figi in figis_for_file:
                tmp = df.copy()
                tmp["unique_identifier"] = figi

                if figi in target_assets:
                    last_t = us.get_asset_earliest_multiindex_update(asset=target_assets[figi])
                    if last_t is not None:
                        tmp = tmp[tmp["time_index"] > last_t]

                if not tmp.empty:
                    frames.append(tmp)

            if c%20==0:
                self.logger.info(f"{c} files processed")

        if not frames:
            return pd.DataFrame()



        out = pd.concat(frames, ignore_index=True)
        out = out.rename(
            columns={"hightradeprice": "high", "lowtradeprice": "low",
                   "firsttradeprice": "open", "lasttradeprice": "close","volumeweightpriceadjusted":"vwap"
                     })
        out.columns=[c.lower() for c in out.columns]
        out[["open", "close", "high", "low", "volume","vwap"]]

        out = out.set_index(["time_index", "unique_identifier"]).sort_index()
        return out

if __name__ == "__main__":
    import os
    US_EASTERN = pytz.timezone("US/Eastern")

    # === Adjust these paths for your machine ===
    BASE_DIR = os.path.expanduser("~/Downloads/demo_data")
    TRADES_DIR = os.path.join(BASE_DIR, "us-equity-1min-trades-adjusted-secid-20220101-20220630")
    DAILY_DIR = os.path.join(BASE_DIR, "us-equity-daily-ohlc-primary-secid-20220101-20220630")
    SECMASTER_CSV = os.path.join(BASE_DIR, "sp500_equity_security_master.csv")
    # node=AlgoSeekSecurityMaster(csv_path=SECMASTER_CSV,is_demo=True)
    # node.run(force_update=True)

    node=AlgoSeekDailyOHLCFIGI(daily_dir=DAILY_DIR,security_master_path=SECMASTER_CSV,is_demo=True)
    # node.get_last_observation()
    # node.run(update_tree=False,force_update=True)

    node=AlgoSeek1MinTradesAdjusted(trades_dir=TRADES_DIR,security_master_path=SECMASTER_CSV,is_demo=True)


    node.run(update_tree=False)
    a=5


