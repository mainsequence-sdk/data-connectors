# Interest Rates Submodule — Fixing Rates & Discount Curves

> **What this is:** A compact set of **registries** and **DataNodes** that build and store **daily fixing rates** and **discount (zero) curves** into Main Sequence tables with consistent schemas and update behavior.

---

## Quick answer

- **Two DataNodes**
  - `FixingRatesNode` → builds **daily fixings** (column: `rate`, **decimal**), MultiIndex `("time_index","unique_identifier")`, table: `FIXING_RATES_1D_TABLE_NAME`.
  - `DiscountCurves` → builds **daily discount/zero curves** (column: `curve`, **compressed string of a dict**), MultiIndex `("time_index","unique_identifier")`, table: `DISCOUNT_CURVES_TABLE_NAME`.

- **Registries drive the builds**
  - `FIXING_RATE_BUILD_REGISTRY` merges per‑provider maps (e.g., Banxico, FRED) keyed by **unique identifiers**.
  - `DISCOUNT_CURVE_BUILD_REGISTRY` merges per‑provider maps (e.g., Valmer, Banxico, Polygon) keyed by **unique identifiers** retrieved from `mainsequence.client.Constant`.

- **Schema & rules**
  - Outputs use **UTC** `time_index`, MultiIndex order **must be** `("time_index","unique_identifier")`, and **no datetime columns**; the base `DataNode` validates after `update()`. fileciteturn0file2

- **Curves are compressed for storage**
  - `curve` is a **Gzip‑compressed, Base64‑encoded JSON** of `{days_to_maturity: zero_rate}`.
  - Helpers `compress_curve_to_string` / `decompress_string_to_curve` are provided.

---

## Contents & layout

```
data_connectors/interest_rates/
├── nodes.py                            # DataNodes: DiscountCurves, FixingRatesNode
├── settings.py                         # Table names: FIXING_RATES_1D_TABLE_NAME, DISCOUNT_CURVES_TABLE_NAME
└── registries/
    ├── discount_curves.py              # DISCOUNT_CURVE_BUILD_REGISTRY (+ providers: Valmer, Banxico, Polygon)
    ├── fixing_rates.py                 # FIXING_RATE_BUILD_REGISTRY (+ providers: Banxico, FRED)
    └── __init__.py
```

---

## How the **registries** work

Both registry modules define a `_merge_unique(*maps)` helper that **merges source‑specific build maps** and raises on duplicate keys with different builders.

### Discount curves registry

- **File:** `registries/discount_curves.py`
- **Contract (builders)**  
  Each builder must match this signature and return a **MultiIndex** DataFrame:
  ```python
  def bootstrap_cmt_curve(
      update_statistics, 
      curve_unique_identifier: str, 
      base_node_curve_points: "APIDataNode"
  ) -> "pd.DataFrame":
      """
      Returns a DataFrame with:
      - Index: MultiIndex ("time_index","unique_identifier")
      - Column: "curve" → dict[days_to_maturity] -> zero_rate (percent)
      """
  ```
- **Keying**  
  Keys are looked up through `mainsequence.client.Constant.get_value(...)` so environments can **centrally control IDs** (e.g., `"ZERO_CURVE__VALMER_TIIE_28"`, `"ZERO_CURVE__BANXICO_M_BONOS_OTR"`, `"ZERO_CURVE__UST_CMT_ZERO_CURVE_UID"`).

- **Aggregate**  
  `_VALMER_CURVES | _BANXICO_CURVES | _POLYGON_CURVES` → `DISCOUNT_CURVE_BUILD_REGISTRY`.

### Fixing rates registry

- **File:** `registries/fixing_rates.py`
- **Contract (builders)**  
  Each builder must accept `(update_statistics, unique_identifier)` and return a **MultiIndex** DataFrame with **column `rate` (decimal)**.
- **Aggregate**  
  `TIIE_FIXING_BUILD_MAP | CETE_FIXING_BUILD_MAP | USD_FRED_FIXING_BUILD_MAP` → `FIXING_RATE_BUILD_REGISTRY`.

**Why registries?**  
They decouple **source‑specific logic** (per provider) from **platform integration**, keep **keys centralized**, and let the DataNodes call the right builder **just by the unique identifier**.

---

## The **DataNodes**

> Main Sequence **MUST** rules: implement `dependencies()` and `update()`, use `time_index` (UTC) as index (or `("time_index","unique_identifier")` MultiIndex), **no datetime columns**, lowercase ≤63‑char column names. The base `DataNode` validates and sanitizes after `update()` — you **should not** self‑validate inside `update()`. fileciteturn0file2

### `DiscountCurves` (daily zero/discount curves)

- **Config:**  
  ```python
  class CurveConfig(BaseModel):
      unique_identifier: str      # registry key, also asset ticker
      name: str                   # asset display name
      curve_points_dependecy_data_node_uid: Optional[str]  # optional dependency UID (APIDataNode)
  ```
- **Dependencies:**  
  If `curve_points_dependecy_data_node_uid` is set, it is resolved to an `APIDataNode` and returned from `dependencies()`.
- **Asset universe:**  
  `get_asset_list()` **registers/retrieves** a custom asset for the curve (`ticker = unique_identifier`) via `msc` before update (so the curve can be managed like any other asset). For general asset operations in Main Sequence, see the ms_client cookbook. fileciteturn0file1
- **Update flow:**
  1. Calls the builder from `DISCOUNT_CURVE_BUILD_REGISTRY[unique_identifier](...)`.
  2. **Compresses** the dict from `"curve"` into a `str` (Gzip+Base64 JSON).
  3. Uses `update_statistics.get_last_update_index_2d(unique_identifier)` to filter only **new dates**.
  4. Returns a **MultiIndex** DataFrame (`("time_index","unique_identifier")`) with a single column: `"curve"` (**str**).
- **Metadata:**  
  - Table: `DISCOUNT_CURVES_TABLE_NAME` (daily).
  - Columns: `curve` (`str`, “Compressed Discount Curve”).

> **Units:** Builder returns `{days_to_maturity: zero_rate}` where `zero_rate` is **percent** (e.g., `8.12` for 8.12%). The stored value is the **compressed dict**; consumers can decode and interpret units as needed.

### `FixingRatesNode` (daily fixings)

- **Config:**  
  ```python
  class RateConfig(BaseModel):
      unique_identifier: str
      name: str

  class FixingRateConfig(BaseModel):
      rates_config_list: List[RateConfig]
  ```
- **Asset universe:**  
  `get_asset_list()` registers/retrieves all the fixings as custom assets (`ticker = unique_identifier`).
- **Update flow:**
  1. Iterates `update_statistics.asset_list` (provided by the platform batch).
  2. For each asset, calls `FIXING_RATE_BUILD_REGISTRY[asset.unique_identifier](...)`.
  3. Concatenates results; asserts index names are exactly `["time_index","unique_identifier"]`.
  4. Keeps only `"rate"` (decimal) and drops NAs.
- **Metadata:**  
  - Table: `FIXING_RATES_1D_TABLE_NAME` (daily).
  - Columns: `rate` (`float`, “Fixing value normalized to decimal (percentage/100)”).

---

## Do it — Common tasks

### 1) Add a **new discount/zero curve** provider

1. **Create a Constant** (admin UI or migration) with a name like `ZERO_CURVE__MYPROVIDER_XYZ` whose **value** is the exact unique identifier string your system will use.
2. **Implement a builder** (match the contract):
   ```python
   # my_provider.py
   from mainsequence.tdag import APIDataNode
   import pandas as pd
   import pytz, datetime

   def bootstrap_my_curve(update_statistics, curve_unique_identifier: str, base_node_curve_points: APIDataNode) -> pd.DataFrame:
       # 1) Determine the date range you need (use update_statistics to be incremental)
       # 2) Fetch/compute curve points and bootstrap the zero curve per date
       # 3) Build: index = ("time_index","unique_identifier"), column "curve" (dict)
       rows = []
       for dt in [datetime.datetime(2024, 1, 2, tzinfo=pytz.UTC)]:  # example
           curve_dict = {1: 7.5, 7: 7.7, 30: 7.9}  # days -> zero rate (percent)
           rows.append({"time_index": dt, "unique_identifier": curve_unique_identifier, "curve": curve_dict})

       out = pd.DataFrame(rows).set_index(["time_index","unique_identifier"])
       return out
   ```
3. **Register it** in `registries/discount_curves.py`:
   ```python
   from mainsequence.client import Constant as _C
   from .my_provider import bootstrap_my_curve

   _MYPROVIDER_CURVES = {
       _C.get_value(name="ZERO_CURVE__MYPROVIDER_XYZ"): bootstrap_my_curve,
   }

   DISCOUNT_CURVE_BUILD_REGISTRY = _merge_unique(
       _VALMER_CURVES, _BANXICO_CURVES, _POLYGON_CURVES, _MYPROVIDER_CURVES
   )
   ```
4. **Instantiate the node**:
   ```python
   from data_connectors.interest_rates.nodes import DiscountCurves, CurveConfig

   cfg = CurveConfig(
       unique_identifier="ZERO_CURVE__MYPROVIDER_XYZ",   # use your registry key string
       name="MyProvider XYZ Zero Curve",
       curve_points_dependecy_data_node_uid=None,         # or a valid APIDataNode UID if you depend on curve points
   )
   node = DiscountCurves(curve_config=cfg)
   # The platform orchestrates .update(); in tests you can call node.update() with a prepared environment.
   ```

### 2) Add a **new fixing** provider

1. **Pick the unique ID** (or create a Constant).
2. **Implement a builder** returning a MultiIndex DataFrame with `"rate"` (decimal):
   ```python
   # my_fixings.py
   import pandas as pd
   import pytz, datetime

   def build_my_fixing(update_statistics, unique_identifier: str) -> pd.DataFrame:
       # Compute new dates only using update_statistics
       dt = datetime.datetime(2024, 1, 2, tzinfo=pytz.UTC)
       out = (pd.DataFrame(
           [{"time_index": dt, "unique_identifier": unique_identifier, "rate": 0.0775}]
       ).set_index(["time_index","unique_identifier"]))
       return out
   ```
3. **Register it** in `registries/fixing_rates.py`:
   ```python
   from .my_fixings import build_my_fixing

   _MY_FIXING_MAP = { "MY_FIXING_UID": build_my_fixing }
   FIXING_RATE_BUILD_REGISTRY = _merge_unique(
       TIIE_FIXING_BUILD_MAP, CETE_FIXING_BUILD_MAP, USD_FRED_FIXING_BUILD_MAP, _MY_FIXING_MAP
   )
   ```
4. **Instantiate the node**:
   ```python
   from data_connectors.interest_rates.nodes import FixingRatesNode, FixingRateConfig, RateConfig

   rates_cfg = FixingRateConfig(rates_config_list=[
       RateConfig(unique_identifier="MY_FIXING_UID", name="My Fixing (decimal)"),
   ])
   node = FixingRatesNode(rates_config=rates_cfg)
   ```

### 3) Decode a stored **discount curve**

```python
from data_connectors.interest_rates.nodes import decompress_string_to_curve

# Suppose df is the (MultiIndex) DataFrame read from the discount curves table
# and you want the curve for a specific (time_index, unique_identifier):
b64 = df.loc[(some_dt_utc, "ZERO_CURVE__VALMER_TIIE_28"), "curve"]
curve_dict = decompress_string_to_curve(b64)  # {days_to_maturity: zero_rate_percent}
```

> **Tip:** When writing any DataNode, follow the MUST/SHOULD rules and incremental update patterns using `UpdateStatistics` to compute **only the new slice**. fileciteturn0file2

---

## Notes

- **Main Sequence DataNode rules**: Implement `dependencies()` & `update()`, stick to index rules (`time_index` in UTC, `("time_index","unique_identifier")` for MultiIndex), no datetime columns, and let the base class sanitize. fileciteturn0file2
- **Assets**: This submodule **registers/retrieves custom assets** for each curve/fixing so they can be managed downstream. For general asset operations, see the `ms_client` cookbook (filtering, FIGI registration, categories). fileciteturn0file1

---

## Troubleshooting

- **`ValueError: Duplicate registry key with different builder`**  
  The same unique identifier appears in two maps with different functions. **Rename** your Constant or remove the duplicate.

- **`assert all_dfs.index.names==["time_index","unique_identifier"]` fails** (fixings)  
  Your builder returned wrong index names/order. Ensure exactly `("time_index","unique_identifier")`.

- **Empty updates**  
  Discount curves filter to dates **after the last stored date** for that specific `unique_identifier`. Make sure your builder emits **new `time_index` rows**.

- **Table identifiers should be strings**  
  In `settings.py`, set:
  ```python
  FIXING_RATES_1D_TABLE_NAME = "fixing_rates_1d"
  DISCOUNT_CURVES_TABLE_NAME = "discount_curves"
  ```
  (Avoid trailing commas that would create tuples.)

---

## File references

- DataNode MUST/SHOULD, schemas, and incremental patterns: **DataNodes — Authoring Guide**. fileciteturn0file2  
- General asset operations with `mainsequence.client`: **LLM Assistant Guide for `ms_client`**. fileciteturn0file1
