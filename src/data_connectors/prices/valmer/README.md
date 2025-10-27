# Valmer Pricing Submodule — TIIE Zero Curves, Time Series, and Instrument Build (Schedule Match)

> End-to-end ingestion and pricing for **Valmer** instruments:
> - **Zero curves** (TIIE-28) from public Valmer CSV
> - **Time series** (dirty/clean price, duration, etc.) from **bucket artifacts**
> - **Instrument build** with a **custom schedule** that matches vendor *“CUPONES X COBRAR”* and *“DIAS TRANSC. CPN”* exactly (used to price like the sheet)

---

## Quick answer

- **Files**
  - `utils.py` → `build_tiie_valmer(...)`: bootstrap a **zero curve** (dict: *days_to_maturity → zero_rate_decimal*) for a single **base date**.
  - `time_series.py`
    - `MexDerTIIE28Zero` (DataNode): stores **compressed zero curve** per evaluation date.
    - `ImportValmer` (DataNode): reads **Valmer bucket artifacts**, normalizes rows, registers assets, attaches **instrument pricing details**, and emits **OHLCV-like** time series + relevant columns.
  - `instrument_build.py` → the **heart**: creates a **QuantLib schedule that force-matches the sheet** (coupon count & days elapsed), then builds a **FixedRateBond/FloatingRateBond** you can price consistently with the vendor sheet.

- **DataNode contract**
  - MultiIndex output: `("time_index", "unique_identifier")`, UTC `time_index`, **no datetime columns**, lowercase columns (≤ 63 chars). The base class validates after `update()` — don’t self-sanitize. fileciteturn0file2

- **Instrument schedule matching**
  - We build an **explicit schedule** that **makes the model’s future coupon count equal the sheet value** and **sets the previous date to match DIAS TRANSC. CPN** (Actual/360) **on FECHA/settlement** according to switches explained below.
  - If the “natural” schedule has **too few** future payments, we **insert** 1‑day slices **just before maturity**. If it has **too many**, we **drop the last** ones (closest to maturity).

- **Assets & artifacts**
  - Assets are batch‑created via `ms_client.Asset.batch_get_or_register_custom_assets(...)` and enriched with **instrument pricing details** created from our builder. Artifacts are listed/created with `ms_client.Artifact`. fileciteturn0file1

---

## Layout

```
prices/valmer/
├── utils.py                 # Zero curve bootstrap from public CSV
├── time_series.py           # DataNodes: MexDerTIIE28Zero, ImportValmer
├── instrument_build.py      # Schedule forcing + instrument construction + price checks
└── __init__.py              # Constants bootstrap (e.g., ZERO_CURVE__VALMER_TIIE_28)
```

---

## Data flow (overview)

1) **Zero curve** (public CSV → curve per base date)
   - `build_tiie_valmer` fetches Valmer CSV (`MEXDERSWAP_IRSTIIEPR.csv`), normalizes rates to **decimals** (divides by 100), produces a single **base date** (`time_index = asof - 1 day`) with a dictionary column: `{days_to_maturity → zero_rate_decimal}`.
   - `MexDerTIIE28Zero.update()` calls the same CSV feed, builds the curve dict, then **compresses** it (JSON → gzip → base64) for storage.

2) **Bucket artifacts → Price time series**
   - `ImportValmer` loads all **new** artifacts in a bucket (CSV/XLS), **normalizes column names** (alphanumeric lower‑case), creates `unique_identifier = "{tipovalor}_{emisora}_{serie}"` and selects relevant instruments (TIIE, CETE, Bonos M - MPS).
   - It **registers assets in batch** and, when needed, **attaches instrument pricing details** (built from **our schedule‑matching instrument**). Then it emits **OHLCV** with extra pricing columns.

3) **Instrument build (matching the sheet)**
   - `instrument_build.py` constructs a **QuantLib schedule** that matches the sheet’s **coupon count** and **dias transcurridos** consistently **under your chosen counting rule** (see switches below), and then builds a **Fixed/Floating Rate bond** from the row’s fields + conventions.

---

## What each file does

### `utils.py` — TIIE(28) Valmer zero curve

```python
def build_tiie_valmer(update_statistics, curve_unique_identifier: str, base_node_curve_points=None) -> pd.DataFrame:
    # Fetch CSV → parse → base_dt = first asof - 1 day
    # Compute days_to_maturity, normalize zero_rate to DECIMAL (value / 100)
    # Return MultiIndex ("time_index","unique_identifier") with column "curve" as dict[days_to_maturity] -> zero_rate_decimal
```
**Key details**
- **Units:** Valmer CSV provides **percent**; builder **divides by 100** → decimals.
- **Single base date:** it emits **one** `time_index = base_dt` row (where `base_dt = asof - 1 day`).
- **Incremental guard:** early‑exit if `update_statistics.asset_time_statistics[curve_uid] >= base_dt` (nothing new).

> When used by a higher‑level DataNode (e.g., `DiscountCurves` elsewhere), the returned dict is typically **compressed** before storage. In this submodule, see `MexDerTIIE28Zero`.

---

### `time_series.py` — DataNodes

#### `MexDerTIIE28Zero`
- **Purpose:** Store daily **compressed** TIIE‑28 zero curves.
- **Asset universe:** single asset `"TIIE_28"` (looked up via `msc.Asset.get`).
- **Update:** same CSV as above, **zero_rate → decimal**, group into dict `{days_to_maturity → zero_rate}`, **compress** to base64(gzip(json)), **MultiIndex** output.
- **Table metadata:** `identifier="valmer_mexder_tiie28_zero_curve"`, frequency = **1D**.

#### `ImportValmer`
- **Inputs:** bucket name (env override: `DEBUG_ARTIFACT_PATH` to read local files instead).
- **What it reads:** `.csv` / `.xls(x)`; **normalizes** column names (lowercase, alnum) via `normalize_column_name`.
- **Asset registration:** Builds a list of **unique identifiers**; fetches existing assets in bulk; registers missing ones with `Asset.batch_get_or_register_custom_assets`. fileciteturn0file1
- **Instrument details attachment:** For selected bonds (TIIE/CETE floaters and **Bonos M** fixed, MPS currency) we:
  1) Compute **conventions** via `get_instrument_conventions(row)`.
  2) Build an instrument with **explicit schedule** using `build_qll_bond_from_row(...)`.
  3) Call `asset.add_instrument_pricing_details_from_ms_instrument(instrument=..., pricing_details_date=row["fecha"])`.

- **Output columns (excerpt):**
  - OHLCV: `open=high=low=close=preciosucio`, `volume=0`.  
  - Pricing extras: `preciolimpio`, `duracion`, `tasaderendimiento`, `cuponactual`, `sobretasa`, `valornominal`, `reglacupon`, `freccpn`, `cuponesxcobrar`, etc.
  - Snapshot-ish extras: `calificacionfitch`, `fechavcto` (epoch), `monedaemision`, `sector`, `nombrecompleto`.
  - **Index:** `("time_index", "unique_identifier")` in UTC. **No datetime columns** outside index. fileciteturn0file2
- **Table metadata:** `identifier="vector_de_precios_valmer"`, frequency = **1M** (monthly).

> **DataNode guardrails:** Always implement `dependencies()` and `update()`, honor index rules, and let the base class sanitize after `update()`. fileciteturn0file2

---

## `instrument_build.py` — **How schedule matching works** (EXACT match to the sheet)

### The problem we solve
Vendor sheets include two key fields per bond on **FECHA** (= evaluation date):
- **`CUPONES X COBRAR`** — how many future coupon payments remain according to the vendor.
- **`DIAS TRANSC. CPN`** — elapsed days in the **current accrual period** as measured by the vendor’s convention.

A “natural” QuantLib schedule (frequency + BDC) often **does not** reproduce those two numbers **exactly** due to:
- **Settlement vs FECHA** counting boundary,
- Inclusion/exclusion of flows **ON** the boundary date,
- Business day adjustments that can **stick** a date on the same day when subtracting frequency,
- Vendor‑specific tweaks (esp. near maturity).

### Configuration toggles
At the top of the module:

```python
COUNT_FROM_SETTLEMENT      = True   # reference is settlementDate() (T+N), not FECHA
INCLUDE_REF_DATE_EVENTS    = False  # flows on the reference date are treated as 'past'
```
- With these defaults, the **coupon count** comes from **settlement**, and a payment **on** settlement **does not** count as future.
- You can flip either toggle to align with a different vendor logic (e.g., count vs FECHA and/or include flows on FECHA).

### Step-by-step: `compute_sheet_schedule_force_match(...)`

We **construct an explicit schedule** (a `ql.Schedule`) whose dates ensure the **model** equals the **sheet** for both counts.

1) **Parse row fields**  
   - `FECHA` (evaluation date), `FECHA VCTO` (maturity), `FREC. CPN` (e.g., “28Dias”), `CUPONES X COBRAR`, `DIAS TRANSC. CPN`.
   - Normalize date strings/ints robustly (`parse_val_date`, `parse_iso_date`).
2) **Boundary for counting**  
   - If `COUNT_FROM_SETTLEMENT`, set boundary = **settlement date** (= FECHA advanced by `settlement_days`, adjusted by BDC). Else boundary = **FECHA**.
   - If *include boundary*, keep payments **>= boundary**; else **> boundary**.
3) **Natural future dates** (walking **backward** from maturity)
   - Repeatedly subtract `freq_days`, adjust via BDC.
   - If adjustment **does not strictly move back**, nudge using `Preceding` and possibly day‑by‑day until strictly earlier.
   - Collect dates **≥ boundary**.
4) **Force coupon count to sheet**  
   - Let `N_nat = len(future_dates)`.
   - If `N_nat < N_sheet`, **insert K = N_sheet − N_nat** small **1‑day slices just before maturity** (in the window `[boundary, maturity‑1d]`). This minimally alters price while matching count.
   - If `N_nat > N_sheet`, **drop the last** dates (closest to maturity).  
   Result: exactly `N_sheet` future payment dates.
5) **Force DIAS TRANSC. CPN** (previous date)  
   - If the sheet provides `DIAS TRANSC. CPN`, set **previous** = `FECHA − dias_trans` (and ensure it is **strictly before** the first future).
   - Else infer “previous” as one frequency back from the first future (adjusted).
6) **Build the final schedule**  
   - The schedule **starts with the previous date**, then lists the **N future payment dates** (including maturity).

**Visual intuition** (not to scale):
```
[ previous ] ——{dias_trans}—— [ boundary/FECHA ] ———— … ———— [ inserted slices ] [ maturity ]
                                 ↑ count from here (>= or > depending on config)
```
Because **QuantLib** cashflow generation depends on schedule **and** day count, this method guarantees that:
- `count_future_coupons(...)` under your chosen refDate/inclusion rule equals the sheet’s `CUPONES X COBRAR`,
- `dayCount(previous, FECHA)` equals `DIAS TRANSC. CPN` (with `Actual/360` in MX).

> If you ever need **explanations/auto-fix**, use `assert_schedule_matches_sheet_debug(...)` which prints a **forensic table** and can **snap** the first future date forward to satisfy the chosen boundary rule.

### Building the instrument

`build_qll_bond_from_row(row, calendar, dc, bdc, settlement_days, SPREAD_IS_PERCENT=True)`:
- Builds the **explicit schedule** via `compute_sheet_schedule_force_match`.
- Chooses **Fixed** vs **Floating** by `reglacupon`:
  - `"Tasa Fija"` → `msi.FixedRateBond(...)` with `coupon_rate = tasacupon / 100`.
  - Else → `msi.FloatingRateBond(...)` with `spread = sobretasa / 100` (if percent), and `floating_rate_index_name` mapped from `SUBYACENTE` (e.g., `"TIIE28" → Constant("REFERENCE_RATE__TIIE_28")`).
- Applies the same **calendar, BDC, settlement days, day count**; sets valuation date to `FECHA`.
- Returns a model object exposing QuantLib internals and `analytics(with_yield=tasaderendimiento/100)` for price checks.

### Conventions
`get_instrument_conventions(row)` returns `(calendar, business_day_convention, settlement_days, day_count)` for MX instruments:
- **MPS** (MXN): `ql.Mexico(ql.Mexico.BMV)`, `Following`, `settlement_days=1`, `Actual/360` for **Bonos M** / **CETE** and **TIIE**‑based floaters.
- Raise for unsupported currency/underlying so you notice gaps early.

---

## End-to-end: examples

### A) Build & verify an instrument from one **sheet row**

```python
import pandas as pd
import QuantLib as ql
from src.data_connectors.prices.valmer.instrument_build import (
    compute_sheet_schedule_force_match,
    build_qll_bond_from_row,
    get_instrument_conventions,
    count_future_coupons,
)

row = pd.Series({
    "fecha":        "2024-09-03",
    "fechavcto":    "2026-03-15",
    "freccpn":      "28Dias",
    "cuponesxcobrar": 15,
    "diastransccpn": 13,
    "reglacupon":   "TIIE28",
    "tasacupon":    10.5,     # only used for fixed
    "sobretasa":    75.0,     # bps if SPREAD_IS_PERCENT=True
    "valornominalactualizado": 100.0,
    "subyacente":   "TIIE28",
    "monedaemision":"MPS",
    "fechaemision": "2023-01-15",
    "preciosucio":  100.25,
    "tasaderendimiento": 10.12,
})

cal, bdc, sett, dc = get_instrument_conventions(row)
bond = build_qll_bond_from_row(row, calendar=cal, dc=dc, bdc=bdc, settlement_days=sett)

# Check coupon count vs the sheet under your configured rule
n_future = count_future_coupons(bond.get_ql_bond())
print("Future coupons (model) =", n_future)
print("Sheet CUPONES X COBRAR =", row["cuponesxcobrar"])
```

### B) Price‑check a filtered sheet subset

```python
from src.data_connectors.prices.valmer.instrument_build import run_price_check, normalize_column_name
import pandas as pd

df = pd.read_excel("vendor_sheet.xlsx")
df.columns = [normalize_column_name(c) for c in df.columns]

# Focus: TIIE/CETE floaters + Bonos M
floating_tiie  = df[df["subyacente"].astype(str).str.contains("TIIE", na=False)]
floating_cetes = df[df["subyacente"].astype(str).str.contains("CETE", na=False)]
bonos_m        = df[df["subyacente"].astype(str).str.contains("Bonos M", na=False)]
bonos_m        = bonos_m[bonos_m.monedaemision == "MPS"]

subset = pd.concat([floating_tiie, floating_cetes, bonos_m], ignore_index=True)
df_out, instrument_map = run_price_check(subset, price_tol_bp=2.0)
print(df_out.head())
```

### C) Ingest artifacts and emit time series (**DataNode**)

```python
from src.data_connectors.prices.valmer.time_series import ImportValmer
node = ImportValmer(bucket_name="ValmerRawArtifacts")
df = node._get_artifact_data()     # load & normalize source rows
assets = node.get_asset_list()     # register assets + attach instrument details
out = node.update()                # OHLCV + extras, MultiIndex ("time_index","unique_identifier")
print(out.head())
```

### D) Build & store the **TIIE‑28** zero curve (compressed)

```python
from src.data_connectors.prices.valmer.time_series import MexDerTIIE28Zero
zc = MexDerTIIE28Zero()
curve_df = zc.update()  # MultiIndex with column "curve" (base64(gzip(JSON)))
print(curve_df.head())
```

---

## Column normalization & required fields

**Normalization:** `normalize_column_name(col)` → lowercase and strip non‑alphanumeric (`A-Za-z0-9`) characters.

**Required fields** (typical sheet/Valmer names, shown **after** normalization):
- Identification: `tipovalor`, `emisora`, `serie` → forms `unique_identifier = "{tipovalor}_{emisora}_{serie}"`
- Dates: `fecha` (YYYYMMDD or ISO), `fechavcto`, (optional: `fechaemision`)
- Conventions: `freccpn`, `reglacupon`, `monedaemision`, `subyacente`
- Pricing: `preciosucio`, `preciolimpio`, `tasaderendimiento`, `duracion`, `cuponactual`, `sobretasa`, `valornominal`, `valornominalactualizado`
- Matching helpers: `cuponesxcobrar`, `diastransccpn`

---

## Units & conventions

- **Rates in sheet (percent)** → the builder converts to **decimal** when needed (e.g., `sobretasa / 100`, `tasacupon / 100`, zero rates `value / 100`).
- **Day count:** `Actual/360` for MX bonds (as selected in `get_instrument_conventions`).
- **Counting boundary:** per **toggles** (`COUNT_FROM_SETTLEMENT`, `INCLUDE_REF_DATE_EVENTS`).
- **Schedule vector:** **first element** is the **previous accrual date**, followed by **future payment dates** (including maturity).

---

## Implementation guardrails & platform notes

- **DataNodes** must implement `dependencies()` and `update()`, output either single‑index or MultiIndex with **first two levels** `("time_index","unique_identifier")`, **no datetime columns**, lowercase column names. The base node will **validate/sanitize** outputs. fileciteturn0file2
- **Assets & artifacts** are handled through `mainsequence.client`:
  - Batch asset registration: `Asset.batch_get_or_register_custom_assets(...)`
  - Artifact filtering/creation for CSV/Excel handling. fileciteturn0file1

---

## Environment variables & performance

- `DEBUG_ARTIFACT_PATH` → when set, `ImportValmer` reads local `*.xls*` files from that folder (recursively) instead of querying the bucket.
- `VALMER_PER_PAGE` (default `5000`) → batch size for asset fetch/registration.
- `PROJECT_BUCKET_NAME` (used by `build_position_from_sheet`) → target bucket for artifacts (e.g., price check CSV).

---

## Troubleshooting

- **Coupon count mismatch vs sheet**
  - Re‑check toggles (**settlement vs FECHA**, include ref date or not).
  - Use `assert_schedule_matches_sheet_debug(...)` to print a diagnostic; set `auto_fix=True` to **snap** the first future date if it lies on the wrong side of your boundary.

- **Zero curve missing for a date**
  - The zero curve builder emits **one** row at `base_dt = asof - 1 day`. If `update_statistics` says the asset is already updated **≥ base_dt**, it returns **empty** by design.

- **Wrong index or datetime columns**
  - Ensure `("time_index","unique_identifier")` ordering and move any datetimes from columns into the **index**. The platform enforces these rules. fileciteturn0file2

- **Assets not updated with instrument details**
  - We only attach details for **TIIE/CETE floaters and Bonos M (MPS)** and **only when** the existing detail is missing/invalid or the **face value changed**. Confirm your row filters and `valornominalactualizado` are correct. Asset operations use the `ms_client` cookbook. fileciteturn0file1

---

## Appendix — Helper APIs (pointers)

- `count_future_coupons(bond, from_settlement=True, include_ref_date_events=False)` → counts coupons as QL does under your rule.
- `extract_future_cashflows(built, ...)` → returns future floating and redemption legs as plain dicts.
- `run_price_check(bonos_df, price_tol_bp=2.0)` → builds each instrument, gets analytics with the sheet yield, and compares **dirty/clean**, **running coupon**, and **coupon count**. Returns a DataFrame plus an `instrument_map`.

---

**References**
- **DataNodes Authoring Guide** — rules for indices/columns, `UpdateStatistics`, patterns, and template. fileciteturn0file2  
- **ms_client Cookbook** — assets, artifacts, querying/registration patterns used by this module. fileciteturn0file1
