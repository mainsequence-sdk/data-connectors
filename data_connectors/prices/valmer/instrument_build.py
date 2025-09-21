# -*- coding: utf-8 -*-
import datetime as dt
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple

import numpy as np
import pandas as pd
import QuantLib as ql
import re
from tqdm import tqdm
from contextlib import contextmanager

from mainsequence.instruments.instruments import PositionLine, Position
from mainsequence.instruments.pricing_models.indices import build_zero_curve
from mainsequence.instruments.instruments.floating_rate_bond import FloatingRateBond
from mainsequence.instruments.settings import (TIIE_28_UID,TIIE_91_UID,TIIE_182_UID,TIIE_OVERNIGHT_UID,CETE_28_UID,
CETE_182_UID
                          )
import mainsequence.client as msc
import pytz
import json
from pathlib import Path

# =============================================================================
# Configuration toggles for “coupon count” to match the sheet convention
# =============================================================================
COUNT_FROM_SETTLEMENT: bool = True     # vendor sheets often count from settlement (T+1/T+2)
INCLUDE_REF_DATE_EVENTS: bool = False  # treat flows ON ref date as already occurred (QL default)

SUBYACENTE_TO_INDEX_MAP = {"TIIE28": TIIE_28_UID,
                           "TIIE182": TIIE_182_UID,
                           "TIIE91": TIIE_91_UID,
                           "TIIE28 EQUIV 182": TIIE_182_UID,
                           "Tasa TIIE Fondeo 1D": TIIE_OVERNIGHT_UID,
                           "CETE_28": CETE_28_UID,
                           "CETE28": CETE_28_UID,
                           "CETE182": CETE_182_UID,
                           }

# =============================================================================
# Small dataclass for the built instrument
# =============================================================================
@dataclass
class BuiltBond:
    row_ix: int
    emisora: str
    serie: str
    bond: FloatingRateBond          # your model
    eval_date: dt.date


# =============================================================================
# Basic date & schedule helpers
# =============================================================================
def qld(d: dt.date) -> ql.Date:
    return ql.Date(d.day, d.month, d.year)

def pyd(d: ql.Date) -> dt.date:
    return dt.date(d.year(), int(d.month()), d.dayOfMonth())

def parse_val_date(v) -> dt.date:
    """Handle integer 20240903, '2024-09-03', pandas Timestamp, etc."""
    if pd.isna(v):
        raise ValueError("FECHA is required")
    s = str(int(v)) if isinstance(v, (int, np.integer)) else str(v)
    try:
        if len(s) == 8 and s.isdigit():
            return dt.date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        return pd.to_datetime(s).date()
    except Exception:
        return pd.to_datetime(v).date()

def parse_iso_date(v) -> dt.date:
    if pd.isna(v):
        raise ValueError("Missing date")
    return pd.to_datetime(v).date()

def parse_coupon_period(freq_val, default_days: int = 28) -> ql.Period:
    """
    Parse strings like: '28Dias', '30 dias', '91DÍAS', '184Dias'. Fallback to default.
    """
    if pd.isna(freq_val):
        return ql.Period(default_days, ql.Days)
    s = str(freq_val).strip().lower()
    m = re.search(r"(\d+)", s)
    days = int(m.group(1)) if m else default_days
    days = days if days > 0 else default_days
    return ql.Period(days, ql.Days)

def parse_coupon_days(freq_val, ) -> int:
    """Integer version (days)."""

    m = re.search(r"(\d+)", str(freq_val).lower())
    return int(m.group(1))

def sch_len(s: ql.Schedule) -> int:
    try:
        return int(s.size())
    except Exception:
        try:
            return len(list(s.dates()))
        except Exception:
            i = 0
            while True:
                try:
                    _ = s.date(i)
                    i += 1
                except Exception:
                    break
            return i

def sch_date(s: ql.Schedule, i: int) -> ql.Date:
    try:
        return s.date(i)
    except Exception:
        return list(s.dates())[i]

def sch_dates(s: ql.Schedule) -> List[ql.Date]:
    try:
        return list(s.dates())
    except Exception:
        return [sch_date(s, j) for j in range(sch_len(s))]

@contextmanager
def ql_include_ref_events(include: bool):
    """Temporarily set Settings.includeReferenceDateEvents, then restore."""
    s = ql.Settings.instance()
    prev = s.includeReferenceDateEvents
    s.includeReferenceDateEvents = include
    try:
        yield
    finally:
        s.includeReferenceDateEvents = prev


# =============================================================================
# Build a schedule that *forces* the sheet's coupon count to match
# =============================================================================
def compute_sheet_schedule_force_match(
    row: pd.Series,
    *,
    calendar: ql.Calendar = ql.Mexico(),
    bdc: int = ql.Following,
    default_freq_days: int = 28,
    adjust_maturity_date: bool = False,
    # vendor sometimes uses these alt columns
    settlement_days: int = 2,
    count_from_settlement: bool = True,
    include_boundary_for_count: bool = True,  # vendor counts the on‑settlement payment
    dc: ql.DayCounter = ql.Actual360(),       # to force DIAS TRANSC. CPN
) -> ql.Schedule:
    """
    Build an explicit schedule that:
      1) Matches the vendor's CUPONES X COBRAR exactly,
      2) Matches DIAS TRANSC. CPN exactly (vs FECHA),
      3) If natural future coupons < sheet N, inserts missing dates as 1‑day
         slices just *before maturity* (minimal price impact),
      4) If natural future coupons > sheet N, removes the *last* coupons
         closest to maturity (minimal price impact).

    Counting convention for (1): from settlement (T+settlement_days) if
    count_from_settlement=True, and include_boundary_for_count=True means that
    a payment on settlement is counted as "to collect".
    """
    # ---- helpers -------------------------------------------------------------
    def _adjust(d: dt.date, convention: int = bdc) -> dt.date:
        return pyd(calendar.adjust(qld(d), convention))

    def _strictly_before(a: dt.date, b: dt.date) -> bool:
        return a < b

    # ---- inputs --------------------------------------------------------------
    eval_date    = parse_val_date(row["fecha"])
    maturity_raw = parse_iso_date(row["fechavcto"])

    # frequency in days (28, 30, 91, ...)
    freq_days = parse_coupon_days(row["freccpn"])

    # counting boundary (vendor: settlement, including on-ref)
    if count_from_settlement:
        boundary = pyd(calendar.advance(qld(eval_date), settlement_days, ql.Days, bdc))
    else:
        boundary = eval_date
    cmp_keep = (lambda d: d >= boundary) if include_boundary_for_count else (lambda d: d > boundary)

    maturity_pay = _adjust(maturity_raw) if adjust_maturity_date else maturity_raw
    coupons_left = int(row["cuponesxcobrar"]) if "cuponesxcobrar" in row and pd.notna(row["cuponesxcobrar"]) else None
    dias_trans   = int(row["diastransccpn"]) if pd.notna(row.get("diastransccpn")) else None

    # Case A: sheet says no coupons left => return redemption-only schedule
    if coupons_left is not None and coupons_left <= 0:
        dv = ql.DateVector()
        dv.push_back(qld(maturity_pay))
        return ql.Schedule(dv, calendar, bdc)

    # ---- step back from maturity to get the "natural" future dates -----------
    # Collect all payment dates >= boundary by walking backwards with 'freq_days'.
    nat_desc: List[dt.date] = [maturity_pay]
    d = maturity_pay
    while True:
        prev_unadj = d - dt.timedelta(days=freq_days)
        prev_adj = _adjust(prev_unadj)
        # if adjustment doesn't move it strictly back, nudge with Preceding and day-by-day
        if not _strictly_before(prev_adj, d):
            prev_adj = _adjust(prev_unadj, ql.Preceding)
            while not _strictly_before(prev_adj, d):
                prev_unadj -= dt.timedelta(days=1)
                prev_adj = _adjust(prev_unadj, ql.Preceding)

        if prev_adj < boundary:
            break
        nat_desc.append(prev_adj)
        d = prev_adj

    future_dates = sorted(set(nat_desc))  # ascending, unique
    natural_cnt = len(future_dates)

    # If sheet didn't give the count, we can return a schedule based on natural dates.
    if coupons_left is None:
        # previous pay for the current period:
        if dias_trans is None:
            # generic: one full freq before first future
            prev_unadj = future_dates[0] - dt.timedelta(days=freq_days)
            prev_pay = _adjust(prev_unadj)
            if not _strictly_before(prev_pay, future_dates[0]):
                prev_pay = pyd(calendar.advance(qld(future_dates[0]), -1, ql.Days, ql.Preceding))
        else:
            # force DIAS TRANSC. CPN vs FECHA
            prev_pay = eval_date - dt.timedelta(days=int(dias_trans))
            # keep strictly before first future
            if not _strictly_before(prev_pay, future_dates[0]):
                prev_pay = future_dates[0] - dt.timedelta(days=1)

        dv = ql.DateVector()
        dv.push_back(qld(prev_pay))
        for x in future_dates:
            dv.push_back(qld(x))
        return ql.Schedule(dv, calendar, bdc)

    # ---- Force the count to EXACTLY match the sheet --------------------------
    N = int(coupons_left)

    if natural_cnt < N:
        # Need to ADD K missing dates with minimal impact.
        # Insert them just before maturity, spaced 1 day apart:
        K = N - natural_cnt
        # Work backward from maturity by 1,2,... days; avoid duplicates.
        existing = set(future_dates)
        extra: List[dt.date] = []
        day_offset = 1
        while len(extra) < K:
            cand = maturity_pay - dt.timedelta(days=day_offset)
            # Don’t go before boundary (otherwise not counted). If that happens, push closer to maturity.
            if cand < boundary:
                # If boundary is too close to maturity and we still need extras,
                # keep stacking more dates between (boundary, maturity) by shrinking the gap (still 1-day apart).
                cand = boundary if include_boundary_for_count else (boundary + dt.timedelta(days=1))
            if cand not in existing and cand not in extra and cand < maturity_pay:
                extra.append(cand)
            day_offset += 1
        future_dates = sorted(set(future_dates + extra))  # now count == at least N
        # If over-shot due to boundary/dupe handling, trim from the far end (closest to maturity)
        if len(future_dates) > N:
            future_dates = future_dates[:N]

    elif natural_cnt > N:
        # Need to DROP extra dates with minimal impact -> drop the last ones (closest to maturity).
        future_dates = future_dates[:N]

    # ---- Force DIAS TRANSC. CPN by setting the previous date ----------------
    if dias_trans is None:
        prev_unadj = future_dates[0] - dt.timedelta(days=freq_days)
        prev_pay = _adjust(prev_unadj)
        if not _strictly_before(prev_pay, future_dates[0]):
            prev_pay = pyd(calendar.advance(qld(future_dates[0]), -1, ql.Days, ql.Preceding))
    else:
        # Make dayCount(prev_pay, FECHA) == dias_trans (Actual/360 returns actual days)
        prev_pay = eval_date - dt.timedelta(days=int(dias_trans))
        if not _strictly_before(prev_pay, future_dates[0]):
            # Keep strictly increasing schedule; if clash, move previous back.
            prev_pay = future_dates[0] - dt.timedelta(days=1)

    # ---- Build final schedule (previous + N future dates) --------------------
    dv = ql.DateVector()
    dv.push_back(qld(prev_pay))
    for x in future_dates:
        dv.push_back(qld(x))
    return ql.Schedule(dv, calendar, bdc)





def _count_future_coupons(b: ql.Bond, ref_py: dt.date, include_ref: bool) -> int:
    n = 0
    with ql_include_ref_events(include_ref):
        ref = qld(ref_py)
        for cf in b.cashflows():
            if ql.as_floating_rate_coupon(cf) is None:
                continue
            if not cf.hasOccurred(ref):    # 1-arg overload only
                n += 1
    return n

def _flow_table(b: ql.Bond, *, eval_date: dt.date, settle_date: dt.date) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for cf in b.cashflows():
        is_cpn = ql.as_floating_rate_coupon(cf) is not None
        pay = pyd(cf.date())
        status_eval = "future" if pay > eval_date else ("on_ref" if pay == eval_date else "past")
        status_sett = "future" if pay > settle_date else ("on_ref" if pay == settle_date else "past")
        rec: Dict[str, Any] = {
            "type": "coupon" if is_cpn else "redemption",
            "pay": pay,
            "Δdays_FECHA": (pay - eval_date).days,
            "Δdays_settle": (pay - settle_date).days,
            "vs_FECHA": status_eval,
            "vs_settle": status_sett,
        }
        cpn = ql.as_floating_rate_coupon(cf)
        if cpn:
            rec.update({
                "accrual_start": pyd(cpn.accrualStartDate()),
                "accrual_end": pyd(cpn.accrualEndDate()),
                "fixing": pyd(cpn.fixingDate()),
                "accrual_days": int(cpn.accrualEndDate() - cpn.accrualStartDate()),
            })
        rows.append(rec)
    return pd.DataFrame(rows).sort_values(["pay", "type"]).reset_index(drop=True)


# =============================================================================
# Debug assertion with forensic report + optional auto-fix
# =============================================================================
def _snap_first_future(schedule: ql.Schedule,
                       *,
                       calendar: ql.Calendar,
                       bdc: int,
                       min_first_date: dt.date,
                       allow_equal: bool) -> ql.Schedule:
    """
    Keep maturity anchored and coupon count unchanged, but ensure the first *future*
    date is >= (or >) min_first_date. (Creates a longer front stub if needed.)
    """
    dts = [pyd(d) for d in sch_dates(schedule)]
    if len(dts) <= 1:
        return schedule  # redemption-only
    prev = dts[0]
    first = dts[1]
    second = dts[2] if len(dts) >= 3 else None

    # Compute the minimum allowed first date
    snap = pyd(calendar.adjust(qld(min_first_date), bdc))
    if not allow_equal:
        if snap <= min_first_date:
            snap = pyd(calendar.advance(qld(min_first_date), 1, ql.Days, bdc))

    # If the original first is already compliant, keep it
    new_first = first
    if (first < snap) or (not allow_equal and first == snap):
        new_first = snap

    # Guard: first must be strictly before second (if present)
    if second and not (new_first < second):
        # Pull back to the previous business day before 'second'
        new_first = pyd(calendar.advance(qld(second), -1, ql.Days, ql.Preceding))
        while not (new_first < second):
            new_first = pyd(calendar.advance(qld(new_first), -1, ql.Days, ql.Preceding))

    # Rebuild schedule
    dv = ql.DateVector()
    dv.push_back(qld(prev))
    dv.push_back(qld(new_first))
    for x in dts[2:]:
        dv.push_back(qld(x))
    return ql.Schedule(dv, calendar, bdc)


def assert_schedule_matches_sheet_debug(
    row: pd.Series,
    schedule: ql.Schedule,
    *,
    calendar: ql.Calendar = ql.Mexico(),
    bdc: int = ql.Following,
    day_count: ql.DayCounter = ql.Actual360(),
    settlement_days: int = 1,
    count_from_settlement: bool = True,
    include_ref_date_events: bool = False,
    auto_fix: bool = False,
    allow_equal_on_first: bool = False,
    probe_bond: Optional[ql.Bond] = None,
) -> ql.Schedule:
    """
    Verifies that future coupon count matches the sheet AND prints a forensic
    report when it doesn't. Optionally returns a *fixed* schedule that snaps the
    first future date forward so the count matches 100%.
    """
    # 0) Core dates — use the SAME robust parser as everywhere else
    eval_date = parse_val_date(row["fecha"])
    issue_date = (parse_iso_date(row["fechaemision"])
                  if "" in row and pd.notna(row["fechaemision"]) else None)
    maturity = parse_iso_date(row["fechavto"])
    expected = (int(row["cuponesxcobrar"])
                if "cuponesxcobrar" in row and pd.notna(row["cuponesxcobrar"]) else None)

    # If the sheet says 0 coupons left, nothing to count/fix: keep schedule as-is.
    if expected is not None and expected == 0:
        return schedule

    # 1) Probe bond & settlement date (use provided probe_bond or build a tiny probe)
    if probe_bond is None:
        probe_bond = _build_probe_bond(
            schedule,
            eval_date=eval_date,
            day_count=day_count,
            calendar=calendar,
            bdc=bdc,
            settlement_days=settlement_days,
            issue_date=issue_date,
        )
    settle_date = pyd(probe_bond.settlementDate())

    # 2) Counts under standard variants
    cnt_eval_excl = _count_future_coupons(probe_bond, eval_date, include_ref=False)
    cnt_eval_incl = _count_future_coupons(probe_bond, eval_date, include_ref=True)
    cnt_sett_excl = _count_future_coupons(probe_bond, settle_date, include_ref=False)
    cnt_sett_incl = _count_future_coupons(probe_bond, settle_date, include_ref=True)

    # 3) Choose the model count according to your config
    ref_date = settle_date if count_from_settlement else eval_date
    chosen_cnt = _count_future_coupons(probe_bond, ref_date, include_ref_date_events)

    # 4) Forensic table
    cf_table = _flow_table(probe_bond, eval_date=eval_date, settle_date=settle_date)

    # 5) Quick schedule digest
    sched_py = [pyd(d) for d in sch_dates(schedule)]
    first_future_idx = 1 if len(sched_py) >= 2 else None
    first_future = sched_py[first_future_idx] if first_future_idx is not None else None
    last_date = sched_py[-1] if sched_py else None

    # 6) If mismatch, print a precise “why”
    if expected is not None and chosen_cnt != expected:
        print("──────────── Coupon Count Assertion (DIAGNOSTIC) ────────────")
        print(f"EMISORA={row.get('EMISORA','')}  SERIE={row.get('SERIE','')}")
        print(f"FECHA={eval_date}   Settlement(T+{settlement_days})={settle_date}")
        print(f"Schedule dates ({len(sched_py)}): {sched_py}")
        print(f"Maturity (sheet)={maturity}  Schedule.last={last_date}\n")
        print("Counts:")
        print(f"  vs FECHA  include_ref=False : {cnt_eval_excl}")
        print(f"  vs FECHA  include_ref=True  : {cnt_eval_incl}")
        print(f"  vs SETTLE include_ref=False : {cnt_sett_excl}")
        print(f"  vs SETTLE include_ref=True  : {cnt_sett_incl}")
        print(f"Chosen (cfg: from_settlement={count_from_settlement}, "
              f"include_ref_date_events={include_ref_date_events}) -> {chosen_cnt}")
        print(f"Sheet 'CUPONES X COBRAR' : {expected}\n")
        if first_future:
            ref_label = "SETTLE" if count_from_settlement else "FECHA"
            ref_val   = settle_date if count_from_settlement else eval_date
            cmp = "≤" if (first_future <= ref_val) else ">"
            print(f"First future date = {first_future}  |  {ref_label} = {ref_val}  "
                  f"→  first_future {cmp} {ref_label}")
            if (include_ref_date_events is False and first_future <= ref_val) or \
               (include_ref_date_events is True  and first_future <  ref_val):
                print("⚠ This boundary is the reason you are off by exactly 1.")
                print("   Under your counting rule, that payment is considered 'past'.")
        if last_date != maturity:
            print("⚠ Schedule.last differs from FECHA VCTO. If the vendor adjusts maturity, "
                  "set adjust_maturity_date=True.")
        print("────────────────────────────────────────────────────────────")
        print(cf_table.head(10).to_string(index=False))

        if not auto_fix:
            raise AssertionError(
                f"Coupon count mismatch. sheet={expected} model={chosen_cnt} "
                f"(from_settlement={count_from_settlement}, "
                f"include_ref_date_events={include_ref_date_events})."
            )

    # 7) Auto-fix: snap the first future date forward so the count matches the sheet
    if expected is not None and chosen_cnt != expected and auto_fix:
        ref_val = settle_date if count_from_settlement else eval_date
        fixed = _snap_first_future(
            schedule,
            calendar=calendar,
            bdc=bdc,
            min_first_date=ref_val,
            allow_equal=include_ref_date_events
        )
        # Recount after fix
        probe2 = _build_probe_bond(
            fixed,
            eval_date=eval_date,
            day_count=day_count,
            calendar=calendar,
            bdc=bdc,
            settlement_days=settlement_days,
            issue_date=issue_date
        )
        chosen_cnt2 = _count_future_coupons(probe2, ref_val, include_ref_date_events)
        if chosen_cnt2 != expected:
            print("Auto-fix attempted but counts still differ.")
            print("Fixed schedule:", [pyd(d) for d in sch_dates(fixed)])
            # raise AssertionError(f"After auto-fix, sheet={expected} model={chosen_cnt2}")
        return fixed

    return schedule



# =============================================================================
# Coupon counters for built instruments
# =============================================================================

def count_future_coupons(
    b: ql.Bond,
    *,
    from_settlement: bool = COUNT_FROM_SETTLEMENT,
    include_ref_date_events: bool = INCLUDE_REF_DATE_EVENTS
) -> int:
    """
    Count future coupons the way QL does it:
    - reference date = settlementDate() (if from_settlement) else Settings.evaluationDate
    - includeRefDateEvents comes from Settings at call time (Python wheel exposes only 0/1 arg)
    """
    ref = b.settlementDate() if from_settlement else ql.Settings.instance().evaluationDate
    n = 0
    with ql_include_ref_events(include_ref_date_events):
        for cf in b.cashflows():
            if ql.as_floating_rate_coupon(cf) is None:
                continue
            if not cf.hasOccurred(ref):  # one-arg form only
                n += 1
    return n


# =============================================================================
# Build a QL floater from a sheet row + curve
# =============================================================================
def build_qll_floater_from_row(
    row: pd.Series,
    *,
    calendar: ql.Calendar,
    dc: ql.DayCounter,
    bdc: int,
    settlement_days: int,
    SPREAD_IS_PERCENT: bool = True,
) -> BuiltBond:
    """
    Create your FloatingRateBond model with an explicit schedule that matches the sheet.
    """
    # --- read inputs (Spanish columns) ---
    eval_date     = parse_val_date(row["fecha"])
    issue_date    = parse_iso_date(row["fechaemision"])
    maturity_date = parse_iso_date(row["fechavcto"])
    face_adj      = float(row["valornominalactualizado"])
    raw_spread    = 0.0 if pd.isna(row["sobretasa"]) else float(row["sobretasa"])
    spread_decimal = (raw_spread / 100.0) if SPREAD_IS_PERCENT else raw_spread




    # --- global QL settings ---
    ql.Settings.instance().evaluationDate = qld(eval_date)
    ql.Settings.instance().includeReferenceDateEvents = INCLUDE_REF_DATE_EVENTS
    ql.Settings.instance().enforceTodaysHistoricFixings = False



    # --- schedule that forces remaining coupons to match the sheet ---
    explicit_schedule = compute_sheet_schedule_force_match(
        row,
        calendar=calendar,
        bdc=bdc,
        default_freq_days=parse_coupon_period(row.get("FREC. CPN")),
        settlement_days=settlement_days,  # ← add this
        count_from_settlement=COUNT_FROM_SETTLEMENT,  # ← and this (keeps your toggle)
        include_boundary_for_count=True,  # ← ven
    )
    try:
        floating_rate_index_name = SUBYACENTE_TO_INDEX_MAP[row["reglacupon"]]
    except KeyError as e:
        raise e
    # --- your model (ensure it supports 'schedule=...') ---
    frb = FloatingRateBond(
        face_value=face_adj,
        floating_rate_index_name=floating_rate_index_name,
        spread=spread_decimal,
        issue_date=issue_date,
        maturity_date=maturity_date,
        coupon_frequency=parse_coupon_period(row.get("freccpn")),
        day_count=dc,
        calendar=calendar,
        business_day_convention=bdc,
        settlement_days=settlement_days,

        schedule=explicit_schedule,        # <— IMPORTANT
    )
    frb.set_valuation_date( eval_date,)

    # --- assert/diagnose + (optionally) auto-fix the front boundary ---
    # with_yield = float(row["TASA DE RENDIMIENTO"]) / 100
    # try:
    #     frb._setup_pricer(with_yield=with_yield)
    # except Exception as e:
    #     raise e
    # fixed_schedule = assert_schedule_matches_sheet_debug(
    #     row,
    #     explicit_schedule,
    #     calendar=calendar,
    #     bdc=bdc,                    # ✅ correct type (int)
    #     day_count=dc,               # ✅ correct type (ql.DayCounter)
    #     settlement_days=settlement_days,
    #     count_from_settlement=COUNT_FROM_SETTLEMENT,
    #     include_ref_date_events=INCLUDE_REF_DATE_EVENTS,
    #     auto_fix=True,                                   # snap first future if needed
    #     allow_equal_on_first=INCLUDE_REF_DATE_EVENTS,    # align with chosen include-ref rule
    #     probe_bond=frb._bond
    # )
    # def _dates(s: ql.Schedule) -> List[dt.date]:
    #     return [pyd(d) for d in sch_dates(s)]
    # if _dates(fixed_schedule) != _dates(explicit_schedule):
    #     frb = FloatingRateBond(
    #         face_value=face_adj,
    #         floating_rate_index=tiie_index,
    #         spread=spread_decimal,
    #         issue_date=issue_date,
    #         maturity_date=maturity_date,
    #         coupon_frequency=parse_coupon_period(row.get("FREC. CPN"), 28),
    #         day_count=dc,
    #         calendar=calendar,
    #         business_day_convention=bdc,
    #         settlement_days=settlement_days,
    #         valuation_date=eval_date,
    #         schedule=fixed_schedule,  # <— IMPORTANT
    #     )
    return  frb


# =============================================================================
# Cashflow extraction (future only)
# =============================================================================
def extract_future_cashflows(
    built: BuiltBond,
    *,
    from_settlement: bool = COUNT_FROM_SETTLEMENT,
    include_ref_date_events: bool = INCLUDE_REF_DATE_EVENTS
) -> Dict[str, List[Dict[str, Any]]]:
    ql_bond = built.bond.bond
    ql.Settings.instance().evaluationDate = qld(built.eval_date)
    ref = ql_bond.settlementDate() if from_settlement else ql.Settings.instance().evaluationDate

    out: Dict[str, List[Dict[str, Any]]] = {"floating": [], "redemption": []}
    with ql_include_ref_events(include_ref_date_events):
        for cf in ql_bond.cashflows():
            if cf.hasOccurred(ref):  # one-arg form only
                continue
            cpn = ql.as_floating_rate_coupon(cf)
            if cpn is not None:
                out["floating"].append({
                    "payment_date": pyd(cpn.date()),
                    "fixing_date": pyd(cpn.fixingDate()),
                    "rate": float(cpn.rate()),
                    "spread": float(cpn.spread()),
                    "amount": float(cpn.amount()),
                })
            else:
                out["redemption"].append({
                    "payment_date": pyd(cf.date()),
                    "amount": float(cf.amount()),
                })
    return out


# =============================================================================
# Main pricing loop
# =============================================================================





def run_price_check(
    TIIE_BONDS: pd.DataFrame,
    *,
    SPREAD_IS_PERCENT: bool = True,
    price_tol_bp: float = 2.0,
    coupon_tol_bp: float = 1.0,  # (kept for symmetry; not used directly here)
) -> Tuple[pd.DataFrame, Dict[str, FloatingRateBond]]:
    results: List[Dict[str, Any]] = []
    instrument_map: Dict[str, FloatingRateBond] = {}
    curve_cache: Dict[(dt.date,str), ql.YieldTermStructure] = {}

    for ix, row in tqdm(TIIE_BONDS.iterrows(), desc="building instruments"):
        eval_date = parse_val_date(row["FECHA"])
        eval_date=dt.datetime(eval_date.year,eval_date.month,eval_date.day,tzinfo=pytz.utc)
        # Build/reuse curve for that eval date
        index_uid = SUBYACENTE_TO_INDEX_MAP[row["SUBYACENTE"]]
        cached_id=(eval_date,index_uid)
        if cached_id not in curve_cache:

            curve_cache[cached_id] = build_zero_curve(target_date=eval_date,
                                                      index_identifier=index_uid)
        curve = curve_cache[cached_id]

        if row["CUPON ACTUAL"] == 0.0 or row["CUPONES X COBRAR"] == 0:
            continue

        # Build bond (explicit schedule)
        built = build_qll_floater_from_row(
            row,
            calendar=ql.Mexico(), dc=ql.Actual360(), bdc=ql.ModifiedFollowing, settlement_days=0,
        )

        # Model analytics (force construction)
        try:
            analytics = built.bond.analytics(with_yield=float(row["TASA DE RENDIMIENTO"]) / 100.0)
        except Exception as e:
            # Some FRNs with CUPONES X COBRAR == 0 might not be representable as FloatingRateBond.
            raise e

        ql_bond = built.bond._bond  # underlying QL object

        face    = float(row["VALOR NOMINAL ACTUALIZADO"])
        model_dirty = float(analytics["dirty_price"]) * face / 100.0
        model_clean = float(analytics["clean_price"]) * face / 100.0
        model_accr  = model_dirty - model_clean
        model_accr_per100 = 100.0 * (model_accr / face)

        # Market sheet dirty/clean (per 100)
        mkt_dirty = float(row["PRECIO SUCIO"])
        mkt_clean = float(row["PRECIO LIMPIO"])
        if mkt_dirty == 0:
            continue

        # Running coupon (find the period containing eval_date or settlement)
        running_coupon_model = np.nan
        dias_transcurridos   = np.nan
        if ql_bond.cashflows():
            ref_for_days = ql_bond.settlementDate() if COUNT_FROM_SETTLEMENT else qld(eval_date)
            for cf in ql_bond.cashflows():
                cpn = ql.as_floating_rate_coupon(cf)
                if cpn is None:
                    continue
                if cpn.accrualStartDate() <= ref_for_days < cpn.accrualEndDate():
                    running_coupon_model = 100.0 * float(cpn.rate())
                    dc_inst = built.bond.day_count
                    dias_transcurridos = int(dc_inst.dayCount(cpn.accrualStartDate(), ref_for_days))
                    break

        # Future coupons
        future_cpn_count = count_future_coupons(
            ql_bond,
            from_settlement=COUNT_FROM_SETTLEMENT,
            include_ref_date_events=INCLUDE_REF_DATE_EVENTS
        )
        expected_count = int(row["CUPONES X COBRAR"]) if not pd.isna(row.get("CUPONES X COBRAR")) else np.nan

        # Diffs
        price_diff_bp  = 100.0 * (model_dirty - mkt_dirty) / mkt_dirty
        coupon_diff_bp = ((running_coupon_model - float(row["CUPON ACTUAL"])) * 100.0
                          if not np.isnan(running_coupon_model) else np.nan)
        pass_price     = abs(price_diff_bp) <= price_tol_bp
        pass_cpn_count = (np.isnan(expected_count) or (future_cpn_count == expected_count))

        instrument_hash= built.bond.content_hash()
        results.append({
            "instrument_hash": instrument_hash,

            "FECHA": eval_date,
            "UID":f"{row['TIPO VALOR']}_{row['EMISORA']}_{row['SERIE']}",
            "SUBYACENTE":row["SUBYACENTE"],
            "VALOR NOMINAL": float(row["VALOR NOMINAL"]),
            "SOBRETASA_in": float(row["SOBRETASA"]),
            "SOBRETASA_decimal": (float(row["SOBRETASA"]) / 100.0) if SPREAD_IS_PERCENT and not pd.isna(row["SOBRETASA"]) else float(row["SOBRETASA"] or 0.0),
            "CUPON ACTUAL (sheet) %": float(row["CUPON ACTUAL"]),
            "CUPON ACTUAL (model) %": running_coupon_model,
            "coupon_diff_bp": coupon_diff_bp,
            "PRECIO SUCIO (sheet)": mkt_dirty,
            "PRECIO SUCIO (model)": model_dirty,
            "price_diff_bp": price_diff_bp,
            "PRECIO LIMPIO (sheet)": mkt_clean,
            "PRECIO LIMPIO (model)": model_clean,
            "accrued_per_100 (model)": model_accr_per100,
            "CUPONES X COBRAR (sheet)": expected_count,
            "CUPONES FUTUROS (model)": future_cpn_count,
            "pass_price": pass_price,
            "pass_coupon_count": pass_cpn_count,
            "DIAS TRANSC. CPN (sheet)": int(row["DIAS TRANSC. CPN"]) if pd.notna(row.get("DIAS TRANSC. CPN")) else np.nan,
            "DIAS TRANSC. CPN (model)": dias_transcurridos,
        })

        instrument_map[instrument_hash] = {"instrument":built.bond,"extra_market_info":{"yield":row["TASA DE RENDIMIENTO"]/100}}

    return pd.DataFrame(results), instrument_map


def build_position_from_sheet(
    sheet_path: str | Path,
    *,
    notional_per_line: float = 100_000_000.0,
    out_path: str | Path | None = None
) -> Tuple[Position, Dict[str, Any], str]:
    """
    Build instruments from a vendor sheet and dump a 'position.json'-style file.
    Returns (Position, cfg_dict, position_json_path, df_out_csv_path).
    """
    sheet_path = str(sheet_path)
    df = pd.read_excel(sheet_path)

    floating_tiie  = df[df["SUBYACENTE"].astype(str).str.contains("TIIE", na=False)]
    floating_cetes = df[df["SUBYACENTE"].astype(str).str.contains("CETE", na=False)]
    all_floating   = pd.concat([floating_tiie, floating_cetes], axis=0, ignore_index=True)

    df_out, instrument_map = run_price_check(all_floating)
    pd.set_option("display.float_format", lambda x: f"{x:,.6f}")

    ms_assets_map=msc.Asset.filter(unique_identifier__in=df_out["UID"].to_list())
    ms_assets_map={k.unique_identifier:k.id for k in ms_assets_map}

    df_out["asset_id"]=df_out["UID"].map(ms_assets_map)
    # Pick best priced instruments
    position_instruments={}
    hash_to_id_map=df_out[["instrument_hash","asset_id"]].set_index("instrument_hash").dropna()["asset_id"].to_dict()
    for k, v in instrument_map.items():

        if k in hash_to_id_map.keys():
            #add main sequence asset
            v["instrument"].main_sequence_asset_id=int(hash_to_id_map[k])
            position_instruments[k]=v

    # Units scaled by price from the model
    position_lines = [
        PositionLine(
            units=int(notional_per_line / b["instrument"].price()),
            **b
        )
        for b in position_instruments.values()
    ]
    position = Position(lines=position_lines)

    # ---- THIS LINE (existing): build JSON dict --------------------------------
    dump = position.to_json_dict()

    # ---- : write position.json immediately after 'dump = ...' --------------
    # Choose a valuation date from the selected rows (fallback: today)
    if not df_out.empty:
        val_ts = pd.to_datetime(df_out["FECHA"].max())
        if isinstance(val_ts, pd.Timestamp) and val_ts.tzinfo is not None:
            val_ts = val_ts.tz_convert(None)
        val_date = val_ts.date() if isinstance(val_ts, pd.Timestamp) else pd.to_datetime(val_ts).date()
    else:
        val_date = dt.date.today()

    cfg = {
        "valuation": {"valuation_date": val_date.isoformat()},
        "position": dump
    }

    # Default output next to the app's expected file
    out_path = Path(out_path) if out_path is not None else (Path(__file__).resolve().parent / "position.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as fh:
        json.dump(cfg, fh, indent=2, default=str)

    # ---- NEW: also dump df_out in the SAME location as position.json ----------
    df_out_path = out_path.parent / "df_out.csv"
    df_out.to_csv(df_out_path, index=False)

    return position, cfg, str(out_path), str(df_out_path)

