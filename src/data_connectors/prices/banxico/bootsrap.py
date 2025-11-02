import numpy as np
import pandas as pd
from scipy.optimize import brentq

# -------------------------
# utilities
# -------------------------

def _pick_price(row):
    """
    Prefer dirty if available; otherwise clean.
    Raises if neither is present.
    """
    dp = row.get("dirty_price")
    if dp is not None and not pd.isna(dp):
        return float(dp)
    cp = row.get("clean_price")
    if cp is None or pd.isna(cp):
        raise ValueError("Instrument has neither dirty_price nor clean_price.")
    return float(cp)

def _build_loglinear_df_func(pillar_points):
    """
    Returns df(t) using *log-linear* interpolation of discount factors.
    For t beyond the last pillar, extends with the *last slope* in log-DF
    but never allows an upward slope (enforces monotone non-increasing DFs).
    """
    xs = np.array(sorted(pillar_points.keys()), dtype=float)
    ys = np.array([pillar_points[x] for x in xs], dtype=float)
    if np.any(ys <= 0):
        raise ValueError("Non-positive DF in pillars.")
    logys = np.log(ys)

    def df(t):
        t = float(t)
        if t <= xs[0]:
            if len(xs) == 1:
                return float(ys[0])
            m = (logys[1] - logys[0]) / (xs[1] - xs[0])
            y = logys[0] + m * (t - xs[0])
            return float(np.exp(y))
        if t >= xs[-1]:
            if len(xs) == 1:
                return float(ys[0])
            m_last = (logys[-1] - logys[-2]) / (xs[-1] - xs[-2])
            m_last = min(m_last, -1e-12)  # never extrapolate upwards
            y = logys[-1] + m_last * (t - xs[-1])
            return float(np.exp(y))
        j = np.searchsorted(xs, t) - 1
        j = max(0, min(j, len(xs) - 2))
        w = (t - xs[j]) / (xs[j + 1] - xs[j])
        y = logys[j] * (1.0 - w) + logys[j + 1] * w
        return float(np.exp(y))

    return df

def _solve_df_T_loglinear_generic(price, pre_known_sum, df_S, S, T, future_times, future_amounts):
    """
    Generic solver for DF(T) with log-linear DF interpolation between S and T.
    Handles arbitrary cashflow amounts at arbitrary times in (S, T].
    Uses brentq with a safe fallback if the bracket fails.
    """
    if T <= S + 1e-12:
        # shouldn't happen in sorted tenor loop
        return df_S

    ln_dfS = np.log(df_S)
    target = float(price)

    def pv_given_dfT(dfT):
        if dfT <= 0.0:
            return pre_known_sum  # avoid log underflow; PV tends to pre_known_sum
        ln_dfT = np.log(dfT)
        pv = pre_known_sum
        for t, a in zip(future_times, future_amounts):
            if t < T - 1e-12:
                w = (t - S) / (T - S)
                df_t = np.exp((1.0 - w) * ln_dfS + w * ln_dfT)
            else:
                df_t = dfT
            pv += a * df_t
        return pv

    # Bracket DF(T) in (0, df_S]
    lo, hi = 1e-14, float(df_S)
    f_lo = pv_given_dfT(lo) - target
    f_hi = pv_given_dfT(hi) - target
    if f_lo * f_hi > 0:
        # Fallback: forward-flat from S for all unknown CFs to approximate PV,
        # then back out DF(T) by a single division on the residual.
        m_ff = min(-1e-12, np.log(df_S) / max(S, 1.0))  # conservative slope if S ~ 0
        sum_unk_ff = 0.0
        for t, a in zip(future_times, future_amounts):
            df_t = df_S * np.exp(m_ff * (t - S))
            sum_unk_ff += a * df_t
        df_T = (target - pre_known_sum - sum_unk_ff) / future_amounts[-1]  # assumes last is at T
        return max(1e-14, min(df_T, hi))

    return brentq(lambda x: pv_given_dfT(x) - target, lo, hi, xtol=1e-14, maxiter=200)

# -------------------------
# core bootstrap (single time_index)
# -------------------------

def bootstrap_from_curve_df(curve_df, day_count_convention: float = 360.0):
    """
    Bootstraps for a *single* time_index.

    Required columns in curve_df:
      ['type','tenor_days','clean_price','dirty_price','coupon']

    Conventions:
      - 'overnight_rate': 'dirty_price' holds the annual rate in *decimal* (e.g., 0.105).
      - CETES (type 'zero_coupon'): face = 10  -> DF = price / 10
      - MBONOS (type 'fixed_bond'): semiannual (182d), Act/360, DIRTY price used, final coupon uses stub alpha
      - BONDES F / G / D (types 'bonde_f','bonde_g','bonde_d'):
          * 28-day coupons; 'coupon' column holds the market spread 's'
            (percentage points; if value looks like bps, auto-converted)
          * First coupon C1 uses realized TC_dev for accrued days and (r+s) for remaining days
          * Accrued (to convert clean->dirty) uses TC_dev (without spread)

    Returns: DataFrame with ['days_to_maturity','zero_rate'] (money-market simple, Act/360, in PERCENT)
    """
    df = curve_df.copy()
    df["tenor_days"]  = pd.to_numeric(df["tenor_days"], errors="coerce").astype(float)
    df["clean_price"] = pd.to_numeric(df["clean_price"], errors="coerce")
    df["dirty_price"] = pd.to_numeric(df["dirty_price"], errors="coerce")
    df["coupon"]      = pd.to_numeric(df["coupon"], errors="coerce")
    df = df.sort_values("tenor_days")

    pillars = {0.0: 1.0}  # tenor (days) -> DF
    on_rate_pct = None     # current overnight funding rate, PERCENT (2-decimal convention)

    # ---- helpers tied to this run ----
    def _ensure_on_rate_pct():
        nonlocal on_rate_pct
        if on_rate_pct is not None:
            return on_rate_pct
        if 1.0 in pillars:
            df1 = pillars[1.0]
            r = ((1.0/df1) - 1.0) * day_count_convention / 1.0 * 100.0
            on_rate_pct = round(float(r), 2)
            return on_rate_pct
        raise ValueError("Need an 'overnight_rate' row (or a 1-day pillar) before BONDES F/G/D.")

    def _normalize_spread_pct(s_raw):
        if s_raw is None or pd.isna(s_raw):
            return 0.0
        s = float(s_raw)
        # If |s| > 5 assume the unit is basis points and convert to percentage points.
        if abs(s) > 5.0:
            s = s / 100.0
        return s

    def _frn_28day_coupon_params(T_days, r_pct, s_pct):
        """
        Returns:
          k   : number of 28d coupons up to and including maturity (ceil)
          d   : days accrued in current coupon
          rem : days to next payment
          C1  : first coupon amount (face 100)
          C   : regular 28d coupon amount (face 100)
        """
        step = 28.0
        k = int(np.ceil(T_days / step)) if T_days > 0 else 0
        if k == 0:
            return 0, 0.0, 0.0, 0.0, 0.0

        rem = T_days - step * (k - 1)  # time from today to next payment
        d   = step - rem if rem > 0 else 0.0  # days already accrued in current coupon

        # Realized part (accrued) without spread:
        TCdev = 0.0 if d <= 0 else (((1.0 + r_pct/36000.0)**d - 1.0) * 36000.0 / d)

        # Expected part with spread applied to the remaining/all days:
        r_eff = r_pct + s_pct
        TC1 = (((1.0 + TCdev/36000.0) * (1.0 + r_eff/36000.0)**(step - d) - 1.0) * 36000.0 / step)
        TC  = (((1.0 + r_eff/36000.0)**step - 1.0) * 36000.0 / step)

        C1 = 100.0 * step * TC1 / 36000.0
        C  = 100.0 * step * TC  / 36000.0
        return k, d, rem, C1, C

    # ---- main loop ----
    for _, r in df.iterrows():
        inst_type = str(r["type"]).strip().lower()
        T = float(r["tenor_days"])
        if "overnight_rate" not in df["type"].to_list():
            continue
        if inst_type == "overnight_rate":
            rate = r["dirty_price"]
            if rate is None or pd.isna(rate):
                raise ValueError("Overnight row must carry the rate in dirty_price (decimal).")
            rate = float(rate)
            if rate > 1.0:  # tolerate percentage input
                rate /= 100.0
            df_1d = 1.0 / (1.0 + rate * (1.0 / day_count_convention))
            pillars[T] = df_1d
            on_rate_pct = round(rate * 100.0, 2)
            continue

        if inst_type == "zero_coupon":  # CETES
            price = _pick_price(r)
            pillars[T] = float(price) / 10.0  # CETES face 10
            continue

        if inst_type == "fixed_bond":  # M Bonos
            price = _pick_price(r)
            c_rate = float(r["coupon"]) / 100.0  # percent -> decimal
            step = 182.0
            k = int(np.floor(T / step))
            pre_T_dates = list(step * np.arange(1, k + 1))
            if pre_T_dates and np.isclose(pre_T_dates[-1], T):
                pre_T_dates = pre_T_dates[:-1]
            last_cpn_time = pre_T_dates[-1] if pre_T_dates else 0.0
            alpha = step / day_count_convention
            alpha_T = (T - last_cpn_time) / day_count_convention
            c_amt = c_rate * alpha * 100.0
            final_cf = 100.0 + c_rate * alpha_T * 100.0

            df_func = _build_loglinear_df_func(pillars)
            S = max(pillars.keys())

            pre_known_sum = 0.0
            future_times = []
            future_amts  = []
            for t in pre_T_dates:
                if t <= S + 1e-12:
                    pre_known_sum += c_amt * df_func(t)
                else:
                    future_times.append(t)
                    future_amts.append(c_amt)

            # final cash flow at T
            future_times.append(T)
            future_amts.append(final_cf)

            if T <= S + 1e-12:
                df_T = df_func(T)
            else:
                df_S = df_func(S)
                df_T = _solve_df_T_loglinear_generic(price, pre_known_sum, df_S, S, T, future_times, future_amts)

            df_T = max(1e-12, min(df_T, df_func(S)))
            pillars[T] = df_T
            continue

        # ---- BONDES F / G / D (28-day FRNs) ----
        if inst_type in {"floating_bondes_d", "floating_bondes_f", "floating_bondes_g"}:
            r_pct = _ensure_on_rate_pct()                 # ON rate, percent
            s_pct = _normalize_spread_pct(r["coupon"])    # spread, percent

            k, d, rem, C1, C = _frn_28day_coupon_params(T, r_pct, s_pct)
            if k <= 0:
                pillars[T] = pillars[max(pillars.keys())]
                continue

            # Accrued (for clean->dirty): TC_dev only (without spread)
            TCdev = 0.0 if d <= 0 else (((1.0 + r_pct/36000.0)**d - 1.0) * 36000.0 / d)
            accrued = 100.0 * d * TCdev / 36000.0

            # choose price (prefer DIRTY; else CLEAN + accrued)
            price_clean = r.get("clean_price", np.nan)
            price_dirty = r.get("dirty_price", np.nan)
            if price_dirty is not None and not pd.isna(price_dirty):
                price = float(price_dirty)
            elif price_clean is not None and not pd.isna(price_clean):
                price = float(price_clean) + accrued
            else:
                raise ValueError("BONDES F/G/D row must bring clean or dirty price.")

            # Build payment times/amounts
            step = 28.0
            pay_times = [rem + step * j for j in range(0, k - 1)]  # k-1 pre-final coupons
            if len(pay_times) > 0:
                pay_amts = [C1] + [C] * (len(pay_times) - 1)
                last_coupon_at_T = C
            else:
                pay_amts = []
                last_coupon_at_T = C1  # k==1 -> the only coupon at T is C1

            # final payment at T: principal + last coupon
            pay_times.append(T)
            pay_amts.append(100.0 + last_coupon_at_T)

            df_func = _build_loglinear_df_func(pillars)
            S = max(pillars.keys())

            pre_known_sum = 0.0
            future_times, future_amts = [], []
            for t, a in zip(pay_times, pay_amts):
                if t <= S + 1e-12:
                    pre_known_sum += a * df_func(t)
                else:
                    future_times.append(t)
                    future_amts.append(a)

            if T <= S + 1e-12:
                df_T = df_func(T)
            else:
                df_S = df_func(S)
                df_T = _solve_df_T_loglinear_generic(price, pre_known_sum, df_S, S, T, future_times, future_amts)

            df_T = max(1e-12, min(df_T, df_func(S)))
            pillars[T] = df_T
            continue

        # Unknown instrument type
        raise ValueError(f"Unknown instrument type: {inst_type}")

    # Build zero curve (money-market simple, Act/360)
    tenors = np.array(sorted(pillars.keys()))
    dfs = np.array([pillars[t] for t in tenors])
    mask = tenors > 0
    T = tenors[mask]
    DF = dfs[mask]
    zeros = ((1.0 / DF) - 1.0) * (day_count_convention / T) * 100.0
    out = pd.DataFrame({"days_to_maturity": T.astype(int), "zero_rate": zeros})
    return out.sort_values("days_to_maturity", kind="mergesort").reset_index(drop=True)


# -------------------------
# wrapper across time_index (public API)
# -------------------------

def boostrap_mbono_curve(update_statistics, curve_unique_identifier: str, base_node_curve_points=None):
    """
    Returns a single DataFrame with:
      ['time_index', 'days_to_maturity', 'zero_rate']

    Bootstraps per time_index using rows of types:
      - 'overnight_rate' (Banxico target as 1d anchor, decimal rate in dirty_price)
      - 'zero_coupon'   (CETES, DF = price/10)
      - 'fixed_bond'    (MBONOS, 182d coupons, Act/360, dirty price)
      - 'bonde_f'/'bonde_g'/'bonde_d' (28d FRNs on ON funding + spread)
    """
    last_update = update_statistics.asset_time_statistics[curve_unique_identifier]
    nodes = base_node_curve_points.get_df_between_dates(start_date=last_update, great_or_equal=True)

    if nodes.empty:
        return pd.DataFrame(columns=["time_index", "days_to_maturity", "zero_rate"])

    required = {"time_index", "type", "tenor_days", "clean_price", "dirty_price", "coupon"}
    missing = required.difference(nodes.columns)
    if missing:
        raise KeyError(f"Missing columns: {sorted(missing)}")

    results = []
    for time_index, curve_df in nodes.groupby("time_index"):
        zdf = bootstrap_from_curve_df(curve_df)
        zdf.insert(0, "time_index", time_index)
        results.append(zdf)

    return pd.concat(results, ignore_index=True)
