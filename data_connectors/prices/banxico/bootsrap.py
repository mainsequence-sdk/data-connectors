import numpy as np
import pandas as pd
from scipy.optimize import brentq

# -------------------------
# utilities
# -------------------------

def _pick_price(row):
    # Prefer dirty if available
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

def _solve_df_T_loglinear(price, pre_known_sum, df_S, S, T, coupon_dates_between_S_T, coupon_amt, final_cf):
    """
    Solve DF(T) with log-linear segment between S and T:
        DF(t) = DF(S)^(1-w) * DF(T)^w, w = (t - S) / (T - S)
    price = pre_known_sum + sum_{S<t<T} coupon_amt * DF(t) + final_cf * DF(T)
    Monotone in DF(T). We bracket and solve with brentq.
    """
    if not coupon_dates_between_S_T:
        # Simple case: no coupons between S and T
        df_T = (price - pre_known_sum) / final_cf
        return max(1e-12, min(df_T, min(df_S, 0.999999)))

    # monotone function of df_T
    def f(df_T):
        # sum unknown coupons
        sum_unk = 0.0
        if df_T <= 0.0:
            return price - (pre_known_sum)  # avoid pow errors
        ln_dfS = np.log(df_S)
        ln_dfT = np.log(df_T)
        for t in coupon_dates_between_S_T:
            w = (t - S) / (T - S)
            # DF(t) = exp( (1-w) ln dfS + w ln dfT )
            df_t = np.exp((1.0 - w) * ln_dfS + w * ln_dfT)
            sum_unk += coupon_amt * df_t
        pv = pre_known_sum + sum_unk + final_cf * df_T
        return price - pv

    # bracket: DF(T) must be in (0, DF(S)]
    lo = 1e-12
    hi = min(df_S, 0.999999)

    f_lo = f(lo)
    f_hi = f(hi)

    if f_lo * f_hi > 0:
        # Failsafe: extend with flat-forward from last pillar for the unknown coupons,
        # then back out DF(T) with a single division.
        m_ff = min(-1e-12, np.log(df_S) / max(S, 1.0))  # very conservative if S~0
        sum_unk_ff = 0.0
        for t in coupon_dates_between_S_T:
            df_t = df_S * np.exp(m_ff * (t - S))  # forward-flat from S
            sum_unk_ff += coupon_amt * df_t
        df_T = (price - pre_known_sum - sum_unk_ff) / final_cf
        return max(1e-12, min(df_T, hi))

    return brentq(f, lo, hi, xtol=1e-14, maxiter=200)

# -------------------------
# core bootstrap (single time_index)
# -------------------------

def bootstrap_from_curve_df(curve_df, day_count_convention: float = 360.0):
    """
    Bootstraps for a *single* time_index.
    Required columns in curve_df:
      ['type','tenor_days','clean_price','dirty_price','coupon']
    Conventions:
      - overnight_rate 'dirty_price' holds the annual rate in *decimal* (e.g., 0.105)
      - CETES face = 10  -> DF = price / 10
      - MBONOS: semiannual (182d), Act/360, dirty price used, final coupon uses stub alpha
    Returns: DataFrame with ['days_to_maturity','zero_rate'] (money-market simple, Act/360)
    """
    # normalize and sort
    df = curve_df.copy()
    df["tenor_days"]  = pd.to_numeric(df["tenor_days"], errors="coerce").astype(float)
    df["clean_price"] = pd.to_numeric(df["clean_price"], errors="coerce")
    df["dirty_price"] = pd.to_numeric(df["dirty_price"], errors="coerce")
    df["coupon"]      = pd.to_numeric(df["coupon"], errors="coerce")
    df = df.sort_values("tenor_days")

    pillars = {0.0: 1.0}  # tenor -> DF

    for _, r in df.iterrows():
        inst_type = str(r["type"]).strip()
        T = float(r["tenor_days"])

        if inst_type == "overnight_rate":
            rate = r["dirty_price"]
            if rate is None or pd.isna(rate):
                raise ValueError("Overnight row must carry the rate in dirty_price (decimal).")
            rate = float(rate)
            if rate > 1.0:  # tolerate percentage input
                rate /= 100.0
            df_1d = 1.0 / (1.0 + rate * (1.0 / day_count_convention))
            pillars[T] = df_1d
            continue

        if inst_type == "zero_coupon":
            price = _pick_price(r)
            pillars[T] = float(price) / 10.0  # CETES face 10
            continue

        if inst_type == "fixed_bond":
            price = _pick_price(r)               # DIRTY price
            c_rate = float(r["coupon"]) / 100.0  # percent -> decimal
            # schedule strictly before T
            step = 182.0
            k = int(np.floor(T / step))  # number of coupons strictly less than or equal to T // step
            pre_T_dates = list(step * np.arange(1, k + 1))  # 182, 364, ..., <= floor(T/182)*182
            # ensure we exclude the coupon at T if T is an exact multiple of 182
            if pre_T_dates and np.isclose(pre_T_dates[-1], T):
                pre_T_dates = pre_T_dates[:-1]
            last_cpn_time = pre_T_dates[-1] if pre_T_dates else 0.0
            alpha = step / day_count_convention
            alpha_T = (T - last_cpn_time) / day_count_convention  # stub for final period
            c_amt = c_rate * alpha * 100.0
            final_cf = 100.0 + c_rate * alpha_T * 100.0

            # interpolate known DFs with log-linear (no cubic, no overshoot)
            df_func = _build_loglinear_df_func(pillars)
            S = max(pillars.keys())  # last pillar < current loop, by construction

            # Split coupons into those we can discount with known pillars (<= S)
            # and those in (S, T) which depend on DF(T)
            pre_known_sum = 0.0
            between_S_T = []
            for t in pre_T_dates:
                if t <= S + 1e-12:
                    pre_known_sum += c_amt * df_func(t)
                else:
                    between_S_T.append(t)

            if T <= S + 1e-12:
                # shouldn't happen if tenor sorted, but guard anyway
                df_T = df_func(T)
            else:
                df_S = df_func(S)
                df_T = _solve_df_T_loglinear(
                    price=price,
                    pre_known_sum=pre_known_sum,
                    df_S=df_S,
                    S=S,
                    T=T,
                    coupon_dates_between_S_T=between_S_T,
                    coupon_amt=c_amt,
                    final_cf=final_cf
                )

            # sanity guards: keep DFs in (0, df(S)]
            df_T = max(1e-12, min(df_T, df_func(S)))
            pillars[T] = df_T
            continue

        # ignore unknown types silently (or raise)
        # raise ValueError(f"Unknown instrument type: {inst_type}")

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
# wrapper across time_index (your public API)
# -------------------------

def boostrap_mbono_curve(update_statistics, curve_unique_identifier: str, base_node_curve_points=None):
    """
    Returns a single DataFrame with:
      ['time_index', 'days_to_maturity', 'zero_rate']
    Bootstraps per time_index using:
      - overnight_rate (Banxico target as 1d anchor, decimal rate in dirty_price),
      - zero_coupon (CETES, DF = price/10),
      - fixed_bond (MBONOS, 182d coupons, Act/360, dirty price).
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
