# us_treasuries/builders.py
from __future__ import annotations

import numpy as np
import pandas as pd
from mainsequence.tdag import APIDataNode


def _build_loglinear_df_func(pillars: dict[float, float]):
    xs = np.array(sorted(pillars.keys()), dtype=float)
    ys = np.array([pillars[x] for x in xs], dtype=float)
    if np.any(ys <= 0): raise ValueError("Non-positive DF in pillars.")
    logys = np.log(ys)
    def df(t: float) -> float:
        t = float(t)
        if t <= xs[0]:
            if len(xs) == 1: return float(ys[0])
            m = (logys[1] - logys[0]) / (xs[1] - xs[0])
            return float(np.exp(logys[0] + m * (t - xs[0])))
        if t >= xs[-1]:
            if len(xs) == 1: return float(ys[0])
            m_last = (logys[-1] - logys[-2]) / (xs[-1] - xs[-2])
            m_last = min(m_last, -1e-12)
            return float(np.exp(logys[-1] + m_last * (t - xs[-1])))
        j = np.searchsorted(xs, t) - 1
        j = max(0, min(j, len(xs) - 2))
        w = (t - xs[j]) / (xs[j + 1] - xs[j])
        y = logys[j] * (1 - w) + logys[j + 1] * w
        return float(np.exp(y))
    return df

def _solve_df_T_loglinear(price, pre_known_sum, df_S, S, T, coupon_dates_between_S_T, coupon_amt, final_cf):
    if not coupon_dates_between_S_T:
        df_T = (price - pre_known_sum) / final_cf
        return max(1e-12, min(df_T, min(df_S, 0.999999)))
    def f(df_T):
        if df_T <= 0.0: return price - (pre_known_sum)
        ln_dfS, ln_dfT = np.log(df_S), np.log(df_T)
        sum_unk = 0.0
        for t in coupon_dates_between_S_T:
            w = (t - S) / (T - S)
            df_t = np.exp((1.0 - w) * ln_dfS + w * ln_dfT)
            sum_unk += coupon_amt * df_t
        pv = pre_known_sum + sum_unk + final_cf * df_T
        return price - pv
    lo, hi = 1e-12, min(df_S, 0.999999)
    from scipy.optimize import brentq
    f_lo, f_hi = f(lo), f(hi)
    if f_lo * f_hi > 0:
        m_ff = min(-1e-12, np.log(df_S) / max(S, 1.0))
        sum_unk_ff = 0.0
        for t in coupon_dates_between_S_T:
            df_t = df_S * np.exp(m_ff * (t - S))
            sum_unk_ff += coupon_amt * df_t
        df_T = (price - pre_known_sum - sum_unk_ff) / final_cf
        return max(1e-12, min(df_T, hi))
    return brentq(f, lo, hi, xtol=1e-14, maxiter=200)

def bootstrap_zero_from_cmt(curve_df: pd.DataFrame, day_count: float = 365.0, coupons_per_year: int = 2) -> pd.DataFrame:
    """
    Input (single time_index slice): ['days_to_maturity','par_yield'] in decimal.
    Policy:
      - T < 365d: money-market zero (Act/365)
      - T >= 365d: par yields with semiannual coupons (Act/365), price=100
    Output: ['days_to_maturity','zero_rate'] in percent.
    """
    df = curve_df.copy()
    df["days_to_maturity"] = pd.to_numeric(df["days_to_maturity"], errors="coerce").astype(float)
    df["par_yield"] = pd.to_numeric(df["par_yield"], errors="coerce")
    df = df.dropna().sort_values("days_to_maturity")

    pillars: dict[float, float] = {0.0: 1.0}

    for _, r in df.iterrows():
        T = float(r["days_to_maturity"])
        y = float(r["par_yield"])
        if T < 365.0:
            df_T = 1.0 / (1.0 + y * (T / day_count))
            pillars[T] = max(1e-12, min(df_T, 1.0))
            continue
        step = day_count / coupons_per_year
        k = int(np.floor(T / step))
        coupon_times = list(step * np.arange(1, k + 1))
        if coupon_times and np.isclose(coupon_times[-1], T, atol=1e-8):
            coupon_times = coupon_times[:-1]
        last_coupon = coupon_times[-1] if coupon_times else 0.0
        alpha = step / day_count
        alpha_T = (T - last_coupon) / day_count
        c_amt = (y / coupons_per_year) * 100.0
        final_cf = 100.0 + y * alpha_T * 100.0

        df_func = _build_loglinear_df_func(pillars)
        S = max(pillars.keys())

        pre_known_sum, between = 0.0, []
        for t in coupon_times:
            if t <= S + 1e-12: pre_known_sum += c_amt * df_func(t)
            else: between.append(t)

        if T <= S + 1e-12:
            df_T = df_func(T)
        else:
            df_T = _solve_df_T_loglinear(
                price=100.0, pre_known_sum=pre_known_sum, df_S=df_func(S), S=S, T=T,
                coupon_dates_between_S_T=between, coupon_amt=c_amt, final_cf=final_cf
            )
        df_T = max(1e-12, min(df_T, df_func(S)))
        pillars[T] = df_T

    tenors = np.array(sorted(pillars.keys())); dfs = np.array([pillars[t] for t in tenors])
    mask = tenors > 0
    T = tenors[mask]; DF = dfs[mask]
    zeros = ((1.0 / DF) - 1.0) * (day_count / T)
    return pd.DataFrame({"days_to_maturity": T.astype(int), "zero_rate": zeros}).sort_values("days_to_maturity").reset_index(drop=True)

def bootstrap_cmt_curve(update_statistics, curve_unique_identifier: str, base_node_curve_points:APIDataNode):
    """
    For each time_index:
      1) Reads node data from `base_node_curve_points` since the last_update.
      2) Bootstraps the zero curve using `bootstrap_zero_from_cmt`.
      3) Returns one dataframe with:
         - MultiIndex ("time_index", "unique_identifier")
         - Column "curve": dict[days_to_maturity] → zero_rate (percent)
    """
    last_update = update_statistics.asset_time_statistics[curve_unique_identifier]

    cmt_input_df = base_node_curve_points.get_df_between_dates(
        start_date=last_update,
        great_or_equal=True
    )

    if cmt_input_df.empty:
        return pd.DataFrame()

    results = []

    for time_index, curve_df in cmt_input_df.groupby("time_index"):
        curve_df = curve_df.copy()
        curve_df["days_to_maturity"] = pd.to_numeric(curve_df["days_to_maturity"], errors="coerce")
        curve_df["par_yield"] = pd.to_numeric(curve_df["par_yield"], errors="coerce")

        try:
            zero_df = bootstrap_zero_from_cmt(curve_df)
        except Exception as e:
            raise e

        zero_df.insert(0, "time_index", time_index)
        zero_df.insert(1, "unique_identifier", curve_unique_identifier)
        results.append(zero_df)

    final_df = pd.concat(results, ignore_index=True)

    grouped = (
        final_df.groupby(["time_index", "unique_identifier"])
        .apply(lambda g: g.set_index("days_to_maturity")["zero_rate"].to_dict())
        .rename("curve")
        .reset_index()
        .set_index(["time_index", "unique_identifier"])
    )

    return grouped
