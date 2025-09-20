import pandas as pd
import requests
from datetime import datetime,timedelta
import io


def build_tiie_valmer(update_statistics,curve_unique_identifier:str,base_node_curve_points=None):
    url = "https://valmer.com.mx/VAL/Web_Benchmarks/MEXDERSWAP_IRSTIIEPR.csv"
    response = requests.get(url)
    response.raise_for_status()

    # Load CSV directly from bytes, using correct encoding
    names = ["id", "curve_name", "asof_yyMMdd", "idx", "zero_rate"]
    # STRICT: comma-separated, headerless, exactly these six columns
    df = pd.read_csv(io.BytesIO(response.content), header=None, names=names, sep=",", engine="c",
                     encoding="latin1",
                     dtype=str)
    # pick a rate column

    df["asof_yyMMdd"] = pd.to_datetime(df["asof_yyMMdd"], format="%y%m%d")
    df["asof_yyMMdd"] = df["asof_yyMMdd"].dt.tz_localize('UTC')

    base_dt = df["asof_yyMMdd"].iloc[0] - timedelta(days=1)

    if update_statistics.asset_time_statistics[curve_unique_identifier] >= base_dt:
        return pd.DataFrame()

    df["idx"] = df["idx"].astype(int)
    df["days_to_maturity"] = (df["asof_yyMMdd"] - base_dt).dt.days
    df["zero_rate"] = df["zero_rate"].astype(float) / 100

    df["time_index"] = base_dt
    df["unique_identifier"] = curve_unique_identifier

    grouped = (
        df.groupby(["time_index", "unique_identifier"])
        .apply(lambda g: g.set_index("days_to_maturity")["zero_rate"].to_dict())
        .rename("curve")
        .reset_index()
    )



    # 3. Final index and structure (your original code)
    grouped = grouped.set_index(["time_index", "unique_identifier"])

    return grouped