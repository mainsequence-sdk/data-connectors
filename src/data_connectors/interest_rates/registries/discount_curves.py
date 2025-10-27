# data_connectors/interest_rates/registries/discount_curves.py
from __future__ import annotations

from typing import Callable, Dict, Mapping

# Provider builders / constants
from src.data_connectors.prices.valmer.utils import build_tiie_valmer
from src.data_connectors.prices.banxico import boostrap_mbono_curve
from mainsequence.client import Constant as _C

# UST CMT (Polygon) — keep source-specific UID in its own settings module
from src.data_connectors.prices.polygon.builders import bootstrap_cmt_curve

def _merge_unique(*maps: Mapping[str, Callable]) -> Dict[str, Callable]:
    out: Dict[str, Callable] = {}
    for m in maps:
        for k, v in m.items():
            if k in out and out[k] is not v:
                raise ValueError(f"Duplicate registry key with different builder: {k}")
            out[k] = v
    return out

# Base maps per source (explicit so adding/removing sources is easy)


"""
signtaure for each zero curfe function should be like the one bellow
def bootstrap_cmt_curve(update_statistics, curve_unique_identifier: str, base_node_curve_points:APIDataNode):
and should 
 Returns one dataframe with:
         - MultiIndex ("time_index", "unique_identifier")
         - Column "curve": dict[days_to_maturity] → zero_rate (percent)

where unique_identifier is the name of this zero_curve, we recommend that you 
build a constant in the backend to retrieve this specific curve
"""
_VALMER_CURVES = {
    _C.get_value(name="ZERO_CURVE__VALMER_TIIE_28"): build_tiie_valmer,
}
_BANXICO_CURVES = {
      _C.get_value(name="ZERO_CURVE__BANXICO_M_BONOS_OTR"): boostrap_mbono_curve,
}
_POLYGON_CURVES = {
    _C.get_value(name="ZERO_CURVE__UST_CMT_ZERO_CURVE_UID"): bootstrap_cmt_curve,
}

# ---- Public, aggregated registry ----
DISCOUNT_CURVE_BUILD_REGISTRY: Dict[str, Callable] = _merge_unique(
    _VALMER_CURVES, _BANXICO_CURVES, _POLYGON_CURVES
)
