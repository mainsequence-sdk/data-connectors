# data_connectors/interest_rates/registries/discount_curves.py
from __future__ import annotations

from typing import Callable, Dict, Mapping

# Provider builders / constants
from data_connectors.prices.valmer.utils import build_tiie_valmer
from data_connectors.prices.banxico import boostrap_mbono_curve
from mainsequence.instruments.settings import TIIE_28_ZERO_CURVE, M_BONOS_ZERO_OTR

# UST CMT (Polygon) — keep source-specific UID in its own settings module
from data_connectors.prices.polygon.builders import bootstrap_zero_from_cmt
from data_connectors.prices.polygon.settings import UST_CMT_ZERO_CURVE_UID

def _merge_unique(*maps: Mapping[str, Callable]) -> Dict[str, Callable]:
    out: Dict[str, Callable] = {}
    for m in maps:
        for k, v in m.items():
            if k in out and out[k] is not v:
                raise ValueError(f"Duplicate registry key with different builder: {k}")
            out[k] = v
    return out

# Base maps per source (explicit so adding/removing sources is easy)
_VALMER_CURVES = {
    TIIE_28_ZERO_CURVE: build_tiie_valmer,
}
_BANXICO_CURVES = {
    M_BONOS_ZERO_OTR: boostrap_mbono_curve,
}
_POLYGON_CURVES = {
    UST_CMT_ZERO_CURVE_UID: bootstrap_zero_from_cmt,
}

# ---- Public, aggregated registry ----
DISCOUNT_CURVE_BUILD_REGISTRY: Dict[str, Callable] = _merge_unique(
    _VALMER_CURVES, _BANXICO_CURVES, _POLYGON_CURVES
)
