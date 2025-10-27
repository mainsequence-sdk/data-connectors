# data_connectors/interest_rates/registries/fixing_rates.py
from __future__ import annotations

from typing import Callable, Dict, Mapping

# Banxico sources (MXN fixings)
from data_connectors.prices.banxico import TIIE_FIXING_BUILD_MAP, CETE_FIXING_BUILD_MAP
from data_connectors.prices.fred import USD_FRED_FIXING_BUILD_MAP



def _merge_unique(*maps: Mapping[str, Callable]) -> Dict[str, Callable]:
    out: Dict[str, Callable] = {}
    for m in maps:
        for k, v in m.items():
            if k in out and out[k] is not v:
                raise ValueError(f"Duplicate registry key with different builder: {k}")
            out[k] = v
    return out

# ---- Public, aggregated registry ----
FIXING_RATE_BUILD_REGISTRY: Dict[str, Callable] = _merge_unique(
    TIIE_FIXING_BUILD_MAP,
    CETE_FIXING_BUILD_MAP,
USD_FRED_FIXING_BUILD_MAP,
)
