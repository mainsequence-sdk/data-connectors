from functools import partial
from .utils import _update_fred_fixings

USD_FRED_FIXING_BUILD_MAP = {
    # SOFR (Secured Overnight Financing Rate, daily) — commonly used USD swap reference
    "USD_SOFR": partial(
        _update_fred_fixings,
        id_map={"USD_SOFR": "SOFR"},
        instrument_label="SOFR",
    ),

    # (Optional examples — enable if you want more USD references)
    # "USD_EFFR": partial(_update_fred_fixings, id_map={"USD_EFFR": "EFFR"}, instrument_label="EFFR"),
    # "USD_OBFR": partial(_update_fred_fixings, id_map={"USD_OBFR": "OBFR"}, instrument_label="OBFR"),
    # Term SOFR (if you decide to add): e.g., "SOFR1MD", "SOFR3MD" — confirm exact FRED IDs before enabling.
}
