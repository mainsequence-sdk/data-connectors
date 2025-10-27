from functools import partial
from .utils import _update_fred_fixings
from mainsequence.client import Constant as _C



constants_to_seed = dict(
    REFERENCE_RATE__USD_SOFR="SOFR",
    REFERENCE_RATE__USD_EFFR="EFFR",
    REFERENCE_RATE__USD_OBFR="OBFR",

)

_C.create_constants_if_not_exist(constants_to_seed)

USD_SOFR=_C.get_value(name="REFERENCE_RATE__USD_SOFR")
USD_EFFR=_C.get_value(name="REFERENCE_RATE__USD_EFFR")
USD_OBFR=_C.get_value(name="REFERENCE_RATE__USD_OBFR")

USD_FRED_FIXING_BUILD_MAP = {
    USD_SOFR: partial(
        _update_fred_fixings,
        id_map={USD_SOFR: USD_SOFR},
    ),
    USD_EFFR: partial(
        _update_fred_fixings,
        id_map={USD_EFFR: USD_EFFR},
    ),
    USD_OBFR: partial(
        _update_fred_fixings,
        id_map={USD_OBFR: USD_OBFR},
    ),


}

