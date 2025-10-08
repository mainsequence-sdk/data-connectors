
from mainsequence.client import Constant as _C

from .builders import *


constants_to_seed = dict(
    REFERENCE_RATE__TIIE_28="TIIE_28",
    REFERENCE_RATE__TIIE_91="TIIE_91",
    REFERENCE_RATE__TIIE_182="TIIE_182",
    REFERENCE_RATE__TIIE_OVERNIGHT="TIIE_OVERNIGHT",

    REFERENCE_RATE__CETE_28="CETE_28",
    REFERENCE_RATE__CETE_91="CETE_91",
    REFERENCE_RATE__CETE_182="CETE_182",

    #curves
    ZERO_CURVE__BANXICO_M_BONOS_OTR = "BANXICO_M_BONOS_OTR",
)

_C.create_constants_if_not_exist(constants_to_seed)




TIIE_FIXING_BUILD_MAP = {
    _C.get_value(name="REFERENCE_RATE__TIIE_OVERNIGHT"): update_tiie_fixings,
    _C.get_value(name="REFERENCE_RATE__TIIE_28"): update_tiie_fixings,
    _C.get_value(name="REFERENCE_RATE__TIIE_91"): update_tiie_fixings,
    _C.get_value(name="REFERENCE_RATE__TIIE_182"): update_tiie_fixings,

}
CETE_FIXING_BUILD_MAP = {
    _C.get_value(name="REFERENCE_RATE__CETE_28"): update_cete_fixing,
    _C.get_value(name="REFERENCE_RATE__CETE_91"): update_cete_fixing,
    _C.get_value(name="REFERENCE_RATE__CETE_182"): update_cete_fixing,

                         }
