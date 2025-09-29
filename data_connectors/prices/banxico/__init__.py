
from mainsequence.client import Constant as _C

from .builders import *


TIIE_FIXING_BUILD_MAP = {
    _C.get_value(name="TIIE_OVERNIGHT_UID"): update_tiie_fixings,
    _C.get_value(name="TIIE_28_UID"): update_tiie_fixings,
    _C.get_value(name="TIIE_91_UID"): update_tiie_fixings,
    _C.get_value(name="TIIE_182_UID"): update_tiie_fixings,

}
CETE_FIXING_BUILD_MAP = {
    _C.get_value(name="CETE_28_UID"): update_cete_fixing,
    _C.get_value(name="CETE_91_UID"): update_cete_fixing,
    _C.get_value(name="CETE_182_UID"): update_cete_fixing,

                         }
