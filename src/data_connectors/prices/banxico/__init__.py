
from mainsequence.client import Constant as _C
from .builders import *
from .settings import *





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
