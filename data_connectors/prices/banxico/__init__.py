


from .builders import *

import  mainsequence.instruments.settings as instrument_settings

TIIE_FIXING_BUILD_MAP = {
    instrument_settings.TIIE_OVERNIGHT_UID: update_tiie_fixings,
    instrument_settings.TIIE_28_UID: update_tiie_fixings,
    instrument_settings.TIIE_91_UID: update_tiie_fixings,
    instrument_settings.TIIE_182_UID: update_tiie_fixings,

}
CETE_FIXING_BUILD_MAP = {instrument_settings.CETE_28_UID: update_cete_fixing,
                         instrument_settings.CETE_91_UID: update_cete_fixing,
                         instrument_settings.CETE_182_UID: update_cete_fixing

                         }
