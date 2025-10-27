from typing import Dict, Tuple, Optional, Union, List
import mainsequence.client as msc
from mainsequence.client import Constant as _C


# -----------------------------
# Gurantee constants on any workflows
# -----------------------------

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


# -----------------------------
# Banxico series catalogs
# -----------------------------


BANXICO_SIE_BASE = "https://www.banxico.org.mx/SieAPIRest/service/v1"
CETES_SERIES: Dict[str, Dict[str, str]] = {
    # CETES (no coupon series in Banxico vector)

    "28d":  {"plazo": "SF45422", "precio_limpio": "SF45438", "precio_sucio": "SF45439"},
    "91d":  {"plazo": "SF45423", "precio_limpio": "SF45440", "precio_sucio": "SF45441"},
    "182d": {"plazo": "SF45424", "precio_limpio": "SF45442", "precio_sucio": "SF45443"},
    "364d": {"plazo": "SF45425", "precio_limpio": "SF45444", "precio_sucio": "SF45445"},
    # keep if you also track this bucket
    "2y":   {"plazo": "SF349886", "precio_limpio": "SF349887", "precio_sucio": "SF349888"},
}

BONOS_SERIES: Dict[str, Dict[str, str]] = {
    # BONOS M (Mbonos)
    "0-3y":   {"plazo": "SF45427", "precio_limpio": "SF45448", "precio_sucio": "SF45449", "cupon_vigente": "SF45475"},
    "3-5y":   {"plazo": "SF45428", "precio_limpio": "SF45450", "precio_sucio": "SF45451", "cupon_vigente": "SF45476"},
    "5-7y":   {"plazo": "SF45429", "precio_limpio": "SF45452", "precio_sucio": "SF45453", "cupon_vigente": "SF45477"},
    "7-10y":  {"plazo": "SF45430", "precio_limpio": "SF45454", "precio_sucio": "SF45455", "cupon_vigente": "SF45478"},
    "10-20y": {"plazo": "SF45431", "precio_limpio": "SF45456", "precio_sucio": "SF45457", "cupon_vigente": "SF45479"},
    "20-30y": {"plazo": "SF60720", "precio_limpio": "SF60721", "precio_sucio": "SF60722", "cupon_vigente": "SF60723"},
}

FUNDING_RATES={"1d":"SF331451"}

FONDEO_GUVBERNAMENTAL={"":"SF43774"}



BANXICO_TARGET_RATE="BANXICO_TARGET_RATE"
ON_THE_RUN_DATA_NODE_TABLE_NAME="banxico_1d_otr_mxn"

MONEY_MARKET_RATES={BANXICO_TARGET_RATE:"SF61745",
                    }


TIIE_FIXING_ID_MAP={
                    _C.get_value(name="REFERENCE_RATE__TIIE_OVERNIGHT"):"SF331451",
                    _C.get_value(name="REFERENCE_RATE__TIIE_28"):"SF43783",
                    _C.get_value(name="REFERENCE_RATE__TIIE_91"):"SF43878",
                    _C.get_value(name="REFERENCE_RATE__TIIE_182"):"SF111916"
                     }

CETE_FIXING_ID_MAP={ _C.get_value(name="REFERENCE_RATE__CETE_28"):"SF45470",
                     _C.get_value(name="REFERENCE_RATE__CETE_91"):"SF45471",
                     _C.get_value(name="REFERENCE_RATE__CETE_182"):"SF45472"

                    }






