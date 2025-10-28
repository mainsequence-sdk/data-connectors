from src.data_connectors.interest_rates.nodes import DiscountCurves, CurveConfig
from mainsequence.client import Constant as _C
from src.data_connectors.prices.banxico.settings import ON_THE_RUN_DATA_NODE_TABLE_NAME
from src.data_connectors.prices.polygon.settings import UST_CMT_YIELDS_TABLE_UID
from src.data_connectors.prices.banxico.data_nodes import BanxicoMXNOTR
from src.data_connectors.prices.polygon.data_nodes import PolygonUSTCMTYields

def main():



    ts = BanxicoMXNOTR()
    ts.run(debug_mode=True, force_update=True)


    try:
        data_node = PolygonUSTCMTYields()
        data_node.run(debug_mode=True, force_update=True)
    except Exception as e:
        data_node.logger.exception(e)


    configs = [
        CurveConfig(
            unique_identifier=_C.get_value("ZERO_CURVE__VALMER_TIIE_28"),
            name="Discount Curve TIIE 28 Mexder Valmer",
        ),
        CurveConfig(
            unique_identifier=_C.get_value("ZERO_CURVE__BANXICO_M_BONOS_OTR"),
            name="Discount Curve M Bonos Banxico Boostrapped",

            curve_points_dependecy_data_node_uid=ON_THE_RUN_DATA_NODE_TABLE_NAME,
        ),
        CurveConfig(
            unique_identifier=_C.get_value("ZERO_CURVE__UST_CMT_ZERO_CURVE_UID"),
            name="Discount Curve UST Bootstrapped",
            curve_points_dependecy_data_node_uid=UST_CMT_YIELDS_TABLE_UID,
        ),
    ]

    for cfg in configs:
        node = DiscountCurves(curve_config=cfg)
        node.run(debug_mode=True, force_update=True)


if __name__ == "__main__":
    main()
