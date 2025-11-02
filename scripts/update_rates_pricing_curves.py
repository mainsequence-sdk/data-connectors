from src.data_connectors.interest_rates.nodes import DiscountCurves, CurveConfig
from mainsequence.client import Constant as _C
from src.data_connectors.prices.banxico.settings import ON_THE_RUN_DATA_NODE_TABLE_NAME
from src.data_connectors.prices.polygon.settings import UST_CMT_YIELDS_TABLE_UID
from src.data_connectors.prices.banxico.data_nodes import BanxicoMXNOTR
from src.data_connectors.prices.polygon.data_nodes import PolygonUSTCMTYields
from src.data_connectors.interest_rates.nodes import FixingRatesNode, FixingRateConfig, RateConfig


def main():


    #
    ts = BanxicoMXNOTR()
    ts.run(debug_mode=True, force_update=True)


    try:
        data_node = PolygonUSTCMTYields()
        data_node.run(debug_mode=True, force_update=True)
    except Exception as e:
        data_node.logger.exception(e)

    #run fixingins
    fixing_config = FixingRateConfig(rates_config_list=[
        RateConfig(unique_identifier=_C.get_value("REFERENCE_RATE__TIIE_OVERNIGHT"),
                   name=f"Interbank Equilibrium Interest Rate (TIIE) {_C.get_value('REFERENCE_RATE__TIIE_OVERNIGHT')}"),
        RateConfig(unique_identifier=_C.get_value("REFERENCE_RATE__TIIE_28"),
                   name=f"Interbank Equilibrium Interest Rate (TIIE) {_C.get_value('REFERENCE_RATE__TIIE_28')}"),
        RateConfig(unique_identifier=_C.get_value("REFERENCE_RATE__TIIE_91"),
                   name=f"Interbank Equilibrium Interest Rate (TIIE) {_C.get_value('REFERENCE_RATE__TIIE_91')}"),
        RateConfig(unique_identifier=_C.get_value("REFERENCE_RATE__TIIE_182"),
                   name=f"Interbank Equilibrium Interest Rate (TIIE) {_C.get_value('REFERENCE_RATE__TIIE_182')}"),
        RateConfig(unique_identifier=_C.get_value("REFERENCE_RATE__CETE_28"),
                   name=f"CETE 28 days {_C.get_value('REFERENCE_RATE__CETE_28')}"),
        RateConfig(unique_identifier=_C.get_value("REFERENCE_RATE__CETE_91"),
                   name=f"CETE 91 days {_C.get_value('REFERENCE_RATE__CETE_91')}"),
        RateConfig(unique_identifier=_C.get_value("REFERENCE_RATE__CETE_182"),
                   name=f"CETE 182 days {_C.get_value('REFERENCE_RATE__CETE_182')}"),

    ]

    )

    ts = FixingRatesNode(rates_config=fixing_config)
    ts.run(debug_mode=True, force_update=True)


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
            unique_identifier=_C.get_value("ZERO_CURVE__POLYGON_UST_CMT_ZERO_CURVE_UID"),
            name="Discount Curve UST Bootstrapped",
            curve_points_dependecy_data_node_uid=UST_CMT_YIELDS_TABLE_UID,
        ),
    ]

    for cfg in configs:
        node = DiscountCurves(curve_config=cfg)
        node.run(force_update=True)


if __name__ == "__main__":
    main()
