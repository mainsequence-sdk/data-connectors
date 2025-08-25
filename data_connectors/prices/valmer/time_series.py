from mainsequence.client.models_tdag import Artifact
from datetime import timedelta
from typing import Union, Dict

import pandas as pd

from mainsequence.client import Asset
from mainsequence.client.utils import DoesNotExist
from mainsequence.tdag import DataNode
import mainsequence.client as msc

from mainsequence.virtualfundbuilder.utils import TIMEDELTA
import numpy as np
import pandas.api.types as ptypes


class ImportValmer(DataNode):
    def __init__(
            self,
            artifact_name: str,
            bucket_name: str,
            *args, **kwargs
    ):
        self.artifact_name = artifact_name
        self.bucket_name = bucket_name
        self.artifact_data = None
        super().__init__(*args, **kwargs)

    _ARGS_IGNORE_IN_STORAGE_HASH = ["bucket_name", "artifact_name"]

    def maximum_forward_fill(self):
        return timedelta(days=1) - TIMEDELTA

    def get_explanation(self):
        explanation = (
            "### Data From Valmer\n\n"
        )
        return explanation

    def _get_artifact_data(self):
        if self.artifact_data is None:
            source_artifact = Artifact.get(bucket__name=self.bucket_name, name=self.artifact_name)
            self.artifact_data = pd.read_csv(source_artifact.content, encoding='latin1')

        return self.artifact_data

    def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:
        return {}

    def get_asset_list(self) -> Union[None, list]:
        source_data = self._get_artifact_data()

        asset_columns = ["unique_identifier", "isin"]
        source_data['unique_identifier'] = source_data["Instrumento"]
        source_data['isin'] = source_data['Isin'].apply(lambda x: None if pd.isna(x) else x)
        source_data = source_data.drop(columns="Isin")
        assets = []

        # insert assets (takes a long time) TODO implement batch update
        # data = source_data[asset_columns].to_dict('records')
        # for i, asset_data in enumerate(data):
        #     if i % 500 == 0:
        #         self.logger.info(f"Registered {i} assets out of {len(data)}")
        #     snapshot = dict(name=asset_data["unique_identifier"], ticker=asset_data["unique_identifier"])
        #     asset = msc.Asset.get_or_register_custom_asset(unique_identifier=asset_data["unique_identifier"], snapshot=snapshot, isin=asset_data["isin"])
        #     assets.append(asset)

        for i in range(0, len(source_data), 500):
            assets_batch = Asset.filter(unique_identifier__in=source_data["unique_identifier"].iloc[i:i+500].to_list())
            assets += assets_batch

        self.source_data = source_data
        return assets

    def _get_column_metadata(self):
        from mainsequence.client.models_tdag import ColumnMetaData
        columns_metadata = [ColumnMetaData(column_name="instrumento",
                                           dtype="str",
                                           label="Instrumento",
                                           description=(
                                               "Unique identifier provided by Valmer; it’s a composition of the "
                                               "columns `tv_emisora_serie`, and is also used as a ticker for custom "
                                               "assets in Valmer."
                                           )
                                           ),
                            ColumnMetaData(column_name="moneda",
                                           dtype="str",
                                           label="Moneda",
                                           description=(
                                               "Correspondes to Valmer code for curries be aware this may not match Figi Currency assets"
                                           )
                                           ),

                            ]
        return columns_metadata

    def update(self):
        source_data = self.source_data

        assert source_data is not None, "Source data is not available"

        source_data.rename(columns={"Fecha": "time_index"}, inplace=True)
        source_data['time_index'] = pd.to_datetime(source_data['time_index'], utc=True)

        # make columns lower case
        for col in source_data.columns:
            source_data.rename(columns={col: col.lower()}, inplace=True)

        price_series = source_data['preciosucio']

        # Map to OHLC columns
        source_data['open'] = price_series
        source_data['close'] = price_series
        source_data['high'] = price_series
        source_data['low'] = price_series
        source_data['volume'] = 0 # TODO placeholder
        source_data['open_time'] = source_data['time_index']
        source_data['open_time'] = source_data['open_time'].astype(np.int64)

        source_data.set_index(["time_index", "unique_identifier"], inplace=True)

        source_data = self.update_statistics.filter_df_by_latest_value(source_data)

        return source_data

    def get_table_metadata(self) -> msc.TableMetaData:
        TS_ID = "vector_de_precios_valmer"
        meta = msc.TableMetaData(
            identifier=TS_ID,
            description=f"Vector de Precios Valmer",
            data_frequency_id=msc.DataFrequency.one_month,
        )

        return meta

if __name__ == "__main__":
    ts = ImportValmer(
        bucket_name="Vector de precios",
        artifact_name="Vector_20250430.csv",
    )

    ts.run(
        debug_mode=True,
        force_update=True,
    )