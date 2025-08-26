from mainsequence.client.models_tdag import Artifact
from datetime import timedelta
from typing import Union, Dict

import pandas as pd

from mainsequence.client import Asset
from mainsequence.client.utils import DoesNotExist
from mainsequence.tdag import DataNode
import mainsequence.client as msc

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
        return timedelta(days=1) - pd.Timedelta("5ms")

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

        source_data['unique_identifier'] = source_data["Instrumento"]
        source_data['isin'] = source_data['Isin'].apply(lambda x: None if pd.isna(x) else x)
        asset_list = []

        batch_size = 500
        for i in range(0, len(source_data), batch_size):

            # Prepare the payload
            assets_payload = []
            for _, row in source_data.iloc[i:i+batch_size][["unique_identifier", "isin"]].iterrows():
                snapshot = {
                    "name": row["unique_identifier"],
                    "ticker": row["unique_identifier"]
                }
                payload_item = {
                    "unique_identifier": row["unique_identifier"],
                    "snapshot": snapshot,
                    "isin": row["isin"]
                }
                assets_payload.append(payload_item)

            self.logger.info(f"Registering {i} to {i+batch_size-1} out of {len(source_data)} assets...")
            try:
                assets = msc.Asset.batch_get_or_register_custom_assets(assets_payload)
                asset_list += assets
            except Exception as e:
                self.logger.error(f"Failed to register assets in batch: {e}")
                raise

        self.source_data = source_data.drop(columns="Isin")
        return asset_list

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