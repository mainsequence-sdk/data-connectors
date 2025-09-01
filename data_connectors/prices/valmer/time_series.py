from mainsequence.client.models_tdag import Artifact
from datetime import timedelta
from typing import Union, Dict
import io

import pandas as pd

from mainsequence.client import Asset
from mainsequence.client.utils import DoesNotExist
from mainsequence.tdag import DataNode
import mainsequence.client as msc

import numpy as np
import pandas.api.types as ptypes
from tqdm import tqdm


class ImportValmer(DataNode):
    def __init__(
            self,
            bucket_name: str,
            *args, **kwargs
    ):
        self.bucket_name = bucket_name
        self.artifact_data = None
        super().__init__(*args, **kwargs)

    _ARGS_IGNORE_IN_STORAGE_HASH = ["bucket_name"]

    def maximum_forward_fill(self):
        return timedelta(days=1) - pd.Timedelta("5ms")

    def get_explanation(self):
        explanation = (
            "### Data From Valmer\n\n"
            "This node reads all files from the specified Valmer bucket, "
            "combines them, and processes them in a single operation."
        )
        return explanation

    def _get_artifact_data(self):
        """
        Reads all artifacts from the bucket, sorts them, and concatenates them into a single DataFrame.
        Single-threaded, but much faster via column selection, explicit dtypes, and proper engines.
        """
        if self.artifact_data is not None:
            return self.artifact_data

        artifacts = Artifact.filter(bucket__name=self.bucket_name)
        sorted_artifacts = sorted(artifacts, key=lambda artifact: artifact.name)

        self.logger.info(f"Found {len(sorted_artifacts)} artifacts to process in bucket '{self.bucket_name}'.")

        # Read only what we need to build Instrumento + price + date
        excel_usecols = ["TIPO VALOR", "EMISORA", "SERIE", "PRECIO SUCIO", "FECHA"]
        excel_dtypes = {
            "TIPO VALOR": "string",
            "EMISORA": "string",
            "SERIE": "string",
            "PRECIO SUCIO": "float64",  # parse numeric directly
            "FECHA": "string",  # parse later in update()
        }

        frames = []
        for artifact in tqdm(sorted_artifacts[100:]):
            name_l = artifact.name.lower()
            content = artifact.content
            buf = content

            df = None
            if name_l.endswith(".xls"):
                # xlrd is fastest for legacy xls if available, else fall back to openpyxl
                try:
                    import xlrd  # noqa: F401
                    df = pd.read_excel(
                        buf, engine="xlrd",
                        usecols=excel_usecols, dtype=excel_dtypes
                    )
                except Exception:
                    df = pd.read_excel(
                        buf, engine="openpyxl",
                        usecols=excel_usecols, dtype=excel_dtypes
                    )
            elif name_l.endswith(".csv"):
                # Use pyarrow engine when available; otherwise let pandas choose.
                try:
                    df = pd.read_csv(buf, encoding="latin1", engine="pyarrow")
                except Exception:
                    df = pd.read_csv(buf, encoding="latin1", low_memory=False)
            else:
                self.logger.info(f"Skipping unsupported file type: {artifact.name}")
                continue

            if df is None or df.empty:
                continue

            # Normalize columns once per file (cheap & vectorized)
            if {"TIPO VALOR", "EMISORA", "SERIE"}.issubset(df.columns):
                # build Instrumento without Python-level loops
                instrumento = (
                    df["TIPO VALOR"].astype("string")
                    .str.cat(df["EMISORA"].astype("string"), sep="_")
                    .str.cat(df["SERIE"].astype("string"), sep="_")
                )
                df = df.rename(columns={"PRECIO SUCIO": "preciosucio", "FECHA": "Fecha"})
                # keep only the columns we actually use downstream
                keep_cols = ["preciosucio", "Fecha"]
                df = pd.DataFrame({
                    "Instrumento": instrumento,
                    "preciosucio": df["preciosucio"],
                    "Fecha": df["Fecha"],
                })[["Instrumento", "preciosucio", "Fecha"]]
            frames.append(df)

        if not frames:
            raise ValueError(f"No valid .xls/.xlsx/.xlsb or .csv files found in bucket '{self.bucket_name}'.")

        # Fast concat; avoid sorting and extra copies
        try:
            self.artifact_data = pd.concat(frames, ignore_index=True, sort=False, copy=False)
        except TypeError:
            # older pandas without copy= parameter
            self.artifact_data = pd.concat(frames, ignore_index=True, sort=False)

        self.logger.info(f"Combined all artifacts into a single DataFrame with {len(self.artifact_data)} rows.")
        return self.artifact_data

    def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:
        return {}

    def get_asset_list(self) -> Union[None, list]:
        """
        Processes and registers each unique asset only once from the combined DataFrame.
        """
        source_data = self._get_artifact_data()

        source_data['unique_identifier'] = source_data["Instrumento"]
        source_data = source_data[source_data['unique_identifier'].notna()].copy()

        # Keep a copy of the full data for the update() method
        self.source_data = source_data.copy()

        # Get a list of unique identifiers to avoid processing duplicates
        unique_identifiers = source_data['unique_identifier'].unique().tolist()
        self.logger.info(f"Found {len(unique_identifiers)} unique assets to process.")

        asset_list = []
        batch_size = 500

        # Loop through the list of unique identifiers in batches
        for i in range(0, len(unique_identifiers), batch_size):
            batch_identifiers = unique_identifiers[i:i + batch_size]
            assets_payload = []

            # Create a payload for each unique identifier in the current batch
            for identifier in batch_identifiers:
                snapshot = {
                    "name": identifier,
                    "ticker": identifier
                }
                payload_item = {
                    "unique_identifier": identifier,
                    "snapshot": snapshot,
                }
                assets_payload.append(payload_item)

            if not assets_payload:
                continue

            self.logger.info(f"Getting or registering assets in batch {i // batch_size + 1}/{len(unique_identifiers)//batch_size}...")
            try:
                assets = msc.Asset.batch_get_or_register_custom_assets(assets_payload)
                asset_list.extend(assets)
            except Exception as e:
                self.logger.error(f"Failed to process asset batch: {e}")
                raise

        return asset_list

    def _get_column_metadata(self):
        # This metadata should reflect the columns you intend to save to the database.
        from mainsequence.client.models_tdag import ColumnMetaData
        return [
            ColumnMetaData(column_name="open", dtype="float", label="Open"),
            ColumnMetaData(column_name="high", dtype="float", label="High"),
            ColumnMetaData(column_name="low", dtype="float", label="Low"),
            ColumnMetaData(column_name="close", dtype="float", label="Close"),
            ColumnMetaData(column_name="volume", dtype="float", label="Volume"),
        ]

    def update(self):
        source_data = self.source_data
        assert source_data is not None, "Source data is not available"

        source_data.rename(columns={"Fecha": "time_index"}, inplace=True)
        source_data['time_index'] = pd.to_datetime(source_data['time_index'], format='%Y%m%d', utc=True)

        # Note: This line drastically reduces the number of columns.
        # Ensure your _get_column_metadata reflects the final schema.
        source_data = source_data[["time_index", "unique_identifier", "preciosucio"]]

        # Clean column names after subsetting
        source_data.columns = [col.lower().replace(' ', '_').replace('.', '_') for col in source_data.columns]

        price_series = source_data['preciosucio']

        # Map to OHLC columns
        source_data['open'] = price_series
        source_data['close'] = price_series
        source_data['high'] = price_series
        source_data['low'] = price_series
        source_data['volume'] = 0
        source_data['open_time'] = source_data['time_index'].astype(np.int64) // 10 ** 9

        # Drop the original price column as it's now represented by OHLC
        source_data = source_data.drop(columns=['preciosucio'])

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
    BUCKET_NAME = "Vector de precios"

    # Initialize the DataNode once with the bucket name
    ts = ImportValmer(
        bucket_name=BUCKET_NAME,
    )

    # Run the process for all files in the bucket
    ts.run(
        debug_mode=True,
        force_update=True,
    )