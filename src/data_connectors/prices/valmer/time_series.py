import re
from datetime import timedelta
from typing import Union, Dict
import pandas as pd
from tqdm import tqdm
import mainsequence.client as msc
import numpy as np
from mainsequence.client.models_tdag import Artifact
from mainsequence.tdag import DataNode
import io
import requests
import pytz

from src.data_connectors.prices.valmer.instrument_build import build_qll_bond_from_row,normalize_column_name,get_instrument_conventions

UTC = pytz.UTC
import json
from typing import Dict, Any
import gzip

import base64
import QuantLib as ql
import os

class MexDerTIIE28Zero(DataNode):
    """Download and return daily MEXDERSWAP_IRSTIIEPR swap rates from valmer.com.mx

        Output:
            - Index: DatetimeIndex named 'time_index' (UTC)
            - Columns: cleaned from the CSV (lowercase, <=63 chars, no datetime columns)
        """
    @staticmethod
    def compress_curve_to_string(curve_dict: Dict[Any, Any]) -> str:
        """
        Serializes, compresses, and encodes a curve dictionary into a single,
        transport-safe text string.

        Pipeline: Dict -> JSON -> Gzip (binary) -> Base64 (text)

        Args:
            curve_dict: The Python dictionary representing the curve.

        Returns:
            A Base64-encoded string of the Gzipped JSON.
        """
        # 1. Serialize the dictionary to a compact JSON string, then encode to bytes
        json_bytes = json.dumps(curve_dict, separators=(',', ':')).encode('utf-8')

        # 2. Compress the JSON bytes using the universal Gzip standard
        compressed_bytes = gzip.compress(json_bytes)

        # 3. Encode the compressed binary data into a text-safe Base64 string
        base64_bytes = base64.b64encode(compressed_bytes)

        # 4. Decode the Base64 bytes into a final ASCII string for storage/transport
        return base64_bytes.decode('ascii')
    @staticmethod
    def decompress_string_to_curve(b64_string: str) -> Dict[Any, Any]:
        """
        Decodes, decompresses, and deserializes a string back into a curve dictionary.

        Pipeline: Base64 (text) -> Gzip (binary) -> JSON -> Dict

        Args:
            b64_string: The Base64-encoded string from the database or API.

        Returns:
            The reconstructed Python dictionary.
        """
        # 1. Encode the ASCII string back into Base64 bytes
        base64_bytes = b64_string.encode('ascii')

        # 2. Decode the Base64 to get the compressed Gzip bytes
        compressed_bytes = base64.b64decode(base64_bytes)

        # 3. Decompress the Gzip bytes to get the original JSON bytes
        json_bytes = gzip.decompress(compressed_bytes)

        # 4. Decode the JSON bytes to a string and parse back into a dictionary
        return json.loads(json_bytes.decode('utf-8'))

    def dependencies(self) :
        return {}

    def get_asset_list(self):
        tiie_asset=msc.Asset.get(unique_identifier="TIIE_28")
        self.tiie_asset=tiie_asset
        return [tiie_asset]

    def update(self):

        # Download CSV from source
        url = "https://valmer.com.mx/VAL/Web_Benchmarks/MEXDERSWAP_IRSTIIEPR.csv"
        response = requests.get(url)
        response.raise_for_status()

        # Load CSV directly from bytes, using correct encoding
        names = ["id", "curve_name", "asof_yyMMdd", "idx", "zero_rate"]
        # STRICT: comma-separated, headerless, exactly these six columns
        df = pd.read_csv(io.BytesIO(response.content), header=None, names=names, sep=",", engine="c",
                         encoding="latin1",
                         dtype=str)
        # pick a rate column

        df["asof_yyMMdd"] = pd.to_datetime(df["asof_yyMMdd"], format="%y%m%d")
        df["asof_yyMMdd"] = df["asof_yyMMdd"].dt.tz_localize('UTC')

        base_dt = df["asof_yyMMdd"].iloc[0] - timedelta(days=1)

        if self.update_statistics.asset_time_statistics[self.tiie_asset.unique_identifier]>=base_dt:
            return pd.DataFrame()

        df["idx"] = df["idx"].astype(int)
        df["days_to_maturity"] = (df["asof_yyMMdd"] - base_dt).dt.days
        df["zero_rate"] = df["zero_rate"].astype(float) / 100

        df["time_index"]=base_dt
        df["unique_identifier"]=self.tiie_asset.unique_identifier

        grouped = (
            df.groupby(["time_index", "unique_identifier"])
            .apply(lambda g: g.set_index("days_to_maturity")["zero_rate"].to_dict())
            .rename("curve")
            .reset_index()
        )

        #    Apply the new compression and encoding function to the 'curve' column.
        grouped["curve"] = grouped["curve"].apply(self.compress_curve_to_string)

        # 3. Final index and structure (your original code)
        grouped = grouped.set_index(["time_index", "unique_identifier"])

        return grouped


    def get_table_metadata(self) -> msc.TableMetaData:
        return msc.TableMetaData(
            identifier="valmer_mexder_tiie28_zero_curve",
            data_frequency_id=msc.DataFrequency.one_d,
            description="Benchmark swap rates (MEXDERSWAP_IRSTIIEPR) from Valmer (valmer.com.mx)"
        )

    def get_column_metadata(self) -> list[msc.ColumnMetaData]:
        return [
            msc.ColumnMetaData(
                column_name=col,
                dtype="float",
                label=col.replace("_", " ").title(),
                description=f"{col} from Valmer swap rate CSV"
            )
            for col in self.update().columns  # will not be called during DAG execution
        ]



class ImportValmer(DataNode):
    def __init__(
            self,
            bucket_name: str,
            *args, **kwargs
    ):
        """
        Initializes the ImportValmer DataNode.

        Args:
            bucket_name (str): The name of the bucket containing the source files.
        """
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
            "combines them, and processes them in a single operation. "
            "It normalizes all column headers by lowercasing them and removing special characters."
        )
        return explanation


    def _get_artifact_data(self):
        """
        Reads all artifacts from the bucket, normalizes columns, and concatenates them into a single DataFrame.
        Optionally filters for new artifacts based on the 'process_all_files' flag.
        """
        import os
        from pathlib import Path
        debug_artifact_path=os.environ.get("DEBUG_ARTIFACT_PATH",None)
        if debug_artifact_path:
            base = Path(debug_artifact_path)
            sorted_artifacts = [pd.read_excel(p, engine=("xlrd" if p.suffix.lower() == ".xls" else "openpyxl"))
                                for p in sorted(base.rglob("*.xls*"))]

            latest_date = self.local_persist_manager.get_update_statistics_for_table().get_max_time_in_update_statistics()
        else:

            if self.artifact_data is not None:
                return self.artifact_data

            artifacts = Artifact.filter(bucket__name=self.bucket_name)
            sorted_artifacts = sorted(artifacts, key=lambda artifact: artifact.name)

            self.logger.info(f"Found {len(sorted_artifacts)} artifacts in bucket '{self.bucket_name}'.")

            # --- Conditional processing based on process_all_files flag ---
            artifact_dates = []
            for artifact in sorted_artifacts:
                match = re.search(r'(\d{4}-\d{2}-\d{2})', artifact.name)
                if match:
                    artifact_dates.append(pd.to_datetime(match.group(1), utc=True))
                else:
                    continue

            latest_date = self.local_persist_manager.get_update_statistics_for_table().get_max_time_in_update_statistics()
            if latest_date:
                self.logger.info(f"Filtering artifacts newer than {latest_date}.")
                sorted_artifacts = [a for a, a_date in zip(sorted_artifacts, artifact_dates) if a_date > latest_date]

            sorted_artifacts = sorted_artifacts[:5]

            self.logger.info(f"Processing {len(sorted_artifacts)} artifacts...")
            if not sorted_artifacts:
                self.logger.info("No new artifacts to process. Task finished.")
                return pd.DataFrame()

        frames = []
        for artifact in tqdm(sorted_artifacts):
            if isinstance(artifact, msc.Artifact):
                name_l = artifact.name.lower()
                content = artifact.content
                buf = content

                df = None
                if name_l.endswith(".xls"):
                    import xlrd  # noqa: F401
                    df = pd.read_excel(buf, engine="xlrd")
                elif name_l.endswith(".csv"):
                    try:
                        df = pd.read_csv(buf, encoding="latin1", engine="pyarrow")
                    except Exception:
                        df = pd.read_csv(buf, encoding="latin1", low_memory=False)
                else:
                    self.logger.info(f"Skipping unsupported file type: {artifact.name}")
                    continue

                if df is None or df.empty:
                    continue
            else:
                df=artifact

            # Normalize all column names
            df.columns = [normalize_column_name(col) for col in df.columns]

            # Check for required columns for instrument identifier
            required_cols = {"tipovalor", "emisora", "serie"}
            if required_cols.issubset(df.columns):
                # Build unique_identifier while keeping all other columns
                df["unique_identifier"] = (
                    df["tipovalor"].astype("string")
                    .str.cat(df["emisora"].astype("string"), sep="_")
                    .str.cat(df["serie"].astype("string"), sep="_")
                )
            else:
                self.logger.warning(
                    f"Skipping unique_identifier creation for {artifact.name} due to missing columns."
                )
                continue

            frames.append(df)

        if not frames:
            raise ValueError(f"No valid data frames could be created from files in bucket '{self.bucket_name}'.")

        try:
            self.artifact_data = pd.concat(frames, ignore_index=True, sort=False, copy=False)
        except TypeError:
            self.artifact_data = pd.concat(frames, ignore_index=True, sort=False)

        self.logger.info(f"Combined all artifacts into a single DataFrame with {len(self.artifact_data)} rows.")
        return self.artifact_data

    def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:
        return {}

    def get_asset_list(self) -> Union[None, list]:
        """
        Processes and registers each unique asset only once from the combined DataFrame.
        """
        self.source_data = self._get_artifact_data()
        if self.source_data.empty: return []

        self.source_data = self.source_data[self.source_data['unique_identifier'].notna()].copy()
        self.source_data['fecha'] = pd.to_datetime(self.source_data['fecha'], format='%Y%m%d', utc=True)

        idx =  self.source_data.groupby('unique_identifier')['fecha'].idxmax()
        df_latest =  self.source_data.loc[idx].reset_index(drop=True)




        floating_tiie =df_latest [df_latest["subyacente"].astype(str).str.contains("TIIE", na=False)]
        floating_cetes =df_latest [df_latest ["subyacente"].astype(str).str.contains("CETE", na=False)]
        m_bono_fixed_0=df_latest [df_latest ["subyacente"].astype(str).str.contains("Bonos M", na=False)]
        m_bono_fixed_0 = m_bono_fixed_0[m_bono_fixed_0.monedaemision == "MPS"]


        all_target_bonds = pd.concat([floating_tiie, floating_cetes,m_bono_fixed_0], axis=0, ignore_index=True)




        unique_identifiers = self.source_data['unique_identifier'].unique().tolist()
        self.logger.info(f"Found {len(unique_identifiers)} unique assets to process.")

        registered_assets = []



        #get all assets fast
        per_page_assets=os.environ.get("VALMER_PER_PAGE",5000)
        existing_assets=msc.Asset.query(unique_identifier__in=unique_identifiers,per_page=per_page_assets)
        existing_assets={a.unique_identifier:a for a in existing_assets}
        uids_to_update=[]
        uids_to_update_pricing_details=[]
        for u in unique_identifiers:


            if u not in existing_assets.keys():
                uids_to_update.append(u)
                continue

            target_asset = existing_assets[u]
            target_row = all_target_bonds[all_target_bonds.unique_identifier == u]
            if  target_row.empty ==True:
                # Note only adding instrument details for floaters
                continue
            target_row=target_row.iloc[0]
            if  target_asset.current_pricing_detail is None :
                uids_to_update.append(u)
                continue
            if target_asset.current_pricing_detail.instrument_dump is None:
                #wrongly assigned
                uids_to_update.append(u)
                continue

            old_face_value=target_asset.current_pricing_detail.instrument_dump["instrument"].get("face_value")
            if old_face_value is None:
                # no pricing instruments
                uids_to_update.append(u)
                continue
            #update in face value
            if old_face_value!=target_row["valornominalactualizado"] :
                uids_to_update.append(u)
                continue



        instrument_pricing_detail_map={}

        for i in range(0, len(uids_to_update), per_page_assets):
            batch_identifiers = uids_to_update[i:i + per_page_assets]
            assets_payload = []

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
                row=df_latest[df_latest['unique_identifier']==identifier].iloc[0]
                if row["unique_identifier"] in all_target_bonds["unique_identifier"].to_list():
                    if str(row["reglacupon"]) =="0":
                        continue #Todo not implemented zero
                    icalendar, business_day_convention, settlement_days, day_count = get_instrument_conventions(row)

                    ql_bond=build_qll_bond_from_row(row=row,

                                                       calendar=icalendar, dc=day_count,
                                                       bdc=business_day_convention, settlement_days=settlement_days,
                                                       )

                    instrument_pricing_detail_map[identifier]={"instrument":ql_bond,
                                                               "pricing_details_date":row["fecha"]
                                                               }
                else:
                    continue
            if not assets_payload:
                continue

            self.logger.info(f"Getting or registering assets in batch {i // per_page_assets + 1}/{len(uids_to_update)//per_page_assets}...")
            try:
                assets = msc.Asset.batch_get_or_register_custom_assets(assets_payload)
                registered_assets.extend(assets)
            except Exception as e:
                self.logger.error(f"Failed to process asset batch: {e}")
                raise

        for asset in registered_assets:
            if asset.unique_identifier in instrument_pricing_detail_map.keys():
                asset.add_instrument_pricing_details_from_ms_instrument(**instrument_pricing_detail_map[asset.unique_identifier]
                                                                        )

        asset_list=registered_assets+list(existing_assets.values())
        return asset_list

    def _get_column_metadata(self):
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

        if source_data.empty:
            return pd.DataFrame()

        # Use the normalized column names 'fecha' and 'preciosucio'
        if "fecha" not in source_data.columns or "preciosucio" not in source_data.columns:
            raise KeyError("Normalized columns 'fecha' and/or 'preciosucio' not found in the data.")

        source_data.rename(columns={"fecha": "time_index"}, inplace=True)


        # Select only the necessary columns for the final OHLCV output
        price_series = source_data['preciosucio'].astype(float)
        ohlc_df = pd.DataFrame(index=source_data.index)
        ohlc_df['time_index'] = source_data['time_index']
        ohlc_df['unique_identifier'] = source_data['unique_identifier']
        ohlc_df['open'] = price_series
        ohlc_df['high'] = price_series
        ohlc_df['low'] = price_series
        ohlc_df['close'] = price_series
        ohlc_df['volume'] = 0
        ohlc_df['open_time'] = ohlc_df['time_index'].astype(np.int64) // 10 ** 9
        ohlc_df['preciolimpio']= pd.to_numeric(source_data['preciolimpio'], errors='coerce')
        ohlc_df['duracion'] =  pd.to_numeric(source_data['duracion'], errors='coerce')

        #This should be in a snapshot table to optimye space
        ohlc_df['calificacionfitch'] = source_data['calificacionfitch']
        ohlc_df['fechavcto'] =pd.to_datetime(source_data['fechavcto'], errors='coerce').apply(
            lambda x: x.timestamp() if pd.isna(x) == False else None
        )
        ohlc_df['monedaemision'] = source_data['monedaemision']
        ohlc_df['sector'] = source_data['sector']
        ohlc_df['nombrecompleto'] = source_data['nombrecompleto']


        ohlc_df['diastransccpn'] = source_data['diastransccpn']
        ohlc_df['tasacupon'] =pd.to_numeric(source_data['tasacupon'], errors='coerce')
        ohlc_df['cuponesxcobrar'] = pd.to_numeric(source_data['cuponesxcobrar'], errors='coerce')
        ohlc_df['valornominal'] = pd.to_numeric(source_data['valornominal'], errors='coerce')
        ohlc_df['reglacupon'] = source_data['reglacupon'].astype(str)
        ohlc_df['freccpn'] =  source_data['freccpn'].astype(str)
        ohlc_df['cuponactual'] = pd.to_numeric(source_data['cuponactual'], errors='coerce')
        ohlc_df['sobretasa'] = pd.to_numeric(source_data['sobretasa'], errors='coerce')

        ohlc_df["tasaderendimiento"]=pd.to_numeric(source_data['tasaderendimiento'], errors='coerce')


        ohlc_df.set_index(["time_index", "unique_identifier"], inplace=True)
        ohlc_df = self.update_statistics.filter_df_by_latest_value(ohlc_df)

        return ohlc_df

    def get_table_metadata(self) -> msc.TableMetaData:
        TS_ID = "vector_de_precios_valmer"
        meta = msc.TableMetaData(
            identifier=TS_ID,
            description=f"Vector de Precios Valmer",
            data_frequency_id=msc.DataFrequency.one_month,
        )
        return meta







