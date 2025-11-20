import re
from datetime import timedelta
from typing import Union, Dict,List, Tuple
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

            artifacts,artifact_dates=self._get_artifacts(self.logger, self.bucket_name)
            latest_date = self.local_persist_manager.get_update_statistics_for_table().get_max_time_in_update_statistics()
            if latest_date:
                self.logger.info(f"Filtering artifacts newer than {latest_date}.")
                sorted_artifacts = [a for a, a_date in zip(artifacts, artifact_dates) if a_date > latest_date]

            sorted_artifacts = sorted_artifacts[:5]

            self.logger.info(f"Processing {len(sorted_artifacts)} artifacts...")
            if not sorted_artifacts:
                self.logger.info("No new artifacts to process. Task finished.")
                return pd.DataFrame()

        frames=self._concatenate_artifacts_content(sorted_artifacts,self.logger)

        try:
            self.artifact_data = pd.concat(frames, ignore_index=True, sort=False, copy=False)
        except TypeError:
            self.artifact_data = pd.concat(frames, ignore_index=True, sort=False)

        self.logger.info(f"Combined all artifacts into a single DataFrame with {len(self.artifact_data)} rows.")
        return self.artifact_data

    def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:
        return {}


    #------- Helpers for bond and vector filter -------#
    @staticmethod
    def _get_target_bonds(df_latest:pd.DataFrame):

        df_latest.tipovalor=df_latest.tipovalor.astype(str)
        floating_tiie = df_latest[df_latest["subyacente"].astype(str).str.contains("TIIE", na=False)]
        floating_cetes = df_latest[df_latest["subyacente"].astype(str).str.contains("CETE", na=False)]
        cetes = df_latest[df_latest["subyacente"].astype(str).str.contains("Cete", na=False)]
        m_bono_fixed_0 = df_latest[df_latest["subyacente"].astype(str).str.contains("Bonos M", na=False)]
        m_bono_fixed_0 = m_bono_fixed_0[m_bono_fixed_0.monedaemision == "MPS"]

        bondes_d=df_latest[df_latest["subyacente"].astype(str).str.contains("Fondeo Bancario", na=False)]
        bondes_f_g=df_latest[df_latest["subyacente"].astype(str).str.contains("Tasa TIIE Fondeo 1D", na=False)]

        zero_corps=df_latest[df_latest["tipovalor"].astype(str).isin(["I","93","92"])]
        zero_corps=zero_corps[zero_corps["monedaemision"].isin(["MPS"])]

        bpas=df_latest[df_latest.emisora.isin(["BPAG91","BPAG28","BPA182"])]
        bpas = bpas[bpas["monedaemision"].isin(["MPS"])]

        all_target_bonds = pd.concat([floating_tiie, floating_cetes, m_bono_fixed_0, cetes,bondes_d,bondes_f_g,
        zero_corps,bpas], axis=0, ignore_index=True)

        # all_target_bonds = pd.concat([zero_corps], axis=0, ignore_index=True)
        all_target_bonds=all_target_bonds[~all_target_bonds.fechaemision.isnull()]

        return all_target_bonds

    @staticmethod
    def _get_uids_to_update(
            unique_identifiers: List[str],
            existing_assets: Dict[str, "msc.Asset"],
            all_target_bonds: pd.DataFrame,
            *,
            force_update: bool = False,
    ) -> Tuple[List[str], List[str]]:
        """
        Decide which UIDs need (a) asset registration and/or (b) pricing-detail update.

        Returns:
            missing_assets: list[str]   -> assets not in existing_assets (register )
            pricing_updates: list[str]  -> assets (existing or newly-created) that need pricing-detail update

        Behavior:
            - Pricing updates are only considered for *target bonds* (present in all_target_bonds).
            - If force_update=True, every existing target bond goes to pricing_updates.
        """
        if all_target_bonds.empty:
            return [], []

        target_rows = (
            all_target_bonds.drop_duplicates("unique_identifier", keep="last")
            .set_index("unique_identifier")
        )
        target_uids = set(target_rows.index)

        missing_assets: List[str] = []
        pricing_updates: List[str] = []

        # If you're only updating pricing, limit the iteration to target UIDs
        candidates = unique_identifiers

        for u in candidates:
            in_targets = u in target_uids
            asset = existing_assets.get(u)

            if asset is None:
                missing_assets.append(u)
                # Newly-created assets also need pricing details *if* they are target bonds.
                if in_targets:
                    pricing_updates.append(u)

                continue

            # Existing asset
            if not in_targets:
                # Not a target bond => no pricing update requested.
                continue

            if force_update:
                pricing_updates.append(u)
                continue

            cpd = getattr(asset, "current_pricing_detail", None)
            if not cpd or getattr(cpd, "instrument_dump", None) is None:
                pricing_updates.append(u)
                continue

            old_face_value = None
            try:
                old_face_value = cpd.instrument_dump.get("instrument", {}).get("face_value")
            except Exception:
                old_face_value = None

            # Compare against latest nominal value in targets
            new_face_value = target_rows.loc[u].get("valornominalactualizado", None)
            if old_face_value is None or old_face_value != new_face_value:
                pricing_updates.append(u)



        # Deduplicate while preserving order
        def _dedup(seq: List[str]) -> List[str]:
            return list(dict.fromkeys(seq))

        return _dedup(missing_assets), _dedup(pricing_updates)

    @staticmethod
    def _get_artifacts(logger,bucket_name):



        artifacts = Artifact.filter(bucket__name=bucket_name,)
        sorted_artifacts = sorted(artifacts, key=lambda artifact: artifact.name)

        logger.info(f"Found {len(sorted_artifacts)} artifacts in bucket '{bucket_name}'.")

        # --- Conditional processing based on process_all_files flag ---
        artifact_dates = []
        for artifact in sorted_artifacts:
            match = re.search(r'(\d{4}-\d{2}-\d{2})', artifact.name)
            if match:
                artifact_dates.append(pd.to_datetime(match.group(1), utc=True))
            else:
                continue



        return sorted_artifacts,artifact_dates
    @staticmethod
    def _concatenate_artifacts_content(sorted_artifacts,logger):
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
                    logger.info(f"Skipping unsupported file type: {artifact.name}")
                    continue

                if df is None or df.empty:
                    continue
            else:
                df = artifact

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
                logger.warning(
                    f"Skipping unique_identifier creation for {artifact.name} due to missing columns."
                )
                continue

            frames.append(df)

        if not frames:
            raise ValueError(f"No valid data frames could be created from files in bucket .")
        return frames

    @staticmethod
    def _pick_latest_artifact(artifacts, logger):
        """Pick the single latest artifact by YYYY-MM-DD in its name; fallback to last by name."""
        import re
        if not artifacts:
            return None

        def _parse_dt(a):
            m = re.search(r"(\d{4}-\d{2}-\d{2})", a.name)
            return pd.to_datetime(m.group(1), utc=True) if m else pd.NaT

        dated = [(a, _parse_dt(a)) for a in artifacts]
        dated = [(a, d) for a, d in dated if pd.notna(d)]
        if dated:
            max_date = max(d for _, d in dated)
            candidates = [a for a, d in dated if d == max_date]
            selected = sorted(candidates, key=lambda x: x.name)[-1]
            logger.info(f"Selected latest artifact: {selected.name} ({max_date.date()})")
            return selected

        logger.warning("No parsable dates in artifact names; falling back to last by name.")
        return sorted(artifacts, key=lambda a: a.name)[-1]

    def _register_and_update_pricing(self,
                                     unique_identifiers: List[str],
                                     df_latest: pd.DataFrame,
                                     all_target_bonds: pd.DataFrame,
                                     *,
                                     force_update: bool = False) -> list:
        """One orchestrator for both paths (full vector or last vector)."""
        import os

        # pull existing assets once
        per_page_assets = int(os.environ.get("VALMER_PER_PAGE", 5000))
        existing_assets_list = msc.Asset.query(unique_identifier__in=unique_identifiers, per_page=per_page_assets)
        existing_assets = {a.unique_identifier: a for a in existing_assets_list}

        missing_assets, pricing_updates = self._get_uids_to_update(
            unique_identifiers, existing_assets, all_target_bonds, force_update=force_update
        )

        df_latest_idx = (
            df_latest.drop_duplicates("unique_identifier", keep="last")
            .set_index("unique_identifier")
        )
        target_uids = set(all_target_bonds["unique_identifier"].unique())

        # --- register missing assets ---
        registered_assets = []
        newly_registered_map: Dict[str, "msc.Asset"] = {}
        if missing_assets:
            for i in range(0, len(missing_assets), per_page_assets):
                batch = missing_assets[i:i + per_page_assets]
                assets_payload = [
                    {"unique_identifier": uid, "snapshot": {"name": uid, "ticker": uid}}
                    for uid in batch
                ]
                self.logger.info(
                    f"Getting or registering assets in batch {i // per_page_assets + 1}/"
                    f"{(len(missing_assets) + per_page_assets - 1) // per_page_assets}..."
                )
                try:
                    assets = msc.Asset.batch_get_or_register_custom_assets(assets_payload)
                    registered_assets.extend(assets)
                    newly_registered_map.update({a.unique_identifier: a for a in assets})
                except Exception as e:
                    self.logger.error(f"Failed to process asset batch: {e}")
                    raise

        # --- decide pricing recipients ---
        uids_needing_pricing = set(pricing_updates)
        uids_needing_pricing.update(u for u in missing_assets if u in target_uids)

        if uids_needing_pricing:
            instrument_pricing_detail_map: Dict[str, dict] = {}
            for uid in uids_needing_pricing:
                if uid not in df_latest_idx.index:
                    continue
                row = df_latest_idx.loc[uid]

                icalendar, business_day_convention, settlement_days, day_count = get_instrument_conventions(row)
                ql_bond = build_qll_bond_from_row(
                    row=row,
                    calendar=icalendar,
                    dc=day_count,
                    bdc=business_day_convention,
                    settlement_days=settlement_days,
                )
                instrument_pricing_detail_map[uid] = {
                    "instrument": ql_bond,
                    "pricing_details_date": row["fecha"],
                }

            # target the correct asset objects (newly registered + existing)
            assets_for_update: Dict[str, "msc.Asset"] = {}
            assets_for_update.update(
                {u: newly_registered_map[u] for u in newly_registered_map.keys() if u in instrument_pricing_detail_map}
            )
            assets_for_update.update(
                {u: existing_assets[u] for u in pricing_updates if
                 u in existing_assets and u in instrument_pricing_detail_map}
            )

            for uid, asset in assets_for_update.items():
                try:
                    asset.add_instrument_pricing_details_from_ms_instrument(**instrument_pricing_detail_map[uid])
                except Exception as e:
                    self.logger.error(f"Failed to update pricing details for {uid}: {e}")

        return registered_assets + list(existing_assets.values())
    def update_pricing_details_from_last_vector(self,force_update=False):
        artifacts, artifact_dates = self._get_artifacts(self.logger, self.bucket_name)
        if not artifacts:
            self.logger.info("No artifacts to process.")
            return []

        last_artifact = self._pick_latest_artifact(artifacts, self.logger)
        if last_artifact is None:
            self.logger.info("No latest artifact could be selected.")
            return []
        source_df_list = self._concatenate_artifacts_content([last_artifact], self.logger)
        if not source_df_list:
            self.logger.info("Latest artifact produced no usable rows.")
            return []
        source_df = source_df_list[0]
        df_latest, all_target_bonds, unique_identifiers = self._prepare_latest_inputs(source_df)
        self.logger.info(f"[last vector] Found {len(unique_identifiers)} unique assets to process.")
        return self._register_and_update_pricing(unique_identifiers, df_latest, all_target_bonds,
                                                 force_update=force_update)

    def _prepare_latest_inputs(self, df: pd.DataFrame):
        """Common prep: normalize, latest rows per UID, target bonds, and universe of UIDs."""
        df = df[df["unique_identifier"].notna()].copy()
        df["fecha"] = pd.to_datetime(df["fecha"], format="%Y%m%d", utc=True)

        idx = df.groupby("unique_identifier")["fecha"].idxmax()
        df_latest = df.loc[idx].reset_index(drop=True)

        all_target_bonds = self._get_target_bonds(df_latest)
        unique_identifiers = df["unique_identifier"].unique().tolist()
        return df_latest, all_target_bonds, unique_identifiers

    def get_asset_list(self) -> Union[None, list]:
        """
        Processes and registers each unique asset only once from the combined DataFrame.
        """
        self.source_data = self._get_artifact_data()
        if self.source_data.empty: return []
        df_latest, all_target_bonds, unique_identifiers = self._prepare_latest_inputs(self.source_data)
        self.logger.info(f"Found {len(unique_identifiers)} unique assets to process.")
        return self._register_and_update_pricing(unique_identifiers, df_latest, all_target_bonds, force_update=False)

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







