import re

import mainsequence.client as msc

from mainsequence.tdag import DataNode, APIDataNode

import pytz

import json

from typing import Dict, Any, List, Union, Optional
import gzip
import base64
from pydantic import BaseModel,Field
import datetime
import pandas as pd


from data_connectors.interest_rates.registries.discount_curves import DISCOUNT_CURVE_BUILD_REGISTRY
from data_connectors.interest_rates.registries.fixing_rates import FIXING_RATE_BUILD_REGISTRY
from data_connectors.interest_rates.registries.constants import TIIE_28_ZERO_CURVE, M_BONOS_ZERO_OTR


UTC = pytz.UTC

class CurveConfig(BaseModel):
    unique_identifier:str=Field(...,title="Curve unique identifier",description="string name of curve to create",
                                          ignore_from_storage_hash=True
                                          )
    name:str=Field(...,title="Curve name",description="string name of curve to create",
                                          ignore_from_storage_hash=True
                                          )
    curve_points_dependecy_data_node_uid:Optional[str]=Field(None,title="Dependecies curve points",description="",
                                          ignore_from_storage_hash=True
                                          )


class RateConfig(BaseModel):
    unique_identifier:str=Field(...,title="asset unique identifier",description="string name of curve to create",
                                          ignore_from_storage_hash=True
                                          )
    name:str=Field(...,title="asset name",description="string name of curve to create",
                                          ignore_from_storage_hash=True
                                          )

class FixingRateConfig(BaseModel):
    rates_config_list: List[RateConfig] = Field(..., title="Interest rates build", description="string name of curve to create",
                                   ignore_from_storage_hash=True
                                   )



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




class DiscountCurves(DataNode):
    """Download and return daily MEXDERSWAP_IRSTIIEPR swap rates from valmer.com.mx

        Output:
            - Index: DatetimeIndex named 'time_index' (UTC)
            - Columns: cleaned from the CSV (lowercase, <=63 chars, no datetime columns)
        """
    OFFSET_START = datetime.datetime(1990, 1, 1, tzinfo=pytz.utc)
    def __init__(self,curve_config:CurveConfig,*args, **kwargs):

        self.curve_config=curve_config

        base_node_curve_points = self.curve_config.curve_points_dependecy_data_node_uid
        if base_node_curve_points is not None:
            base_node_curve_points = APIDataNode.build_from_identifier(identifier=base_node_curve_points)
        self.base_node_curve_points = base_node_curve_points
        super().__init__(*args, **kwargs)

    def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:

        if self.base_node_curve_points is not None:
            return {self.curve_config.curve_points_dependecy_data_node_uid: self.base_node_curve_points}

        return {}

    def get_asset_list(self):
        assets_payload=[]
        snapshot = {
            "name": self.curve_config.name,
            "ticker": self.curve_config.unique_identifier,
        }
        payload_item = {
            "unique_identifier":   self.curve_config.unique_identifier,
            "snapshot": snapshot,
        }
        assets_payload.append(payload_item)

        assets = msc.Asset.batch_get_or_register_custom_assets(assets_payload)


        return assets

    def update(self):

        # Download CSV from source
        df=DISCOUNT_CURVE_BUILD_REGISTRY[self.curve_config.unique_identifier](update_statistics=self.update_statistics,
                                           curve_unique_identifier=self.curve_config.unique_identifier,
                                                                              base_node_curve_points=self.base_node_curve_points,
                                           )

        #    Apply the new compression and encoding function to the 'curve' column.
        df["curve"] = df["curve"].apply(compress_curve_to_string)

        last_update=self.update_statistics.get_last_update_index_2d(self.curve_config.unique_identifier)

        df=df[df.index.get_level_values("time_index")>last_update]

        if df.empty:
            return pd.DataFrame()

        return df


    def get_table_metadata(self) -> msc.TableMetaData:
        return msc.TableMetaData(
            identifier="discount_curves",
            data_frequency_id=msc.DataFrequency.one_d,
            description="Collection of Discount Curves"
        )

    def get_column_metadata(self) -> list[msc.ColumnMetaData]:
        return [
            msc.ColumnMetaData(
                column_name="curve",
                dtype="str",
                label="Compressed Curve",
                description=f"Compressed Discount Curve"
            )

        ]


class FixingRatesNode(DataNode):
    OFFSET_START = datetime.datetime(1990, 1, 1, tzinfo=pytz.utc)

    def __init__(self, rates_config: FixingRateConfig, *args, **kwargs):
        self.rates_config = rates_config



        super().__init__(*args, **kwargs)

    def get_asset_list(self):
        assets_payload = []
        for config in self.rates_config.rates_config_list:

            snapshot = {
                "name": config.name,
                "ticker": config.unique_identifier,
            }
            payload_item = {
                "unique_identifier": config.unique_identifier,
                "snapshot": snapshot,
            }
            assets_payload.append(payload_item)

        assets = msc.Asset.batch_get_or_register_custom_assets(assets_payload)

        return assets

    def dependencies(self):
        return {}

    def update(self):

        all_dfs=[]
        for asset in self.update_statistics.asset_list:
            # Download CSV from source
            df=FIXING_RATE_BUILD_REGISTRY[asset.unique_identifier](update_statistics=self.update_statistics,
                                               unique_identifier=asset.unique_identifier,

                                               )
            if df.empty:
                continue
            all_dfs.append(df)
        if len(all_dfs)==0:
            return pd.DataFrame()
        all_dfs=pd.concat(all_dfs,axis=0)

        assert all_dfs.index.names==["time_index", "unique_identifier"]
        all_dfs=all_dfs[["rate"]].dropna()

        return all_dfs

    def get_table_metadata(self) -> msc.TableMetaData:
        return msc.TableMetaData(
            identifier="fixing_rates_1d",
            data_frequency_id=msc.DataFrequency.one_d,
            description=f"Daily fixing rates ",
        )

    def get_column_metadata(self) -> List[msc.ColumnMetaData]:
        return [
            msc.ColumnMetaData(
                column_name="rate",
                dtype="float",
                label="Fixing Rate (decimal)",
                description="Fixing value normalized to decimal (percentage/100).",
            ),
        ]