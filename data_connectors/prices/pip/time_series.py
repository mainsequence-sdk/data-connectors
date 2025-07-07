

import datetime

from dateutil.tz import tzoffset
from mainsequence.tdag.time_series import TimeSerie, WrapperTimeSerie, ModelList
import os
import pytz
from typing import List
import pandas  as pd
from mainsequence.client import MARKETS_CONSTANTS as CONSTANTS, IndexAsset, ExecutionVenue, Asset,IndexAsset
import pandas_market_calendars as mcal



class PIPDailyBenchmarkFromFile(TimeSerie):
    """
    Integration for Alpaca Data Series

    """
    INIT_HISTORY = datetime.datetime(2018, 1, 1).replace(tzinfo=pytz.UTC)

    @TimeSerie._post_init_routines()
    def __init__(self, asset_list: ModelList,
                  local_kwargs_to_ignore: List[str] = ["asset_list"], *args, **kwargs):
        """

        Args:
            asset:
            frequency_id:
            *args:
            **kwargs:
        """
        for a in asset_list:
            assert a.execution_venue.symbol ==CONSTANTS.PIP_EXECUTION_VENUE
        self.asset_list = asset_list

        super().__init__(*args, **kwargs)
    def run_after_post_init_routines(self):
        """
        Use post init routines to configure the time series
        """
        if self.metadata is None:
            return None
        if self.metadata["protect_from_deletion"]==False:
            self.local_persist_manager.protect_from_deletion()



    def update(self, latest_value, **class_arguments):



        pip_lake_path=self.local_persist_manager.metadata['build_meta_data'].get("pip_lake_path",None)
        if pip_lake_path is None:
          # raise Exception("No pip_lake_path defined")
            pass
        if latest_value is None:
            latest_value=datetime.datetime(2017,1,1).replace(tzinfo=pytz.UTC)

        end = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC)-datetime.timedelta(days=1)
        data_df=[]
        for a in self.asset_list:
            #todo: substitute by  file reading
            self.logger.warning("Prices from PIP are just been mocked do not use in production")
            calendar = mcal.get_calendar(a.calendar)
            full_schedule = calendar.schedule(latest_value,
                                              datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0,
                                                                              tzinfo=pytz.UTC)
                                              ).reset_index()
            data = pd.DataFrame(columns=["asset_symbol"],
                                index=full_schedule["market_close"].to_list()
                                                   )
            data["open_time"]=full_schedule["market_open"].astype(int)
            data.loc[:,"asset_symbol"]=a.symbol


        data_df.append(data)
        data_df=pd.concat(data_df,axis=1)
        data_df["close"]=1.0
        data_df["vwap"]=1.0
        data_df["volume"] = 0.0

        data_df["execution_venue_symbol"]=CONSTANTS.PIP_EXECUTION_VENUE
        data_df=data_df[data_df.index>latest_value]
        data_df.index.name="time_index"
        data_df=data_df.set_index(["asset_symbol","execution_venue_symbol"],append=True)

        return data_df