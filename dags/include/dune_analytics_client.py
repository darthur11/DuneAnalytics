import datetime
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dune_client.types import QueryParameter

import pandas as pd

class DuneAnalyticsClient:
    def __init__(self, logger, api_key):
        self.logger = logger
        self.dune_client = DuneClient(api_key)

    def get_query(self, date_start: datetime.datetime, date_end: datetime.datetime) -> QueryBase:
        return QueryBase(
            name="CowSwap Trades",
            query_id=4154546,
            params=[
                QueryParameter.date_type(name="ts_start", value=date_start.strftime('%Y-%m-%d %H:%M:%S')),
                QueryParameter.date_type(name="ts_end", value=date_end.strftime('%Y-%m-%d %H:%M:%S')),
            ],
        )

    def get_latest_result(self, date_start: datetime.datetime, date_end: datetime.datetime) -> pd.DataFrame:
        query = self.get_query(date_start, date_end)
        return self.dune_client.run_query_dataframe(query)
