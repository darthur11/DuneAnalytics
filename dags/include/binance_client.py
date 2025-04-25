import datetime
from typing import Optional

import requests
import random
import time
import json


class BinanceClient:
    BASE_URL = "https://api.binance.com/"
    HISTORICAL_TRADES_ENDPOINT = "/api/v3/historicalTrades"

    def __init__(self, logger):
        self.logger = logger

    def get_url(self, symbol, limit: Optional[int] = None, from_id: Optional[int] = None) -> str:
        limit_part = ""
        from_id_part = ""
        if limit:
            limit_part = f"&limit={limit}"
        if from_id:
            from_id_part = f"&fromId={from_id}"
        return f"{self.BASE_URL}{self.HISTORICAL_TRADES_ENDPOINT}?symbol={symbol}{limit_part}{from_id_part}"

    def get_historical_trades(
            self,
            symbol: str,
            limit: int = 1000,
            min_datetime: datetime.datetime = None,
            max_datetime: datetime.datetime = None
    ) -> list[dict]:
        order_id, min_response_ts, max_response_ts = None, None, None
        min_ts, max_ts = min_datetime.timestamp() * 1000, max_datetime.timestamp() * 1000
        output_array = []
        self.logger.info(f"Min ts: {min_ts}, Max ts: {max_ts}")
        while True:
            try:
                url = self.get_url(symbol, limit, order_id)
                self.logger.info(f"URL: {url}")
                request = requests.get(url)
                request.raise_for_status()
                response = request.json()
                if response:
                    order_id, min_response_ts, max_response_ts = response[0]["id"], response[0]["time"], response[-1][
                        "time"]
                    self.logger.info(f"ID, min_ts, max_ts: {order_id}, {min_response_ts}, {max_response_ts}")
                    # Check if the response is within the time range and the whole output within the time range
                    if max_response_ts <= max_ts and min_response_ts >= min_ts:
                        output_array.extend(response)
                    # If one side is less than date end and the other side is greater than date end
                    elif min_response_ts < max_ts < max_response_ts:
                        output_array.extend(response)
                    # If one side is less than date start and the other side is greater than date start
                    elif min_response_ts < min_ts < max_response_ts:
                        output_array.extend(response)
                        break
                    # Highest response timestamp is less than date start
                    elif max_response_ts < min_ts:
                        break
                    else:
                        order_id = order_id - limit
                        continue
                    order_id = order_id - limit
                    time.sleep(random.random() / 2)
            except requests.exceptions.RequestException as e:
                self.logger.error(e)
        return [element for element in output_array if min_ts <= element["time"] < max_ts]
