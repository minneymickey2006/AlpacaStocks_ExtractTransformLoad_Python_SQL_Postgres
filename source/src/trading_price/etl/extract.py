import pandas as pd 
from utility.date_time import DateTime
import requests

class Extract():

    @staticmethod
    def extract(
            stock_ticker:str, 
            api_key_id:str, 
            api_secret_key:str, 
            start_date:str=None, 
            end_date:str=None
        )->pd.DataFrame:
        """
        Extract trades data from the Alpaca API. 
        - stock_ticker: ticker of a stock e.g. tsla 
        - api_key_id: api key id from Alpaca
        - api_secret_key: api key secret from Alpaca
        - start_date: date to begin extracting data from 
        - end_date: date to stop extracting data to 
        
        Returns: 
        - DataFrame with the requested dates 
        """
        
        base_url = f"https://data.alpaca.markets/v2/stocks/{stock_ticker}/trades"
        response_data = []
        for date in DateTime.generate_datetime_ranges(start_date=start_date, end_date=end_date):
            start_time = date.get("start_time")
            end_time = date.get("end_time")

            params = {
                "start": start_time,
                "end": end_time
            }

            # auth example: https://alpaca.markets/docs/api-references/trading-api/
            headers = {
                "APCA-API-KEY-ID": api_key_id,
                "APCA-API-SECRET-KEY": api_secret_key
            }
            response = requests.get(base_url, params=params, headers=headers)
            if response.json().get("trades") is not None: 
                response_data.extend(response.json().get("trades"))
        # read json data to a dataframe 
        df = pd.json_normalize(data=response_data, meta=["symbol"])
        return df 

    @staticmethod
    def extract_exchange_codes(fp:str)->pd.DataFrame:
        """
        Reads exchange codes CSV file and returns a dataframe.
        - fp: filepath to the exchange codes CSV file
        """
        df = pd.read_csv(fp)
        return df
