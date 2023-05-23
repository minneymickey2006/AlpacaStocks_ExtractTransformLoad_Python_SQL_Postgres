import pandas as pd 

class Transform():

    @staticmethod
    def transform(
            df:pd.DataFrame, 
            df_exchange_codes:pd.DataFrame
        )->pd.DataFrame:
        """
        Performs transformation on dataframe produced from extract() function.
        - df: dataframe produced from extract() function 
        - df_exchange_codes: dataframe produced from extract_exchange_codes() function 

        Returns: 
        - a transformed dataframe 
        """
        
        df_quotes_renamed = df.rename(columns={
            "t": "timestamp",
            "x": "exchange",
            "p": "price",
            "s": "size",
        })
        df_quotes_selected = df_quotes_renamed[['timestamp', 'exchange', 'price', 'size']]
        df_exchange = pd.merge(left=df_quotes_selected, right=df_exchange_codes, left_on="exchange", right_on="exchange_code").drop(columns=["exchange_code", "exchange"]).rename(columns={"exchange_name": "exchange"})
        # remove duplicates by doing a group by on the keys: timestamp and exchange
        # get the mean of price, and sum of size
        df_ask_bid_exchange_de_dup = df_exchange.groupby(["timestamp", "exchange"]).agg({
            "price": "mean",
            "size": "sum",
        }).reset_index()
        return df_ask_bid_exchange_de_dup