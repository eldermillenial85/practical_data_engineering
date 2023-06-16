
import os
import pandas as pd 
import pandas_datareader as web

import yfinance as yfinance
yf.pdr_override()

def get_eth_price():

    date_range = pd.read_sql(
        sql="SELECT min(created_at), max(created_at) FROM crypto_transactions",
        con=os.getenv
    )

    eth_price_df = web.data.get_data('ETH-USD', date_range.iloc[0]["min"], date_range.iloc[0]["max"])

    return eth_price_df