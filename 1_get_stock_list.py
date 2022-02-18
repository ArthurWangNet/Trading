"""
This file will get the stock list from TdAmeritrade API.
Save the latest stock list to a file.
Compare the latest stock list with the old one and write down the differences between the two if there are any.
Update the ListTracker.csv file to keep track of the stock list.
"""

import pandas as pd
import requests
import TD_API
import api_keys
import os
import Paths
import datetime


def what_day_is_today():
    # Utility function to get the current date in a string format.
    # Get the date of today
    today = datetime.datetime.today()
    today = today.strftime('%Y-%m-%d')
    return today

def get_stock_list_from_td():
    today = what_day_is_today()
    # Get the stock list from TdAmeritrade API
    endpoint = TD_API.get_instruments()
    payload ={
        'apikey': api_keys.APIKEY_FUNDAMENTAL,
        'symbol': r'[A-Z].*',
        'projection': 'symbol-regex',
    }
    
    # Request data using paramaeters above.
    print('Getting stock list from TdAmeritrade API...')
    response = requests.get(endpoint, params=payload)
    content = response.content
    # Update 2022-02-17: Somehow the content become a byte array start with b', so we need to decode it.
    content = content.decode('utf-8')
    print('Done!')

    # Parse json content
    print('Parsing json content...')
    instruments_list = pd.read_json(content,orient='index')
    instruments_list = instruments_list.sort_index()
    print('Done!')

    # Save the latest stock list to a file for future reference
    print('Saving the all instruments list to a file...')
    instruments_list.to_csv(os.path.join(Paths.All_Instruments_List, today + '.csv'))
    print('Done!')

    # The list above contians all tradeble instruments on TD Ameritrade platform. 
    # We only want the stocks and ETFs.
    # We will filter the list to get only the stocks and ETFs.
    # We will also filter the list to get only stocks and ETFs from NYSE, NASDAQ, AMEX, Pacific and BATS
    print('Filtering the list to get only Equities and ETFs from NYSE, NASDAQ, AMEX, Pacific and BATS...')
    assetType = ['EQUITY', 'ETF']
    exchange = ['NYSE', 'NASDAQ', 'AMEX', 'Pacific', 'BATS']

    # Filter the dataframe, only keep where assetType is in the list above and exchange is in the list above
    filtered_instruments_list = instruments_list[instruments_list['assetType'].isin(assetType) 
                                & instruments_list['exchange'].isin(exchange)]
    print('Done!')
    
    # Save the filtered stock list to a file for future reference
    print('Saving the filtered instruments list to a file...')
    filtered_instruments_list.to_csv(os.path.join(Paths.Stocks_List, today + '.csv'))
    print('Done!')


get_stock_list_from_td()
