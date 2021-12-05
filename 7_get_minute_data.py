import time_utilities
import pandas as pd
import numpy as np
import os
import TD_API
import Paths
import time
import api_keys
import requests
import utilities
from tqdm import tqdm
import datetime
import time


"""
TD's API has date limit on minute data. That is it will givin maxium aournd 30 trading days back.
The trick is only specify the date 90 days back of today, so we can be sure that we always has the latest data every time we request.
There will be some split stock issue need to be handled when prepare production.
But this script will only focus on getting the minute data every day.
"""




def get_minute_data(symbol):
    #TD API limits request to 120 per second. That is maxium 2 request per second, so sleep here.
    time.sleep(0.6)

    payload = {
        'apikey' : api_keys.APIKEY_MINUTE,
        'periodType' : 'day',
        'frequencyType': 'minute',
        'frequency': '1',
        'startDate': start_date,
        'needExtendedHoursData': 'true'
    }

    endpoint = TD_API.get_price_history_endpoint(symbol)

    try:
        td_response = requests.get(url=endpoint, params=payload)
        content = td_response.json()
        df = pd.DataFrame(content['candles'])
        df.to_csv(os.path.join(updates_folder, symbol + '.csv'), index=False)
    except Exception as ex:
        print(symbol + ' failed.')



def get_failed_symbols():
	# The difference between the update symbols and the stock list is the failed stocks.
	update_files = [os.path.join(updates_folder, f) for f in os.listdir(updates_folder) if f.endswith('.csv')]
    #update_files_symbols = os.path.splitext(f for f in update_files) 
	update_files_symbols = [os.path.splitext(f)[0] for f in [os.path.basename(f) for f in update_files]]

	failed_list = list(set(stock_list) - set(update_files_symbols))

	# Somehow, some stocks come with empty data. The request are successful, but the data is empty. 
	# We will re-try these stocks, along with the failed stocks if there are any.
	# Check if a dataframe is empty.
    #TODO: Change this into multiporcessing.
	empty_files = []
	for csv_file in update_files:
		try:
			df = pd.read_csv(csv_file)
		except pd.errors.EmptyDataError:
			# get only file name without path, without extension, only the symbol.
			# WARNING: don't use split('.') since there will be . in symbol.
			# empty_files.append(os.path.basename(csv_file).split('.')[0])
			empty_files.append(os.path.splitext(os.path.basename(csv_file))[0])
	
	# Combine the failed list and the empty files.
	retry_list = failed_list + empty_files

	return retry_list

if __name__ == '__main__':
    start_time = time.time #script start time counter.

    # Use 90 days before today as the start date of requesting data.
    start_date = str(time_utilities.eastern_to_epoch((datetime.datetime.today() - datetime.timedelta(days=90)).strftime('%Y-%m-%d 01:00:00')))

    # Create update forlder if not exist.
    today = time.strftime("%Y-%m-%d")
    updates_folder = os.path.join(Paths.Minute_Data_Update_Folder, today)
    if not os.path.exists(updates_folder):
        os.makedirs(updates_folder)
    
    stock_list = utilities.get_stock_list()

    progress_bar = tqdm(stock_list)
    for symbol in progress_bar:
        progress_bar.set_description("Requesting %s" % symbol)
        get_minute_data(symbol)
    

    # Retry failed symbols, which contians failed requests, and empty files.
    print('Retry failed symbols.')
    retry_list = get_failed_symbols()
    progress_bar = tqdm(retry_list)
    for symbol in progress_bar:
        progress_bar.set_description("Requesting %s" % symbol)
        get_minute_data(symbol)
    
    # If there are any failed symbols, we will write them to a file.
    failed_symobls = get_failed_symbols()

    if len(failed_symobls) > 0:
        with open(os.path.join(Paths.Minute_Data_Failed_List, today + '.txt'), 'w') as f:
            for symbol in failed_symobls:
                f.write(symbol + '\n')
    print("--- %s seconds ---" % (time.time() - start_time))

    
            

    










