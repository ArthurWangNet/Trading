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
import tqdm

# We first need a function which will get all eod data up to today. This function will run at the first time, and will run every time a split happens.

def get_eod_data_from_start(symbol):
	#Define payload
	payload = {
		'apikey': api_keys.APIKEY_DAILY,
		'periodType' : 'ytd',
        'period' : '1',
        'frequencyType': 'daily',
        'frequency':'1',
		'startDate': '21600000' #This is 1970-01-01
	}
	endpoint = TD_API.get_price_history_endpoint(symbol)

	#Get data
	try:
		time.sleep(0.6)
		td_response = requests.get(url=endpoint, params=payload)
		content = td_response.json()
		df = pd.DataFrame(content['candles'])
		df.to_csv(os.path.join(updates_folder, symbol + '.csv'), index=False)
		return None
	except:
		print(symbol + " failed.")
		return symbol


if __name__ == '__main__':
	start_time = time.time()
	# today's date used as folder name.
	today = time.strftime("%Y-%m-%d")
	updates_folder = os.path.join(Paths.Daily_Data_Update_Folder, today)
	# Create update folder if it does not exist.
	if not os.path.exists(updates_folder):
		os.makedirs(updates_folder)
	
	# Getting the latest stock list.
	stock_list = utilities.get_stock_list()
	failed_list = []
	progress_bar = tqdm(stock_list)
	for symbol in progress_bar:
		# Show progress of the loop
		progress_bar.set_description("Requesting %s" % symbol)
		failed = get_eod_data_from_start(symbol)
		if failed is not None:
			failed_list.append(failed)

# If there are any failed symbols, we will write them to a file.
	if len(failed_list) > 0:
		with open(os.path.join(Paths.Daily_Data_Failed_List, today + '.txt'), 'w') as f:
			for symbol in failed_list:
				f.write(symbol + '\n')
	# Print the time it took to run the script.			
	print("--- %s seconds ---" % (time.time() - start_time))