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
	except:
		print(symbol + " failed.")

def get_failed_symbols():
	# The difference between the update symbols and the stock list is the failed stocks.
	update_files = [os.path.join(updates_folder, f) for f in os.listdir(updates_folder) if f.endswith('.csv')]
	update_files_symbols = [f.split('.')[0] for f in os.listdir(updates_folder) if f.endswith('.csv')]
	failed_list = list(set(stock_list) - set(update_files_symbols))

	# Somehow, some stocks come with empty data. The request are successful, but the data is empty. 
	# We will re-try these stocks, along with the failed stocks if there are any.
	# Check if a dataframe is empty.
	empty_files = []
	for csv_file in update_files:
		try:
			df = pd.read_csv(csv_file)
		except pd.errors.EmptyDataError:
			# get only file name without path, without extension, only the symbol.
			empty_files.append(os.path.basename(csv_file).split('.')[0])
	
	# Combine the failed list and the empty files.
	retry_list = failed_list + empty_files

	return retry_list

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

	progress_bar = tqdm(stock_list)
	for symbol in progress_bar:
		# Show progress of the loop
		progress_bar.set_description("Requesting %s" % symbol)
		get_eod_data_from_start(symbol)


	# Retry failed symbols, which contians failed requests, and empty files.
	print("Retrying failed symbols...")
	retry_list = get_failed_symbols()
	progress_bar = tqdm(retry_list)
	for symbol in progress_bar:
		progress_bar.set_description("Requesting %s" % symbol)
		get_eod_data_from_start(symbol)

	# If there are any failed symbols, we will write them to a file.
	failed_symobls = get_failed_symbols()
	if len(failed_symobls) > 0:
		with open(os.path.join(Paths.Daily_Data_Failed_List, today + '.txt'), 'w') as f:
			for symbol in failed_symobls:
				f.write(symbol + '\n')

	# Print the time it took to run the script.			
	print("--- %s seconds ---" % (time.time() - start_time))







	
