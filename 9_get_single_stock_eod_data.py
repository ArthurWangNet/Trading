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
		df.to_csv('./'+symbol+'.csv')
	except:
		print(symbol + " failed.")

def get_failed_symbols():
	# The difference between the update symbols and the stock list is the failed stocks.
	update_files = [os.path.join(updates_folder, f) for f in os.listdir(updates_folder) if f.endswith('.csv')]
#	update_files_symbols = os.path.splitext(f for f in update_files) 
	update_files_symbols = [os.path.splitext(f)[0] for f in [os.path.basename(f) for f in update_files]]

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
			# WARNING: don't use split('.') since there will be . in symbol.
			# empty_files.append(os.path.basename(csv_file).split('.')[0])
			empty_files.append(os.path.splitext(os.path.basename(csv_file))[0])
	
	# Combine the failed list and the empty files.
	retry_list = failed_list + empty_files

	return retry_list

if __name__ == '__main__':
	# today's date used as folder name.
	# Create update folder if it does not exist.

	
	symbol = 'AAPL'
	get_eod_data_from_start(symbol)








	
