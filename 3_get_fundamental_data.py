from click import progressbar
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

from tqdm import tqdm
import Paths
import TD_API
import api_keys
import time
import requests
import utilities

# This function will using td api to get fundamental data for the symbol provided.
def get_fundamental_update(symbol):
    #TD API limits request to 120 per second. That is maxium 2 request per second, so sleep here.
    time.sleep(0.6)
    #Define payload
    payload = {
            'apikey' : api_keys.APIKEY_FUNDAMENTAL,
            'symbol': symbol,
            'projection':'fundamental',
    }

    try:
        td_response = requests.get(url=TD_API.get_fundamental_endpoint(), params=payload)
        content = td_response.json()
        content_dict = content[symbol]['fundamental']
        df = pd.DataFrame(content_dict, index=[0])
        return df
    except Exception as ex:
        return ex

# get latest stock symbol list
stock_list = utilities.get_stock_list()

# Prepare storage directory. Create a new folder with the current date in update folder of fundamental data.
today = time.strftime("%Y-%m-%d")
updates_folder = os.path.join(Paths.Fundamental_Data_Update_Folder, today)
if not os.path.exists(updates_folder):
	os.makedirs(updates_folder)


"""
This script will try to get fundamental data for 4 times. 
During the process, the successed stock will be removed from the list, so next try will only on stocks that failed last time.
After 4 tries, the remaining stocks will be write to fail list file, stamped with date and stored in Report directory.
"""
for i in range(4):
	print('Try {}'.format(i))
	stock_list_length = len(stock_list)
	progressbar = tqdm(stock_list)
	for symbol in progressbar:
		# print progress of the process
		#percentage = round(100*(stock_list_length-len(stock_list))/stock_list_length)
		#print('Working on ' + symbol + ' ' + str(percentage) + '%' + ': {}/{}'.format(stock_list.index(symbol), len(stock_list)))
		progressbar.set_description("Requesting %s" % symbol)
		try:
			df = get_fundamental_update(symbol)
			df.to_csv(os.path.join(updates_folder, symbol + '.csv'))
			# remove the stock from the list
			stock_list.remove(symbol)
		except Exception as ex:
			print(symbol + " failed")
	if len(stock_list) == 0:
		break

# If the stock list is not empty, write to fail list file in txt format.
if len(stock_list) > 0:
	with open(os.path.join(Paths.Fundamental_Data_Failed_List, today + '.txt'), 'w') as f:
		for symbol in stock_list:
			f.write(symbol + '\n')