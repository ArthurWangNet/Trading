"""
This script is used to initialize the production database with the EOD data. 
The first run time is based on the data gathered at 2021-11-28. Which should include back to around 1980.
This script is not intent to run on a regular basis. It is intended to be run once.
"""

import os
import Paths
import datetime
import pandas as pd
import utilities
import time_utilities
from multiprocessing import Pool
import time
import tqdm




#NOTE: Be very careful when handling these file names.
# Using '.' as separator is not a good idea, since some files have '.' in the name.


def prepare_production_data(csv_file_path):
	try:
		# get the file name
		# WARNING: Do not use '.' as separator, since some files have '.' in the name.
		symbol = os.path.basename(csv_file_path)
		#print("Processing:", symbol)
		df_update = pd.read_csv(csv_file_path)
		# Rename 'datetime' column to 'timestamp'
		df_update.rename(columns={'datetime': 'timestamp'}, inplace=True)
		# Convert timestamp to datetime
		df_update['datetime'] = df_update['timestamp'].apply(lambda x: time_utilities.epoch_to_eastern(x))
		# sort by timestamp in descending order and save to csv
		df_update.sort_values(by=['timestamp'], ascending=False, inplace=True)
		# Save csv to production folder
		df_update.to_csv(os.path.join(Paths.Daily_Data_Production_Folder, symbol), index=False)
	except pd.errors.EmptyDataError:
		pass


if __name__ == "__main__":
	# Inital update folder:
	update_data_folder = "./Data/Daily/Updates/2021-11-28"
	update_files = [os.path.join(update_data_folder, f) for f in os.listdir(update_data_folder) if f.endswith('.csv')]
	# Start timer
	print("Started at:", datetime.datetime.now())
	# Get timmer
	start_time = time.time()

	pool = Pool(os.cpu_count())

	for _ in tqdm.tqdm(pool.imap_unordered(prepare_production_data, update_files), total=len(update_files)):
		pass

	pool.close()
	pool.join()	

	# End timer
	end_time = time.time()
	print("Time taken: ", end_time - start_time)