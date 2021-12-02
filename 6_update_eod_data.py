import os
import time
import datetime
import pandas as pd
import numpy as np
import requests
import json
from multiprocessing import Pool
import Paths
import time_utilities
import utilities
import tqdm


def update_eod_data(csv_file):
	try:
		df_update = pd.read_csv(os.path.join(folder,csv_file))
		# Rename 'datetime' column to 'timestamp'
		df_update.rename(columns={'datetime': 'timestamp'}, inplace=True)
		# Convert timestamp to datetime
		df_update['datetime'] = df_update['timestamp'].apply(lambda x: time_utilities.epoch_to_eastern(x))
		# sort by timestamp in descending order and save to csv
		df_update.sort_values(by=['timestamp'], ascending=False, inplace=True)
		# Save csv to production folder
		df_update.to_csv(os.path.join(Paths.Daily_Data_Production_Folder, csv_file), index=False)
	except pd.errors.EmptyDataError:
		pass

def get_empty_files(csv_file):
	try:
		df = pd.read_csv(os.path.join(folder, csv_file))
		if df.empty:
			# empty_files.append(csv_file)
			return csv_file
	except pd.errors.EmptyDataError:
			# empty_files.append(csv_file)
			return csv_file





if __name__ == '__main__':
	print("script start at :", datetime.datetime.now())
	script_start_time = time.time()
	update_folders = utilities.list_subfolders(Paths.Daily_Data_Update_Folder) #This retruns full paths of the folders.
	update_folders.sort()

	for folder in update_folders:
		update_time_stamp = folder.split('/')[-1]
		print("Checking update folder: " + update_time_stamp)

		# Get files from folder which ends with csv and store them into a list
		update_csv_files = [f for f in os.listdir(folder) if f.endswith('.csv')] # Return filenames without full path.
		update_csv_files.sort()
		production_csv_files = [f for f in os.listdir(Paths.Daily_Data_Production_Folder) if f.endswith('.csv')] # Return filenames without full path.
		production_csv_files.sort()

		# We will get empty files first, it only takes around 20s so I won't use multiprocessing here.
		print("Checking empty files...: ")
		empty_files = []
		pool = Pool(processes=os.cpu_count())	
		empty_files = [f for f in tqdm.tqdm(pool.imap_unordered(get_empty_files, update_csv_files), total=len(update_csv_files))]
		#empty_files = pool.map(get_empty_files, update_csv_files)
		pool.close()
		pool.join()
		empty_files = [f for f in empty_files if f is not None]
		#empty_files = result.get(p.get() for p in result)
		
		# for file in progress_bar:
		# 	try:
		# 		df = pd.read_csv(os.path.join(folder, file))
		# 		if df.empty:
		# 			empty_files.append(file)
		# 	except pd.errors.EmptyDataError:
		# 		empty_files.append(file)
		
		# Check if there are any empty files already in production, but somehow is empty this time.
		new_empty_symbol = list(set(empty_files) & set(production_csv_files))

		# Check if there are any missing files, that is files in production but not in update folder.
		missing_files = list(set(production_csv_files) - set(update_csv_files))

		# Check if there any new symobls been added, that is files in update without empty, but not in production.
		# Also, working files are the files we really need to process and update.
		working_files = list(set(update_csv_files) - set(empty_files))
		new_symbol = list(set(working_files) - set(production_csv_files))

		# We can update tracker file now.
		tracker = pd.read_csv(Paths.Daily_Data_Tracker)
		tracker = tracker.append({
			'Date': update_time_stamp,
			'New Symbol': new_symbol,
			'Missing Symbol': missing_files,
			'Empty Symbol': empty_files,
			'New Empty Symbol': new_empty_symbol
		}, ignore_index=True)
		tracker.to_csv(Paths.Daily_Data_Tracker, index=False)

		# Now we can start to update the data.
		start_time = time.time()
		print("Start multiprocessing data in folder " + folder + " at " + str(datetime.datetime.now()))
		pool = Pool(os.cpu_count())
		for _ in tqdm.tqdm(pool.imap_unordered(update_eod_data, working_files), total=len(working_files)):
			pass
		pool.close()
		pool.join()
		end_time = time.time()
		print("Time taken to process data in folder " + folder + " is" +  str(end_time - start_time))

		# Move the updated folder to archived folder.
		os.rename(folder, os.path.join(Paths.Daily_Data_Archived_Folder, update_time_stamp))
		print("Folder " + folder + " has been moved to archived folder.")
	
	print("Total time taken to process all data is " + str(time.time() - script_start_time))















