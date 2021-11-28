# This script will run after get fundamental data is done. It will update the latests data to production files.
import os
import Paths
import datetime
import pandas as pd
from multiprocessing import Pool
import time

# list all sub folders in the given folder
def list_subfolders(folder):
	return [f.path for f in os.scandir(folder) if f.is_dir()]

def update_csv(csv_file):
	"""
	This function will update the csv file to the latests data.
	Taking each csv file from the update csv file list, it will try to match existing file with the same name in production folder.
	If there is a match, it will update the existing file with the latests data, by appending the latests data to the existing file.
	If there is no match, it will create a new file with the latests data.

	: param csv_file: csv file to be updated
	: return: None
	"""
	if csv_file in production_csv_files:
				#print(csv_file + " already in production folder, updating.")
			# append the update csv file to the production csv file
			df_update = pd.read_csv(os.path.join(folder, csv_file))
			# Add new column to the dataframe with value, this is the update date stamp
			df_update['Update_Date'] =update_time_stamp 

			#For the production files:
			df_production = pd.read_csv(os.path.join(Paths.Fundamental_Data_Production_Folder, csv_file))
			df_production = df_production.append(df_update)
			df_production.to_csv(os.path.join(Paths.Fundamental_Data_Production_Folder, csv_file), index=False)
			#	print(csv_file + " updated.")
	else:
		# create new csv file in the production folder with the update date stamp
			#print(csv_file + " not in production folder, creating new file.")
		df_update = pd.read_csv(os.path.join(folder, csv_file))
		# Add new column to the dataframe with value, this is the update date stamp
		df_update['Update_Date'] =update_time_stamp
		df_update.to_csv(os.path.join(Paths.Fundamental_Data_Production_Folder, csv_file), index=False)
			#print(csv_file + " created.")

def multi_update():
	"""
	This function will using multiprocessing to update the csv files in the update csv file list by calling the update_csv function.
	: return: None
	"""
	#Using multiprocessing to speed up the process
	pool = Pool(os.cpu_count())
	pool.map(update_csv, update_csv_files)
	pool.close()
	pool.join()



if __name__ == '__main__':
	start_time = time.time()
	update_folders = list_subfolders(Paths.Fundamental_Data_Update_Folder)
	update_folders.sort()

	# Check each folder in the update folder, which should be sorted by date, and process them one by one
	for folder in update_folders:
		# Get update folder names from path, it will be a update date stamp.
		update_time_stamp= folder.split('/')[-1]
		print("Checking update folder: " + update_time_stamp)
		# Get files from folder which ends with csv and store them into a list
		update_csv_files = [f for f in os.listdir(folder) if f.endswith('.csv')]
		production_csv_files = [f for f in os.listdir(Paths.Fundamental_Data_Production_Folder) if f.endswith('.csv')]

		print("Start Multiprocessing for folder: " + update_time_stamp)
		# Call the multi_update function to update the csv files in the update csv file list
		multi_update()
		
		print("Finished Multiprocessing for folder: " + update_time_stamp)
		#Move the update folder and all its files to the archive folder
		os.rename(folder, os.path.join(Paths.Fundamental_Data_Archived_Folder, update_time_stamp))
		print("Update folder moved to archive folder.")

	print("--- %s seconds ---" % (time.time() - start_time))