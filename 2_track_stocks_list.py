# The purpose of this script is to track the stocks list changes
# It should be run every time after the get_stock_list.py been run, and compare the latest stock list with the previous one.
# It will write into a tracker file called "tracker.csv", that file will contian a dataframe which have columns:
# Date,TotalNum,Added Stocks,Missing Stocks
# Added Stocks means the latest stock list contains stocks that are not in the previous one.
# Missing Stocks means the previous stock list contains stocks that are not in the latest one.

import pandas as pd
import os
import Paths

def track_stock_list():
	# Get all .csv files in the specified directory and store them into a list	
	csv_files = []
	print("Getting all .csv files in the specified directory...")
	for file in os.listdir(Paths.Stocks_List):
		if file.endswith(".csv"):
			csv_files.append(os.path.join(Paths.Stocks_List, file))
	print('Done!')

	# Since the files are sotred in YYYY-MM-DD.csv format, we will just sort the list decreasingly so the latest file is on the top.
	# Now we will readin the latest and previous list to compare the difference.
	csv_files.sort(reverse=True)
	print("The latest file is: " + os.path.basename(csv_files[0]))
	latest = pd.read_csv(csv_files[0])

	print("The previous file is: " + os.path.basename(csv_files[1]))
	previous = pd.read_csv(csv_files[1])

	# Convert symbol column to a list
	print('Convert symbol column to a list...')
	latest_list = latest['symbol'].tolist()
	previous_list = previous['symbol'].tolist()
	print('Done')

	print('Analysis the differences...')
	TotalNum = len(latest_list)
	# Compare the counting of the symbols in the latest list and previous list
	DifferenceCount = len(latest_list) - len(previous_list)

	# Getting differences.
	new_stocks = []
	for item in latest_list:
		if item not in previous_list:
			new_stocks.append(item)
	
	missing_stocks = []
	for item in previous_list:
		if item not in latest_list:
			missing_stocks.append(item)
	print('Done!')

	# Print the differences to log
	if len(new_stocks) > 0:
		print(str(len(new_stocks)) + ' new sybmols found:')
		for item in new_stocks:
			print(item)
	
	if len(missing_stocks) > 0:
		print(str(len(missing_stocks)) + 'symbols missing:')
		for item in missing_stocks:
			print(item)



	# The date will just be the same date as on the filename.
	base_date = os.path.splitext(os.path.basename(csv_files[0]))[0]

	# Update Tracker file.
	print('Update Tracker file...')
	tracker = pd.read_csv(Paths.Stocks_List_Tracker)
	tracker = tracker.append({
		'Date':base_date,
		'TotalNum':TotalNum,
		'Difference Count': DifferenceCount,
		'Added Stocks':new_stocks,
		'Missing Stocks':missing_stocks},ignore_index=True)
	tracker.to_csv(Paths.Stocks_List_Tracker,index=False)
	print('Done!')


if __name__ == '__main__':
	track_stock_list()