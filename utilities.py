# This files contians some utilities functions used across the project.
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import Paths
import TD_API
import api_keys

# Get the most recent stock list file.
# list all files ends with .csv in given directory
def get_stock_list():
	files = [f for f in os.listdir(Paths.Stocks_List) if os.path.isfile(os.path.join(Paths.Stocks_List, f))]
	files = [f for f in files if f.endswith('.csv')]
	files.sort(reverse=True)
	stock_list_df = pd.read_csv(os.path.join(Paths.Stocks_List, files[0])) 
	stock_list =  stock_list_df['symbol'].tolist()
	stock_list.sort()
	return stock_list

def get_file_name_from_full_path(path):
	return os.path.basename(path)

def get_symol_name_from_file_name(file_name):
	os.path.splitext(file_name)[0]

def get_symbol_name_from_full_path(path):
	return get_symol_name_from_file_name(get_file_name_from_full_path(path))

def list_subfolders(folder):
	return [f.path for f in os.scandir(folder) if f.is_dir()]
