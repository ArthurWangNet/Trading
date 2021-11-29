# Convert milliseconds epoch time into GMT time
# The return is in datatime format, not string.
# Test:
# Epoch: 968907600000
# GMT: 2000-09-14 05:00:00
# NYT: 2000-09-14 01:00:00
# Some referneces:
# https://stackoverflow.com/questions/13866926/is-there-a-list-of-pytz-timezones
# https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
# https://stackoverflow.com/questions/36797566/how-to-correct-wrong-timezone-offset-in-pytz-and-django
import datetime
import pytz

def epoch_to_eastern(epoch_time):
	tz = pytz.timezone('America/New_York')
	dt = datetime.datetime.fromtimestamp(epoch_time/1000, tz)
	return dt.strftime('%Y-%m-%d %H:%M:%S')

def eastern_to_epoch(eastern_time):
	dt = datetime.datetime.strptime(eastern_time, '%Y-%m-%d %H:%M:%S')
	dt = pytz.timezone('America/New_York').localize(dt)
	return int(dt.astimezone(pytz.utc).timestamp() * 1000)


"""
Function below has problems with offset. Need more digging. Reference to :
https://www.codenong.com/3acfca4f386b74a6ffca/
https://stackoverflow.com/questions/27531718/datetime-timezone-conversion-using-pytz

In short, try to use localize() instead of replace()
"""


# def epoch_to_gmt(epoch_time):
# 	gmt = pytz.timezone('GMT')
# 	return datetime.datetime.fromtimestamp(epoch_time/1000, gmt)

# # Convert GMT datetime to US/Eastern datetime
# # The return is in datatime format, not string.
# def gmt_to_eastern(gmt_time):
# 	eastern = pytz.timezone('US/Eastern')
# 	return gmt_time.astimezone(eastern)

# # Convert US/Eastern datetime to GMT datetime
# # Input is in string format in YYYY-MM-DD HH:MM:SS format, convert it to datetime format
# # The return is in datatime format, not string.
# def eastern_to_gmt(eastern_time):
# 	eastern = pytz.timezone('Etc/GMT+5') # The US/Eastern timezone is GMT+5, if using US/Eastern as name, the STD is offset to 04:56, which causes all sort of trouble.
# 	eastern_time = datetime.datetime.strptime(eastern_time, '%Y-%m-%d %H:%M:%S')
# 	eastern_time = eastern_time.replace(tzinfo=eastern)
# 	return eastern_time.astimezone(pytz.timezone('GMT'))


# # Convert GMT time to Epoch time in milliseconds
# # The return is in epoch time, not datatime format.
# def gmt_to_epoch(gmt_time):
# 	gmt = pytz.timezone('GMT')
# 	return int(gmt_time.astimezone(pytz.utc).timestamp() * 1000)





