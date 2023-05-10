import dlt
import time

from datetime import datetime, timedelta
from algolia_hn_search import algolia_hn_search


keywords = ["data loading", "data load tool","load"] # Specify your list of keywords here

def load_data_by_date():
	'''
		Loads all the hits between start_date and end_date
		that are an exact match for the specified keywords
	'''
	start_date='2022-01-01'
	# end_date='2023-05-10'
	pipeline = dlt.pipeline(
		pipeline_name="algolia_hn_search",
		destination="duckdb",
		dataset_name="hn_search_data"
	)

	# By default the end_date is today
	# To specify a different end_date, pass it as a parameter in "yyyy-mm-dd" format
	data = algolia_hn_search(
		keywords=keywords,
		start_date=datetime.strptime(start_date,"%Y-%m-%d"), 
		# end_date=end_date,
		text="any" # Change this to "story" to load only stories or to "comment" to load only comments
	)
	info = pipeline.run(data)

	print(info)

def load_new_data():
	'''
		Loads all the data from yesterday
		that are an exact match for the specified keywords
	'''
	pipeline = dlt.pipeline(
		pipeline_name="algolia_hn_search",
		destination="duckdb",
		dataset_name="hn_search_data"
	)

	yesterday = datetime.today() - timedelta(days=1)
	start_date = datetime(
		yesterday.year, yesterday.month, yesterday.day, 0, 0, 0
	)# First timestamp for yesterday
	end_date = datetime(
		yesterday.year, yesterday.month, yesterday.day, 23, 59, 59
	)# Last timestamp for yesterday

	data = algolia_hn_search(
		keywords=keywords,
		start_date=start_date,
		end_date=end_date,
		text="any", # Change this to "story" to load only stories or to "comment" to load only comments
		daily_load=True
	)
	info = pipeline.run(data)

	print(info)


if __name__ == '__main__':
	## First run load_data_by_date() to backfill all data between the start and end dates
	## Then run load_new_data() on schedule to load fresh data daily

	start = time.time()
	load_data_by_date() 
	# load_new_data() 
	print(f"Time taken: {time.time() - start}")
