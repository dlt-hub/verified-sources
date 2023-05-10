"""
	Loads all the search hits for specific keywords from the Algolia Hacker News Search API
	https://hn.algolia.com/api
"""
import dlt
import time

from datetime import datetime
from dlt.sources.helpers import requests


url = "https://hn.algolia.com/api/v1/search_by_date"

@dlt.source
def algolia_hn_search(
	keywords, # list of keywords for which the data needs to be loaded
	start_date, # start date in datetime or "yyyy-mm-dd" format
	end_date=datetime.today(), # end date in datetime or "yyyy-mm-dd" format
	text="any", # possible values: "story","comment". For any other value, everything is loaded.
	daily_load=False # loads data on a daily schedule when set to True
):

	"""
		Source method for the Algolia Hacker News Search API: https://hn.algolia.com/api
	"""	

	# Read start date as string or datetime and convert it to UNIX timestamp
	if isinstance(start_date,str):
		start_timestamp = int(time.mktime(
			datetime.strptime(start_date,"%Y-%m-%d").timetuple()
		))
	else:
		start_timestamp = int(
			time.mktime(start_date.timetuple())
		)

	# Read end date as string or datetime and convert it to UNIX timestamp
	if isinstance(end_date,str):
		end_timestamp = int(time.mktime(
			datetime.strptime(end_date,"%Y-%m-%d").timetuple()
		))
	else:
		end_timestamp = int(
			time.mktime(end_date.timetuple())
		)

	today = int(time.mktime(datetime.today().timetuple()))

	# Don't load the data for dates after the current date
	end_timestamp = min(today,end_timestamp)

	# Ensure that the input start date is smaller than the input end date
	if start_timestamp > end_timestamp:
		raise Exception(f"""
			start_date {start_date} is larger than end_date {end_date}.\n
			Ensure that the start_date is smaller or equal to than the end_date.\n
			Also ensure that the start_date is smaller than or equal to today {today}.
		""")

	# Specify text = "comment" or text="story" when calling the function
	# to load only comments or stories
	if text in ["comment","story"]:
		tags = text
	# Pass any other value to load everything (default behaviour)
	else: 
		tags = '(story,comment)'

	return keyword_hits(keywords,start_timestamp,end_timestamp,tags,daily_load)


@dlt.resource(write_disposition="append")
def keyword_hits(
	keywords, # list of keywords for which the data needs to be loaded
	start_timestamp, # UNIX timestamp for the start date
	end_timestamp, # UNIX timestamp for the end date
	tags, # parameter for the API call to specify "story", "comment" or "(story,comment)"
	daily_load=False # loads data on a daily schedule when set to True
):
	
	"""
		This methods makes a call to the Algolia Hacker News
		and returns all the hits corresponding the the input keywords

		Since the API response is limited to 1000 hits,
		a separate call is made for each keyword for each week between the start and end dates

		If daily_load=True, then a single call is made for eaach keyword for the previous day
	"""

	def _generate_hits(keyword,batch_start_date,batch_end_date,tags):
		"""
			This function makes the API call and returns all the hits for the input parameters
		"""
		params = {
            "query": f'"{keyword}"',
            "tags": f"{tags}",
            "numericFilters": f"""created_at_i>={batch_start_date},created_at_i<{batch_end_date}""",
            "hitsPerPage": 1000,
		}
		response = requests.get(url,params=params)
		response.raise_for_status()

		return response.json()['hits']

	
	time_delta = 86400 if daily_load else 604800 # The length of a day/week in UNIX timestamp
	
	# Iterate across all keywords
	for keyword in keywords:

		batch_start_date = start_timestamp
		batch_end_date = batch_start_date + time_delta

		# Iterate across each week between the start and end dates
		while batch_end_date < end_timestamp+time_delta:
			batch_end_date = min(batch_end_date,end_timestamp) # Prevent loading data ahead of the end date
			# The response json
			data = _generate_hits(keyword,batch_start_date,batch_end_date,tags) 

			for hits in data:
				yield {
					key:value for (key,value) in hits.items() 
					if not key.startswith("_") # Filtering down to relevant fields from the response json
				}

			batch_start_date = batch_end_date
			batch_end_date += time_delta
