import pymongo
import urllib.parse

import time

# credentials
username = urllib.parse.quote_plus('mongo')
password = urllib.parse.quote_plus('ananke')

# connect to DB
client = pymongo.MongoClient('mongodb://{}:{}@localhost:27017/'.format(username,password))
db = client["ananke"]
# col = db["provenance_tuples"]
col = db["ananke"]

while True:

	# get all sinks, sources and edges that have not yet been sent
	sinks_sources_edges = col.find(
		{ 
			"sent" : False, 
			"$or" : [{"type" : "sink"}, {"type" : "edge"}, {"type" : "source"}],
		})

	# mark those retrieved as already sent
	col.update_many(
		{
			"sent" : False, 
			"$or" : [{"type" : "sink"}, {"type" : "edge"}, {"type" : "source"}],
		},
		{
			"$set" : {"sent" : True},
		}) 

	# obtain the lowest safe_ts among all sinks
	lowest_ts = col.aggregate( [
		{ 
			"$match" : { "type" : "safe_ts" },
		},
		{
			"$group" : {
							"_id" : None,
							"min" : { "$min" : "$ts" }
			}
		}
		 ] )

	lowest_ts_data = 0
	try:
		lowest_ts_data = list(lowest_ts)[0]["min"]
	except:
		pass

	# send out acks for sources
	acks = col.find(
		{
			"type" : "source",
			"ts" : { "$lt" : lowest_ts_data},
			"sent" : True,
			"expired": False,
		})


	col.update_many(
		{
			"type" : "source",
			"ts" : { "$lt" : lowest_ts_data},
			"sent" : True,
			"expired": False,
		},
		{
			"$set" : {"expired" : True},
		}) 


	with open("mongo_out.txt", "a") as outf:
		for element in sinks_sources_edges:
			print(element, file=outf)
		for element in acks:
			print(element, file=outf)


