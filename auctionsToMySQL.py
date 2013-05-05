# Timestamp (line 62) needs date formatting fixed
# * id
# * player
# * created
# * expires (may be unnecessary - not sure if time limits are constant)
# * disappeared (approx.) (may be blank)
# * class_tsid
# * category
# * count
# * cost
# * tool_state (default 0)
# * tool_uses  (default 0)
# * tool_capacity (default 0)
# * url
# * state ('active','canceled','expired','sold')

from datetime import datetime
import json
import MySQLdb as mdb
from os import listdir, remove
from subprocess import call,popen
from sys import exit
from time import localtime, sleep

AUCTIONS_DB_LOCATION = '<Put database path here>'
DB_USER = '<Put username here'
DB_PASSWORD = '<Put password here>'
DB_SCHEMA = 'glitch'
GET_POSSIBLY_EXPIRED_AUCTIONS_QUERY = 'select id,expires from auctions where state=\'active\' and datediff(expires,created) <= 0'
GET_ACTIVE_AUCTIONS_QUERY           = 'select id from auctions where state=\'active\' and datediff(expires,created) >  0'
INSERT_QUERY_PREFIX                 = 'insert into auctions (id,player,created,expires,class_tsid,category,count,cost,tool_state,tool_uses,tool_capacity,url) values '
RELATIVE_DATA_PATH = '../unedited/'
SLEEP_TIME = 10
MAX_PROCESSES = 1000

def getSQLdateFromEpoch(epochTime):
	return ''.join([str(entry) for entry in localtime(epochTime)[:6]])

processList = []
def spawnAuctionUpdate(id,timestamp):
	while len(processList) >= MAX_PROCESSES:
		for process in processList[:]:
			status = process.poll()
			if status == None:
				processList.pop(process)
		if len(processList) >= MAX_PROCESSES:
			sleep(SLEEP_TIME/2)
	processList.append(popen(['python','updateAuctionRecord.py',id,timestamp])

ls = listdir(RELATIVE_DATA_PATH)

if len(ls) <= 1:
	exit(0)
	
# Initialize database connection
try:
	con = mdb.connect(AUCTIONS_DB_LOCATION,DB_USER,DB_PASSWORD,DB_SCHEMA)
	cur = con.cursor()
except mdb.Error, e
	print 'Error', str(e.args[0]), ':', e.args[1]
	print 'Could not open database connection, so exiting'
	exit(1)	
	
# Get list of all auctions that may have expired
try:
	cur.execute(GET_POSSIBLY_EXPIRED_AUCTIONS_QUERY)
	results = cur.fetchall()
except mdb.Error, e
	print 'Error', str(e.args[0]), ':', e.args[1]
	print 'Could not get list of possibly expired auctions, so exiting'
	exit(1)	

for row in results:
	# Call secondary script to determine whether auction was canceled, completed, or expired.
	# Secondary script gets auction id and auction's expiration time
	id = row[1]
	timestamp= row[2] # NEEDS REFORMATTING
	spawnAuctionUpdate(id,timestamp)

# get list of all active auctions
try:
	cur.execute(GET_ACTIVE_AUCTIONS_QUERY)	
	results = cur.fetchall()
except mdb.Error, e
	print 'Error', str(e.args[0]), ':', e.args[1]
	print 'Could not get list of active auctions, so exiting'
	exit(1)
	
# Move results to a better data structure (something that can be extended)
lastAuctions = {}
for row in results:
	lastAuctions[row[1]] = 1
	
for filename in ls[:-1]:
	# get the time of the read
	timeStr = filename[:-4]
	yearMonthDay,hourMinuteSeconds = timeStr.split('_')
	year,month,day = yearMonthDay.split('-')
	hour,minute,secondsMilliseconds = hourMinuteSeconds.split(':')
	seconds,milliseconds = secondsMilliseconds.split('.')

	auctionTime = datetime(int(year),int(month),int(day),int(hour),int(minute),int(seconds),int(milliseconds))
	auctionSecs = mktime(auctionTime.timetuple())

	# Load all of the auctions
	text = open(RELATIVE_DATA_PATH+filename,'r').read()
	data = json.loads(text)
	
	if data["pages"] == 1:
		# When requesting auction data, we intentionally collect all auctions
		# on single pages. If a particular file has multiple pages of auctions
		# something has gone wrong
		
		curAuctions = data["items"]
	
		for key in lastAuctions.keys():
			if key not in curAuctions:
				# Call secondary script to determine whether auction was canceled, completed, or expired.
				# Secondary script gets auction id and time that auction list was read.
				spawnAuctionUpdate(key,getSQLdateFromEpoch(auctionSecs))
				del lastAuctions[key]
				
		newAuctions = []
		for key in curAuctions:
			if key not in lastAuctions:
				# Add key to list
				# Parse auction data
				# Write auction into database
				lastAuctions[key] = 1
				
				# add record to new auctions
				
				try:
					tool_state    = curAuctions["tool_state"]
					tool_uses     = curAuctions["tool_uses"]
					tool_capacity = curAuctions["tool_capacity"]
				except:
					tool_state     = 0
					tool_uses      = 0
					tool_capacity  = 0
				
				newAuctions.append('('+', '.join([
					"'"+key                                                   +"'",
					"'"+curAuctions[key]["player"]["tsid"]                    +"'",
					"'"+getSQLdateFromEpoch(int(curAuctions[key]["created"])) +"'",
					"'"+getSQLdateFromEpoch(int(curAuctions[key]["expires"])) +"'",
					"'"+curAuctions[key]["class_tsid"]                        +"'",
					"'"+curAuctions[key]["category"]                          +"'",
					"'"+curAuctions[key]["count"]                             +"'",
					"'"+curAuctions[key]["cost"]                              +"'",
					    str(tool_state),
					    str(tool_uses),
					    str(tool_capacity),
					"'"+curAuctions["url"].replace('\\','')                   +"'"])+')')
				
		# Insert all new auctions into database
		try:
			cur.execute(INSERT_QUERY_PREFIX+', '.join(newAuctions))
		except mdb.Error, e
			print 'Error', str(e.args[0]), ':', e.args[1]
			print 'Could not insert processed auctions, so exiting.'
			exit(1)
			
		del newAuctions
	