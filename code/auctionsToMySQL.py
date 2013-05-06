# Don't know why queries that have been disappeared sometimes reappear.
# it's fixed, but _don't_know_why_it_occurs_. This may have ramiifications later.

# FIXED failure to log tool_state, BUT this will now result in illegal values being passed to mysqul. ("used" and "new" in tool_state


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

from BeautifulSoup import BeautifulSoup
from datetime import datetime
import json
import MySQLdb as mdb
from os import listdir, mkdir, rename
from os.path import getsize, exists
from subprocess import call,Popen
from sys import exit
from time import localtime, mktime, sleep

import curlAuctionPages

def getSQLdateFromEpoch(epochTime):
	return datetime.fromtimestamp(epochTime).strftime("%Y-%m-%d %H:%M:%S")

AUCTIONS_DB_LOCATION = 'edgar.casos.cs.cmu.edu'
DB_USER = 'plandweh'
DB_PASSWORD = 'askmefi=gr34t'
DB_SCHEMA = 'glitch'
#GET_POSSIBLY_EXPIRED_AUCTIONS_QUERY = "select id from auctions where isnull(disappeared) and datediff(expires,'"+getSQLdateFromEpoch(mktime(localtime()))+"') <= 0"
GET_ACTIVE_AUCTIONS_QUERY = "select id from auctions where isnull(disappeared)"    
INSERT_QUERY_PREFIX = 'insert into auctions (id,player,created,expires,class_tsid,category,count,cost,tool_state,tool_uses,tool_capacity,url) values '
INSERT_QUERY_SUFFIX = ' on duplicate key update disappeared=null'
RELATIVE_DATA_PATH      = '../unedited/' #'../test/'
RELATIVE_PROCESSED_PATH = '../processed/'
RELATIVE_DELETION_PATH  = '../canBeDeleted/'
BAD_FILE_PATH = '../badAuctions/'
SLEEP_TIME = 10
MAX_PROCESSED_FILE_SIZE_IN_BYTES = 10 * 1024 * 1024 * 1024 # 10 gb

# Check if there are files to work on
# In order to avoid editing a possibly locked file,
# we only work on files that are not conflicted.
# If no files available, sleep untile there are.
ls = listdir(RELATIVE_DATA_PATH)
while len(ls) <= 1:
	sleep(SLEEP_TIME*2)
	ls = listdir(RELATIVE_DATA_PATH)

# Initialize database connection
try:
	con = mdb.connect(AUCTIONS_DB_LOCATION,DB_USER,DB_PASSWORD,DB_SCHEMA)
except mdb.Error, e:
	print 'Error', str(e.args[0]), ':', e.args[1]
	print 'Could not open database connection, so exiting'
	exit(1)	

try:
	cur = con.cursor()
	cur.execute(GET_ACTIVE_AUCTIONS_QUERY)	
	results = cur.fetchall()
	print 'Got',len(results),'auctions that aren\'t disappeared'
except mdb.Error, e:
	print 'Error', str(e.args[0]), ':', e.args[1]
	print 'Could not get list of active auctions, so exiting'
	con.close()
	exit(1)
	
# Move active auctions to lastAuctions (something that can be extended)
lastAuctions = {}
for row in results:
	lastAuctions[row[0]] = 1

# Review each auction file
# If auctions in the active auction list aren't in a file, 
for filename in ls[:-1]:
	
	auctionsToBeMarkedDisappeared = []
	
	print 'At',filename
	
	# get the time of the read
	timeStr = filename[:-4]
	yearMonthDay,hourMinuteSeconds = timeStr.split('_')
	year,month,day = yearMonthDay.split('-')
	hour,minute,secondsMilliseconds = hourMinuteSeconds.split(':')
	seconds,milliseconds = secondsMilliseconds.split('.')
	
	auctionTime = datetime(int(year),int(month),int(day),int(hour),int(minute),int(seconds),int(milliseconds))
	auctionSecs = mktime(auctionTime.timetuple())
	
	# Load all of the auctions
	errorBoolean = False
	try:
		data = json.loads(open(RELATIVE_DATA_PATH+filename,'r').read())
	except ValueError:
		bs = BeautifulSoup(open(RELATIVE_DATA_PATH+filename,'r').read())
		try:
			testStr = bs.title.contents[0]
		except AttributeError:
			testStr = ''
			
		if testStr == '403 Forbidden':
			print '  File has code 403 Forbidden'
			data = {"pages":-1}
		else:
			print 'Error parsing auction file:',filename
			rename(RELATIVE_DATA_PATH+filename,BAD_FILE_PATH+filename)
			errorBoolean = True
		
		
	if "pages" in data and data["pages"] == 1:
		# The check for the pages is key is because sometimes auction data is unavailable
		# Glitch's API will return valid json, but with error values
		# I haven't yet worked out how to handle multipage data
		# As such, I recommend just dropping the pages and going on.
		# A sacrifice, but for the moment acceptable.
		curAuctions = data["items"]
	else:
		curAuctions = lastAuctions.copy()
	
	# Find all auctions that are no longer listed
	for key in lastAuctions.keys():
		if key not in curAuctions:
			# ADD AUCTION TO LIST OF THINGS TO GET A DISAPPEARED TIME
			auctionsToBeMarkedDisappeared.append("'"+key+"'") # quoted format because we use it in a query later
			del lastAuctions[key]
			
	# Find all auctions that are freshly listed.
	newAuctions = []
	for key in curAuctions:
		if key not in lastAuctions:
			# Add key to list
			# Parse auction data
			# Write auction into database
			lastAuctions[key] = 1
			
			# add record to new auctions
			try:    tool_state    = curAuctuions[key]["tool_state"]
			except: tool_state    = 0
			if tool_state == 'new':
				tool_state = 1
			elif tool_state == 'used':
				tool_state = 2
			try:    tool_uses     = int(curAuctions[key]["tool_uses"])
			except: tool_uses     = 0
			try:    tool_capacity = int(curAuctions[key]["tool_capacity"])
			except: tool_capacity = 0
			
			try:
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
					"'"+curAuctions[key]["url"].replace('\\','')                   +"'"])+')')
			except KeyError, e:
				print 'Error', str(e)
				print 'Found at',key
				con.close()
				exit(1)
	
	
	# Update all auctions in the database that have disappeared to be marked as disappeared
	if len(auctionsToBeMarkedDisappeared) > 0:
		print '  Disappearing',len(auctionsToBeMarkedDisappeared),'old auctions...'
		update_query = "update auctions set disappeared='"+getSQLdateFromEpoch(auctionSecs)+"' where id in ("+', '.join(auctionsToBeMarkedDisappeared)+")"
		try:
			cur.execute(update_query)
			con.commit()
		except mdb.Error, e:
			print 'Error', str(e.args[0]), ':', e.args[1]
			print 'Could not mark auctions as disappeared, so exiting.'
			con.close()
			exit(1)
	else:
		print '  No auctions need disappeared...'
	
	# Insert all new auctions into database
	if len(newAuctions) > 0:
		print '  Adding',len(newAuctions),'new auctions...'
		executeString = INSERT_QUERY_PREFIX+', '.join(newAuctions)+INSERT_QUERY_SUFFIX
		try:
			cur.execute(executeString)
			con.commit()
		except mdb.Error, e:
			print 'Error', str(e.args[0]), ':', e.args[1]
			print 'Could not insert processed auctions, so exiting.'
			con.close()
			exit(1)
	else:
		print '  No auctions to be added...'
		
	# Clean up and go on to the next file
	del newAuctions
	if errorBoolean == False:
		rename(RELATIVE_DATA_PATH+filename,RELATIVE_PROCESSED_PATH+filename)
	
con.close()
print 'All auctions added to database. Curling...'
curlAuctionPages.main()


# Check how many files have been processed
print 'Done adding auctions. Cleaning up processed files...'
ls = listdir(RELATIVE_PROCESSED_PATH)
processed_file_size = sum([getsize(RELATIVE_PROCESSED_PATH+entry) for entry in ls])
if processed_file_size >= MAX_PROCESSED_FILE_SIZE_IN_BYTES:
	print ' ',processed_file_size,'>=',MAX_PROCESSED_FILE_SIZE_IN_BYTES
	print '  Compacting...'
	archiveName = ls[0].split('_')[0].replace('-','')[2:]+'-'+ls[-1].split('_')[0].replace('-','')[2:]+'.7z'
	call(['7za','a','-t7z',archiveName,RELATIVE_PROCESSED_PATH+'*'])
	if not exists(RELATIVE_DELETION_PATH):
		mkdir(RELATIVE_DELETION_PATH)
	for entry in ls:
		print '  Moving processed files...'
		rename(RELATIVE_PROCESSED_PATH+entry,RELATIVE_DELETION_PATH+entry)
else:
	print ' ',processed_file_size,'<',MAX_PROCESSED_FILE_SIZE_IN_BYTES
	print '  No need to compact yet.'

deletableFileCount = 0
if exists(RELATIVE_DELETION_PATH):
	deletableFileCount = len(listdir(RELATIVE_DELETION_PATH))
print deletableFileCount,'files need to be deleted.'
	
	