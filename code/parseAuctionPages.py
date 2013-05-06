# Queries the DB for all entries that are disappeared but still marked as active and curls all of the auction pages 

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
import MySQLdb as mdb
from os import listdir,remove
from subprocess import Popen
from sys import exit
from time import sleep
from types import NoneType

AUCTIONS_DB_LOCATION = '<Put database path here>'
DB_USER = '<Put username here>'
DB_PASSWORD = '<Put password here>'
DB_SCHEMA = '<Put database schema here>'
GET_DISAPPEARED_BUT_ACTIVE_AUCTIONS_QUERY = 'select id,url from auctions where state_id=1 and isnull(disappeared)=false'
PATH_TO_AUCTION_PAGES = '../auctionPages/'
SLEEP_TIME = 5
MAX_QUERY_SIZE = 500
MAX_PAGES_AT_A_TIME = 7500
ERRORFILE = 'badAuctionHTML.txt'

def updateAuctionStates(con,cur,newState,queryData):
	if len(queryData) == 0:
		return
	try:
		executeString = "update auctions set state_id='"+newState+"' where id in ("+','.join(queryData)+")"
		cur.execute(executeString)
		con.commit()
	except mdb.Error, e:
		print 'Error', str(e.args[0]), ':', e.args[1]
		print 'Could not update state to '+newState
		con.close()
		exit(1)

	
def parsePages(ls):		
	soldAuctions     = []
	canceledAuctions = []
	expiredAuctions  = []
	deadRecords      = []
	modVal = len(ls)/10
	if modVal < 1:
		modVal = 1
	print 'Got',len(ls),'auction pages. Parsing...'
	for index,filename in enumerate(ls):
		id = 'NULL_ID'
		if filename[-5:] == '.html':
			id = filename[:-5]
		elif filename[-4:] == '.txt':
			id = filename[:-4]
		
		bs = BeautifulSoup(open(PATH_TO_AUCTION_PAGES+filename,'r').read())
		try:
			stateStr = bs.h5.contents[0]
		except AttributeError:
			stateStr = ''
			if type(bs.h1) is NoneType or bs.h1.contents[0] != 'Forbidden':
				outfile = open(ERRORFILE,'a')
				outfile.write('Could not get auction status for '+filename+'\n')
				outfile.close()
			else:
				deadRecords.append("'"+id+"'")
		if stateStr == 'You snooze, you lose...':
			# Auction ended in a sale
			soldAuctions.append("'"+id+"'")
		elif stateStr == 'Oh boo!':
			# Auction ended in a cancellation
			canceledAuctions.append("'"+id+"'")
		elif stateStr == 'The fat lady sang her ditty...':
			# Auction expired
			expiredAuctions.append("'"+id+"'")
			
		if index % modVal == 0:
			print '  Finished',index+1,'auctions'

	print 'Got',len(soldAuctions),'sold auctions'
	print 'Got',len(canceledAuctions),'canceled auctions'
	print 'Got',len(expiredAuctions),'expired auctions'
	print 'Got',len(deadRecords),'dead records'
			
	# Open database connection
	try:
		con = mdb.connect(AUCTIONS_DB_LOCATION,DB_USER,DB_PASSWORD,DB_SCHEMA)
	except mdb.Error, e:
		print 'Error', str(e.args[0]), ':', e.args[1]
		print 'Could not open database connection, so exiting'
		exit(1)

	# Initialize cursor
	try:
		cur = con.cursor()
	except mdb.Error, e:
		print 'Error', str(e.args[0]), ':', e.args[1]
		print 'Could not initialize cursor, so exiting'
		con.close()
		exit(1)
		
	# Update sold auctions
	print 'Updating sold auctions...'
	while len(soldAuctions) >= MAX_QUERY_SIZE:
		updateAuctionStates(con,cur,'2',soldAuctions[:MAX_QUERY_SIZE])
		soldAuctions = soldAuctions[MAX_QUERY_SIZE:]
	updateAuctionStates(con,cur,'2',soldAuctions)

	# Update canceled auctions
	print 'Updating canceled auctions...'
	while len(canceledAuctions) >= MAX_QUERY_SIZE:
		updateAuctionStates(con,cur,'3',canceledAuctions[:MAX_QUERY_SIZE])
		canceledAuctions = canceledAuctions[MAX_QUERY_SIZE:]
	updateAuctionStates(con,cur,'3',canceledAuctions)

	# Update expired auctions
	print 'Update expired auctions...'
	while len(expiredAuctions) >= MAX_QUERY_SIZE:
		updateAuctionStates(con,cur,'4',expiredAuctions[:MAX_QUERY_SIZE])
		expiredAuctions = expiredAuctions[MAX_QUERY_SIZE:]
	updateAuctionStates(con,cur,'4',expiredAuctions)

	print 'Updating dead records...'
	while len(deadRecords) >= MAX_QUERY_SIZE:
		updateAuctionStates(con,cur,'5',deadRecords[:MAX_QUERY_SIZE])
		deadRecords = deadRecords[MAX_QUERY_SIZE:]
	updateAuctionStates(con,cur,'5',deadRecords)	
	
	# Close connection
	con.close()


def main():
	auctionPages = listdir(PATH_TO_AUCTION_PAGES)
	if len(auctionPages) > MAX_PAGES_AT_A_TIME:
		print 'Parsing',len(auctionPages),'auctions as',len(auctionPages)/MAX_PAGES_AT_A_TIME+1,'chunks'
	while len(auctionPages) > MAX_PAGES_AT_A_TIME: 
		parsePages(auctionPages[:MAX_PAGES_AT_A_TIME])
		for filename in auctionPages[:MAX_PAGES_AT_A_TIME]:
			remove(PATH_TO_AUCTION_PAGES+filename)
		auctionPages = auctionPages[MAX_PAGES_AT_A_TIME:]
		print '==== Finished chunk ===='
	parsePages(auctionPages)
	for filename in auctionPages:
		remove(PATH_TO_AUCTION_PAGES+filename)
	
if __name__ == "__main__":
    main()