from BeautifulSoup import BeautifulSoup
import MySQLdb as mdb
from os import remove
from subprocess import call
from sys import argv,exit

if len(argv) != 3:
	Print 'Bad Arguments: python updateAuctionRecord.py <auction id> <timestamp>'
	exit(1)

AUCTION_ID   = argv[1]
DATE_NOTICED = argv[2]
ERRORFILE    = ''

try:
	con = mdb.connect(AUCTIONS_DB_LOCATION,DB_USER,DB_PASSWORD,DB_SCHEMA)
	cur = con.cursor()
	cur.execute('select url from auctions where id='+AUCTION_ID)
		
	results = cur.fetchall()

except mdb.Error, e
	print 'Error', str(e.args[0]), ':', e.args[1]
	exit(1)
	
call(['curl','--output',AUCTION_ID+'.html','http://www.glitch.com'+auctionUrl])

bs = BeautifulSoup(open(AUCTION_ID+'.html','r').read())

stateStr = bs.h5.contents[0]

if stateStr == 'You snooze, you lose...':
	stateStr = 'sold'
elif stateStr == 'Oh boo!':
	stateStr = 'canceled'
elif stateStr == 'The fat lady sang her ditty...':
	stateStr = 'expired'
else:
	outfile = open(ERRORFILE,'a')
	outfile.write('Could not get auction status for '+AUCTION_ID+'.html\n')
	outfile.close()
	con.close()
	exit(1)

try:
	cur.execute("update auctions set state='"+stateStr+"' and disappeared='"+DATE_NOTICED+"' where id = '"+AUCTION_ID+"'")
except mdb.Error, e
	print 'Error', str(e.args[0]), ':', e.args[1]
	print 'Error executing update to auction',AUCTION_ID
	exit(1)
	
con.close()
remove(AUCTION_ID+'.html')