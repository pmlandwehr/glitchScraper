import urllib2
from datetime import datetime
from time import sleep

BASE_AUCTION_URL   = 'http://api.glitch.com/simple/auctions.list'
RELATIVE_DATA_PATH = '../unedited/'
AUCTION_INTERVAL   = 10 # Time between scans in seconds
AUCTIONS_TO_GET    = -1 #24*60*60/AUCTION_INTERVAL # Number of auctions to read

i = 0
while AUCTIONS_TO_GET == -1 or i < AUCTIONS_TO_GET:

	try:
		req = urllib2.Request(BASE_AUCTION_URL)
		response = urllib2.urlopen(req)
		the_page = response.read()
		
		print 'Got initial read'
		
		startIndex = the_page.find('"total"')
		endIndex = the_page[startIndex:].find(',')
		auctionCount = the_page[startIndex:(startIndex+endIndex)].split(':')[1]
		
		print 'Got',auctionCount,'auctions, repaginating and redownloading'
	
	except:
		auctionCount = '10000' # random high defualt value - might be too small, hopefully not.
		print 'Going with default of',auctionCount,'auctions, repaginating and redownloading'
		
	try:
		req = urllib2.Request(BASE_AUCTION_URL+'?per_page='+auctionCount)
		response = urllib2.urlopen(req)
		the_page = response.read()
		
		outfilename = str(datetime.now()).replace(' ','_')+'.txt'
		
		outfile = open(RELATIVE_DATA_PATH+outfilename,'w')
		outfile.write(the_page)
		outfile.close()
		
	except:
		print 'Couldn\'t get auctions, skipping.'
		pass

	i += 1
	if AUCTIONS_TO_GET == -1 or i < AUCTIONS_TO_GET:
		#print AUCTIONS_TO_GET-i,'auctions to go'
		#print 'Sleeping for',AUCTION_INTERVAL,'seconds'
		sleep(AUCTION_INTERVAL)