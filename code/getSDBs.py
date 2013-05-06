#import urllib2
from os import remove
from subprocess import call
from datetime import datetime
from time import sleep
import pycurl

def getSDBPage(outfilename,urlSuffix=''):
	curl = pycurl.Curl()
	curl.setopt(pycurl.FAILONERROR,1)
	curl.setopt(pycurl.TIMEOUT,60)
	curl.setopt(pycurl.URL,'http://api.glitch.com/simple/economy.recentSDBs'+urlSuffix)
	outfile = open(outfilename,'w')
	curl.setopt(pycurl.WRITEDATA,outfile)
	curl.perform()
	outfile.close()
	curl.close()
	print '  curl done'

TEST_FILE          = 'sdbTest.txt'
RELATIVE_DATA_PATH = '../sdbs/'
SDB_INTERVAL   = 5*60*60 # Time between scans in seconds
SDBS_TO_GET    = -1 #24*60*60/AUCTION_INTERVAL # Number of auctions to read

i = 0
while SDBS_TO_GET == -1 or i < SDBS_TO_GET:

	auctionCount = '10000'

	try:
		#req = urllib2.Request(BASE_AUCTION_URL)
		#response = urllib2.urlopen(req)
		#the_page = response.read()
		#call(['curl','--output',RELATIVE_DATA_PATH+TEST_FILE,BASE_SDB_URL])
		
		getSDBPage(RELATIVE_DATA_PATH+TEST_FILE)
		print 'Got initial read'
		
		the_page = open(RELATIVE_DATA_PATH+TEST_FILE,'r').read()
		startIndex = the_page.find('"total"')
		endIndex = the_page[startIndex:].find(',')
		auctionCount = the_page[startIndex:(startIndex+endIndex)].split(':')[1]
		remove(RELATIVE_DATA_PATH+TEST_FILE)
		print 'Got',auctionCount,'auctions, repaginating and redownloading'
	
	except:
		auctionCount = '10000' # random high defualt value - might be too small, hopefully not.
		print 'Going with default of',auctionCount,'auctions, repaginating and redownloading'

		
	try:
		outfilename = str(datetime.now()).replace(' ','_')+'.txt'
		print outfilename
		#req = urllib2.Request(BASE_AUCTION_URL+'?per_page='+auctionCount)
		#response = urllib2.urlopen(req)
		#the_page = response.read()
		
		#outfile = open(RELATIVE_DATA_PATH+outfilename,'w')
		#outfile.write(the_page)
		#outfile.close()
		getSDBPage(RELATIVE_DATA_PATH+outfilename,'?per_page='+auctionCount)
		#call(['curl','--output',RELATIVE_DATA_PATH+outfilename,BASE_AUCTION_URL+'?per_page='+auctionCount])
		
	except:
		print 'Couldn\'t get auctions, skipping.'

	if SDBS_TO_GET != -1:
		i += 1
	if SDBS_TO_GET == -1 or i < SDBS_TO_GET:
		#print AUCTIONS_TO_GET-i,'auctions to go'
		#print 'Sleeping for',AUCTION_INTERVAL,'seconds'
		sleep(SDB_INTERVAL)
