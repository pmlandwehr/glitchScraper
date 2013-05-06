# This program queries the DB for all entries that are disappeared but still marked as active
# It tries to curl all of these pages
# If it fails because the resource is blocked (not unlikely)...
#  It parses the pages it has pulled
#  It waits until the list of processes is clean
#  It tries again.
# NOTE that if a page isn't successfully retrieved, the database won't be updated
# and the un-updated entry will be fixed on a future run.

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

import multiprocessing
import MySQLdb as mdb
#from subprocess import call,Popen,PIPE
import pycurl
from sys import exit
from time import sleep

import parseAuctionPages

AUCTIONS_DB_LOCATION = '<Put database path here>'
DB_USER = '<Put username here>'
DB_PASSWORD = '<Put password here>'
DB_SCHEMA = '<Put database schema here>'
GET_DISAPPEARED_BUT_ACTIVE_QUERY = "select id,url from auctions where state_id=1 and isnull(disappeared)=false"
PATH_TO_AUCTION_PAGES = '../auctionPages/'
SLEEP_TIME = 15
MAX_PROCESSES = 20


def worker(urlToGet,outfilename):
	curl = pycurl.Curl()
	curl.setopt(pycurl.FAILONERROR,1)
	curl.setopt(pycurl.TIMEOUT,60)
	curl.setopt(pycurl.URL,urlToGet)
	outfile = open(outfilename,'w')
	curl.setopt(pycurl.WRITEDATA,outfile)
	curl.perform()
	outfile.close()
	curl.close()
	
def main():
	# Initialize database connection
	try:
		con = mdb.connect(AUCTIONS_DB_LOCATION,DB_USER,DB_PASSWORD,DB_SCHEMA)
	except mdb.Error, e:
		print 'Error', str(e.args[0]), ':', e.args[1]
		print 'Could not open database connection, so exiting'
		exit(1)	

	# Get list of all auctions that have been disappeared but have active status
	try:
		cur = con.cursor()
		cur.execute(GET_DISAPPEARED_BUT_ACTIVE_QUERY)
		results = cur.fetchall()
	except mdb.Error, e:
		print 'Error', str(e.args[0]), ':', e.args[1]
		print 'Could not get list of possibly expired auctions, so exiting'
		con.close()
		exit(1)

	con.close()

	# Curl all of these auctions
	print 'Got',len(results),'auctions. Curling pages...'
	processList = []
	modVal = len(results)/10
	if modVal < 1:
		modVal = 1
		
	#curl = pycurl.Curl()
	#curl.setopt(pycurl.FAILONERROR,1)
	#curl.setopt(pycurl.TIMEOUT,60)
	
	for index,row in enumerate(results):
		outfilename = PATH_TO_AUCTION_PAGES+row[0]+'.html'
		url         =  'http://www.glitch.com/'+row[1]
		
		while len(processList) >= MAX_PROCESSES:
			for i,proc in enumerate(processList):
				if not proc.is_alive():
					processList.pop(i)
		#	for proc in processList[:]:
		#		status = proc.poll()
		#		if not status:
		#			processList.remove(proc)
			if len(processList) >= MAX_PROCESSES*3/4:
				#print ' ',len(processList),'processes remain. Sleeping for',SLEEP_TIME,'seconds'
				sleep(SLEEP_TIME)
		
		try:
			#outfile = open(outfilename,'w')
			#curl.setopt(pycurl.URL,url)
			#curl.setopt(pycurl.WRITEDATA,outfile)
			#curl.perform()
			#outfile.close()
			#call(['curl','--output',outfilename,url],stderr=PIPE)
			processList.append(multiprocessing.Process(target=worker,args=(url,outfilename)))
			processList[-1].start()
			#processList.append(Popen(['curl','--output',outfilename,url],stderr=PIPE))
		except OSError:
			pass
		
		if index % modVal == 0:
			print '  Begun',index+1,'curls...'
				
	# Wait for all auctions to finish
	print 'All curls begun. Waiting until they finish...'
	while len(processList) > 0:
		for i,proc in enumerate(processList):
			if not proc.is_alive():
				processList.pop(i)
	#	for proc in processList[:]:
	#		status = proc.poll()
	#		if not status:
	#			processList.remove(proc)
	#curl.close()
	print '  All curls done.'
	
	parseAuctionPages.main()
	
if __name__ == "__main__":
    main()
