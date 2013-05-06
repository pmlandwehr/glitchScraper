import json
import MySQLdb as mdb
from os import listdir, mkdir, rename
from os.path import getsize, exists
from time import mktime,gmtime
from datetime import datetime
from BeautifulSoup import BeautifulSoup
import traceback
RELATIVE_DATA_PATH = '../allSales/'
RELATIVE_PROCESSED_PATH = '../allSalesProcessed/'
RELATIVE_DELETION_PATH  = '../canBeDeleted/'
BAD_FILE_PATH = '../badSDBs/'

AUCTIONS_DB_LOCATION = '<Put database path here>'
DB_USER = '<Put username here>'
DB_PASSWORD = '<Put password here>'
DB_SCHEMA = '<Put database schema here>'
INSERT_QUERY_PREFIX = 'insert ignore into sdbSales (id,sold,class_tsid,count,cost) values '
INSERT_QUERY_SUFFIX = '' #' on duplicate key update disappeared=null'
MAX_PROCESSED_FILE_SIZE_IN_BYTES = 2 * 1024 * 1024 * 1024 # 2 gb

def getSQLdateFromEpoch(epochTime):
	return datetime.fromtimestamp(epochTime).strftime("%Y-%m-%d %H:%M:%S")

ls = listdir(RELATIVE_DATA_PATH)

# Initialize database connection
try:
	con = mdb.connect(AUCTIONS_DB_LOCATION,DB_USER,DB_PASSWORD,DB_SCHEMA)
	cur = con.cursor()
except mdb.Error, e:
	print 'Error', str(e.args[0]), ':', e.args[1]
	print 'Could not open database connection, so exiting'
	exit(1)


# We assume working order.
for index,filename in enumerate(ls):

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
			print 'Error parsing sdb file:',filename
			rename(RELATIVE_DATA_PATH+filename,BAD_FILE_PATH+filename)
			errorBoolean = True

	if "pages" in data and data["pages"] == 1:
		# The check for the pages is key is because sometimes auction data is unavailable
		# Glitch's API will return valid json, but with error values
		# I haven't yet worked out how to handle multipage data
		# As such, I recommend just dropping the pages and going on.
		# A sacrifice, but for the moment acceptable.
		curSales = data["items"]
	else:
		curSales = []
		
	print 'Got',len(curSales),'current sales'
	
	#timeStr = filename[:-4]
	#yearMonthDay,hourMinuteSeconds = timeStr.split('_')
	#year,month,day = yearMonthDay.split('-')
	#hour,minute,secondsMilliseconds = hourMinuteSeconds.split(':')
	#seconds,milliseconds = secondsMilliseconds.split('.')

	#auctionTime = datetime(int(year),int(month),int(day),int(hour),int(minute),int(seconds),int(milliseconds))
	#auctionSecs = mktime(auctionTime.timetuple())
	
	sdbSales = []
	
	# Parse through all sales to find SDB sales.
	# Add appropriate SDB sales to the SQL Statement
	for key in curSales:
		if curSales[key]["source"] == "sdb":
		
			try:
				entryStr  = '('
				entryStr += "'"+key                                             +"', "
				entryStr += "'"+getSQLdateFromEpoch(curSales[key]["date_sold"]) +"', "
				entryStr += "'"+str(curSales[key]["item_class_tsid"])           +"', "
				entryStr += "'"+str(curSales[key]["qty"])                       +"', "
				entryStr += "'"+str(curSales[key]["total_price"])               +"')"
				sdbSales.append(entryStr)
				
				#sdbSales.append('('+
				#	', '.join([
				#		"'"+key                                             +"'",
				#		"'"+getSQLdateFromEpoch(curSales[key]["date_sold"]) +"'",
				#		"'"+curSales[key]["item_class_tsid"]                +"'",
				#		"'"+curSales[key]["qty"]                            +"'",
				#		"'"+curSales[key]["total_price"]                    +"'"])+')')
			except KeyError, e:
				print 'Error', str(e)
				print 'Found at',key
				traceback.print_exc()
				#con.close()
				exit(1)
	
	print 'Got',len(sdbSales),'SDB Sales'
	
	# Add SDB sales to MySQL
	if len(sdbSales) > 0:
		print '  Adding',len(sdbSales),'SDB sales into the database...'
		executeString = INSERT_QUERY_PREFIX+', '.join(sdbSales)+INSERT_QUERY_SUFFIX
		try:
			cur.execute(executeString)
			con.commit()
		except mdb.Error, e:
			print 'Error', str(e.args[0]), ':', e.args[1]
			print 'Could not insert processed auctions, so exiting.'
			con.close()
			exit(1)
			
	else:
		print 'No sales to add from this file.'
			
	# Clean up and go on to the next file
	del sdbSales
	if errorBoolean == False:
		rename(RELATIVE_DATA_PATH+filename,RELATIVE_PROCESSED_PATH+filename)

con.close()


# Check how many files have been processed
print 'Done adding SDB Sales. Cleaning up processed files...'
ls = listdir(RELATIVE_PROCESSED_PATH)
processed_file_size = sum([getsize(RELATIVE_PROCESSED_PATH+entry) for entry in ls])
if processed_file_size >= MAX_PROCESSED_FILE_SIZE_IN_BYTES:
	print ' ',processed_file_size,'>=',MAX_PROCESSED_FILE_SIZE_IN_BYTES
	print '  Compacting...'
	archiveName = 'sdbs_'+ls[0].split('_')[0].replace('-','')[2:]+'-'+ls[-1].split('_')[0].replace('-','')[2:]+'.7z'
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
	

	