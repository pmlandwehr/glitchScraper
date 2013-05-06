#The glitchScraper Readme #

## Intro ##

From November 2011 through its closure in December of 2012, I collected a large amount of economic data from Glitch, a casual, flash-based, freemium online game. I've made the data from the scrape available at http://www.doeverythingforever.com/projects/the-glitch-dataset, along with a tech report that describes both the game and the data in additional detail. This repository includes the source code for the scraper, and this readme is intended to provide some annotations for the code.

## Files ##

### Constantly Running Scrapers ###

#### getAuctions.py ####

The basic auction scraper. Ran continuously, pulling down auction files and storing them in the "unedited" folder.

#### getAllSales.py ####

When Glitch was redesigned to support SDB sales as well as auctions, the developers added a new interface that pulled down all sales, both from auctions and from SDBs. This script was used to capture all sales data, which was then parsed for SDB information; getAuctions.py remained in use because it covered all open auctions, not just those that ended in sales.

#### getSDBs.py ####

Not actually used, because getAllSales.py provided a richer version of the same data. This script would collect all recent SDB sales.


### Parsers of scraped data ###

#### auctionsToMySQL.py ####

The basic auction parser. Checks the MySQL database for auctions that are still active, then goes through the collection of scraped auctions finding both new auctions and timestamps for when auctions have disappeared. Adds all of the auctions to the database and updates the status for those that have vanished, then calls curlAuctionPages.

#### curlAuctionPages.py ####

Searches the database for auctions that have been marked as "disappeared" - no longer showing up in the list of active auctions - and systematically scrapes all of the specific webpages for those auctions. Once all the scraping is complete, calls parseAuctionPages.

#### parseAuctionPages.py ####

Looks at the collection of scraped auction pages and determines why the auctions ended: either they expired, were canceled by the seller, or purchased. The script updates the database listings of the individual records and then deletes the scraped pages.

#### sdbSalesToMySQL.py ####

The basic SDB sales parser. Goes through lists of all sales collected by getAllSales.py, pulls out SDB sales, and adds them to the database.
