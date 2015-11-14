#mk_sample.py
#mike byrne
#nov 6, 2015
#post process open address data
#the primary goal of this script is to get the US state csv files into a more usable
#condition by ensuring the maximum number of fields are filled out
#these fields are primarily zip codes and city names, but also cleaning out where fields
#are null.  to backfill these fields, this script uses census data

#dependencies - software/libraries
#	- postgis
#	- the psycopg library for talking w/ postgis

#dependencies - data
#	- the state download csv files from OpenAddress 
#		found under the "U.S. addresses (list of states by region)"here;
#		http://results.openaddresses.io/ 
#	NOTE: this script requires a directory structure of all state sub folders to
#		be in one folder.  e.g. us=> al, us=>ak ... 
#
#	- the nationwide zip code tabulation areas shapefile loaded as a table in 
#		postgis from US Census found here:
#		ftp://ftp2.census.gov/geo/tiger/TIGER2015/ZCTA5/
#
#	- the individual state place shapefiles loaded as statewide tables in
#		postgis from the US Census found here:
#		ftp://ftp2.census.gov/geo/tiger/TIGER2015/PLACE/

import os
import psycopg2
import time
#import multiprocessing as mp
now = time.localtime(time.time())
print "start time:", time.asctime(now)

#variables
myHost = "localhost"
myPort = "5432"
myUser = "feomike"
db = "feomike"
sch = "openaddress"
pre = "add" #"source"
myPath = "/Users/feomike/documents/analysis/2015/openaddress/source_data/us/"
outPath = "/Users/feomike/documents/analysis/2015/openaddress/out_data/"
zipTB = "tl_2015_us_zcta510"
prj = "4269"

#globals
#there is one global - theCur - which is single cursor connection from the script 
#	to postgis

#this function drives the process.  it receives a state code, then performs the 
#following functions; 1) make a table to dump the data into; 2) determine which source
#files to load (statewide or local); 3) create geometry for the points; 4) delete null
#number and street rows; 5) update the state field; 6) update the zip code based on the
#census ZCTA containing that point if that row is null; 7) update the city name based 
#on the census place name containing that point if the city is null and 8) potentially
#write out the csv file to ingest in grasshopper
def drive_process(myST):
	print "...doing " + myST
	mk_tb(myST)
	#determine files to load
	myFiles = get_files(myST)
	for myFile in myFiles:
		#if there is an error in the file to be loaded, continue on to the next file
		try:
			pop_data(myST, myFile)
		except:
			print "     " + myFile + " failed; continuing on ..."
			continue
	mk_geom(myST)
	clean_null(myST)
	upd_st(myST)	
	upd_zip(myST)
	upd_city(myST)
	copy_out(myST)

#this function receives a 2 digit state code, and returns the list of .csv files to 
#load into;  this function sees if there is a statewide file and if so, only returns 
#that .csv, even if there are other .csv's.  the idea is that this would reduce overlap
#but this could easily be modified to change that to include all .csv's.  if there is no
#statewide.csv, then it makes a list of all of those files
def get_files(myST):
	#create a statewide table if there is a statewide.csv
	myList = []
	if os.path.isfile(myPath + myST + "/statewide.csv"):
		#print "...doing " + myST + "     has a statewide.csv  "
		myList.append("statewide.csv")
	#if there isn't a statewide.csv, create a state table and populate it from
	#all of the .csv files in that state directory
	else:
		files = os.listdir(myPath + myST + "/")
		for file in files:
			if file[-4:] == ".csv":
				myList.append(file)
	return(myList)

#this function creates a single blank table for an individual state into which all
#input state files would eventually be dumped and processed on
#the schema for this table is;
#	lon, lat, number, street, city, district, region, postcode and id
def mk_tb(myST):
	mySQL = "DROP TABLE IF EXISTS " + sch + "." + pre + "_" + myST + "; COMMIT; "
	mySQL = mySQL + "CREATE TABLE " + sch + "." + pre + "_" + myST + " ( "
	mySQL = mySQL + "lon real, lat real, st_number character varying (35), "
	mySQL = mySQL + "street character varying(100), city character varying(50), "
	mySQL = mySQL + "district character varying(200), region character varying(200), "
	mySQL = mySQL + "postcode character varying(200), id character varying(200) ); " 
	theCur.execute(mySQL)
	return

#this function receives a state code and a csv file to load, and copies that into the 
#resulting state table
def pop_data(myST,myFile):
	mySQL = "COPY " + sch + "." + pre + "_" + myST + " FROM " 
	mySQL = mySQL + "'" + myPath + myST + "/" + myFile + "'"
	mySQL = mySQL + "CSV HEADER DELIMITER ','; "
	theCur.execute(mySQL)
	return

#this function receives a state 2 digit code and performs a couple of functions on the
#state table already generated;  1) it adds a geometry column to be used later for point
#in polygon matching to ZIP and Place names, 2) it generates an index on the geometry 
#column and 3) it vacuums the table b/c every row got updated
def mk_geom(myST):
	mySQL = "ALTER TABLE " + sch + "." + pre + "_" + myST 
	mySQL = mySQL + " add column state character varying(2); "
	mySQL = mySQL + "ALTER TABLE " + sch + "." + pre + "_" + myST 
	mySQL = mySQL + " add column gid serial not null; "
	mySQL = mySQL + "SELECT AddGeometryColumn ('" + sch + "','" + pre + "_" + myST 
	mySQL = mySQL + "','geom'," + prj + ",'POINT',2); "
	mySQL = mySQL + "UPDATE " + sch + "." + pre + "_" + myST
	mySQL = mySQL + " set geom = ST_SetSRID(ST_MakePoint(lon, lat), " + prj + "); "
	mySQL = mySQL + "CREATE INDEX " + sch + "_" + pre + "_" + myST + "_geom_ndx ON "
	mySQL = mySQL + sch + "." + pre + "_" + myST + " USING gist (geom); "
	theCur.execute(mySQL)
	mySQL = "VACUUM " + sch + "." + pre + "_" + myST + "; "
	theCur.execute(mySQL)
	return

#this function receives a state two digit code so it can delete any rows on the state 
#table. rows to be deleted are rows where (1) the st_number is null; (2) the street name 
#is null.  it then vacuums the table because of the number of potential deletes
def clean_null(myST):
	mySQL = "DELETE FROM " + sch + "." + pre + "_" + myST + " WHERE st_number is null; "
	mySQL = mySQL + "DELETE FROM " + sch + "." + pre + "_" + myST + " WHERE street is null; "
	theCur.execute(mySQL)
	mySQL = "VACUUM " + sch + "." + pre + "_" + myST + "; "
	theCur.execute(mySQL)	
	return

#this function receives a state two digit code so it can update the state table with a 
#consistent state two digit code for the entire table.  it then vacuums the table b/c
#every row has been operated on
def upd_st(myST):
	mySQL = "UPDATE " + sch + "." + pre + "_" + myST
	mySQL = mySQL + " set state = '" + myST.upper() + "'; "
	theCur.execute(mySQL)
	mySQL = "VACUUM " + sch + "." + pre + "_" + myST + "; "
	theCur.execute(mySQL)	
	return	

#this function receives a state two digit code so it can update the state table with zip
#codes that are null.  zipcodes are updated from the nationwide census zip code
#tabulation area table, which needs to exist in the same schema.  only those rows which 
#are null are updated
def upd_zip(myST):
	mySQL = "UPDATE " + sch + "." + pre + "_" + myST + " SET postcode = zcta5ce10 " 
	mySQL = mySQL + "FROM " + sch + "." + zipTB + " WHERE postcode is NULL AND "
	mySQL = mySQL + "ST_Intersects(" + zipTB + ".geom, " + pre + "_" + myST + ".geom); "
	theCur.execute(mySQL)
	mySQL = "VACUUM " + sch + "." + pre + "_" + myST + "; "
	theCur.execute(mySQL)	
	return

#this function receives a two digit state code so it can update the state table with 
#city names.  it uses place names from the us census place shape file which much be 
#already loaded into the same schema.  the updates only occur on rows for which the 
#city field is null	
def upd_city(myST):
	#this function translates the two digit state code to a fips number; this is 
	#required b/c census publishes the state place shape file with the name of its
	#associated fips code
	myFIPS = ret_FIPS(myST)
	mySQL = "UPDATE " + sch + "." + pre + "_" + myST + " SET city = name "
	mySQL = mySQL + "FROM " + sch + ".tl_2015_" + myFIPS + "_place WHERE city is NULL "
	mySQL = mySQL + "AND ST_Intersects(tl_2015_" + myFIPS + "_place.geom, " + pre + "_" + myST
	mySQL = mySQL + ".geom) AND tl_2015_" + myFIPS + "_place.name not like '%balance%'; "
	theCur.execute(mySQL)
	mySQL = "VACUUM " + sch + "." + pre + "_" + myST + "; "
	theCur.execute(mySQL)	
	return

#this function copies the state table out to a csv after all of the updates have happened
def copy_out(myST):
	mySQL = "COPY ( SELECT lon, lat, st_number as number, street, city, state, postcode " 
	mySQL = mySQL + " FROM " + sch + "." + pre + "_" + myST + " ) "
	mySQL = mySQL + " TO '" + outPath + "openaddress_" + myST + ".csv'"
	mySQL = mySQL + " CSV HEADER DELIMITER ',' ; "
	theCur.execute(mySQL)
	return

#return the state abbreviation given the state fips code
def ret_FIPS(myST):
	myST = myST.upper()
	states = {"AL":"01","AK":"02","AZ":"04","AR":"05","CA":"06","CO":"08","CT":"09",
	   "DE":"10","DC":"11","FL":"12","GA":"13","HI":"15","IA":"19","ID":"16","IL":"17",		
	   "IN":"18","KS":"20","KY":"21","LA":"22","MA":"25","MD":"24","ME":"23","MI":"26",
	   "MN":"27","MO":"29","MS":"28","MT":"30","NC":"37","ND":"38","NE":"31","NH":"33",
	   "NJ":"34","NM":"35","NV":"32","NY":"36","OH":"39","OK":"40","OR":"41","PA":"42",
	   "RI":"44","SC":"45","SD":"46","TN":"47","TX":"48","UT":"49","VA":"51","GA":"13",
	   "VT":"50","WA":"53","WI":"55","WV":"54","WY":"56"
	   }
	try:
		state = states[myST]
	except:
		theMsg = "You likely did not enter a valid two letter state abbreviation, "
		theMsg = theMsg + "please run again."
		print theMsg
		state = "0"
	return state

#make database connection - so that we can use cursors to update the table
myConn = "dbname=" + db + " host=" + myHost + " port=" + myPort + " user=" + myUser
conn = psycopg2.connect(myConn)
theCur = conn.cursor()

#loop through each state; to post process the data
States = ["ak","al","ar","az","ca","co","ct"]          #1
States = States + ["dc","de","fl","ga","hi","ia","id"] #2
States = States + ["il","in","ks","ky","la","ma","md","me"] #3 
States = States + ["mi","mn","mo","ms","mt","nc","nd"] #4 
States = States + ["ne","nh","nj","nm","nv","ny","oh","ok"] #5
States = States + ["or","pa","ri","sc","sd","tn","tx"] #6
States = States + ["ut","va","vt","wa","wi","wv","wy"] #7
States = ["nd"]
for theST in States:
	try:
		drive_process(theST)
	except:
		print "     " + theST + "   failed, continuing on ..."
		continue
		
#pool = mp.Pool(processes=4)
#results = pool.map(drive_process, States)

now = time.localtime(time.time())
print "end time:", time.asctime(now)		
