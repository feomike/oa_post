# oa_post
post processing of open address data

this repo is an example code to post process Open Address US statewide address csv files to a more usable state.  the primary operations on these files are;

* remove any null street number and street name rows
* populate null ZIP code rows with ZIP codes from US census zipcode tablulation areas
* populate null city rows with place names from US census place name areas
* populate standard state codes
* pass over any ill-formatted csv files (very small number found so far)
* push out a new csv file


Dependencies (software)
-----------------------
* PostGIS
* Psycopg2

Dependenceis (data)
-------------------
* the state download csv files from OpenAddress found under the "U.S. addresses (list of states by region)" [here](http://results.openaddresses.io/)

NOTE: this script requires a directory structure of all state sub folders to be in one folder.  e.g. us=> al, us=>ak ... 

* the nationwide zip code tabulation areas shapefile loaded as a table in postgis from US Census found [here] (ftp://ftp2.census.gov/geo/tiger/TIGER2015/ZCTA5/)
* the individual state place shapefiles loaded as statewide tables in postgis from the US Census found [here](ftp://ftp2.census.gov/geo/tiger/TIGER2015/PLACE/)

Example output
--------------
* [Post Processed DC file](https://dl.dropboxusercontent.com/u/40278130/oa_post/openaddress_dc.csv.zip)
* [Post Processed North Dakota file](https://dl.dropboxusercontent.com/u/40278130/oa_post/openaddress_nd.csv.zip)

Issues
------
* the code is single threaded and quite slow, given that it touches every row in large-ish tables several inddependent times


Future Enhancements
-------------------
* it would be worthwhile to test the notion w/o postgis as a dependency and move to using the libraries shapely and fiona.  the hypothesis is that the most expensive part of this code currently is the time to update every row with a point geometry for each address, that shapely would handle this faster.
* it would be fun to test this as a multi-threaded functional programming exercise to increase processing speed
* it would be a good idea to set this up as a chron job, and incorporate some hashing or something to see if change had happened and therefore not reprosses the entire dataset






