require(sqlsurvey)		# load sqlsurvey package (analyzes large complex design surveys)
require(MonetDB.R)		# load the MonetDB.R package (connects r to a monet database)
require(foreign) 		# load foreign package (converts data files into R)
# require(downloader)		# downloads and then runs the source() function on scripts from github

#setwd( "V:/temp" )

#source_url( "https://raw.github.com/ajdamico/usgsd/master/MonetDB/read.SAScii.monetdb.R" )
source( "db.tests/read.SAScii.monetdb.R" )

dbname <- "acs"
dbport <- 50000

drv <- dbDriver("MonetDB")
monet.url <- paste0( "monetdb://localhost:" , dbport , "/" , dbname )
db <- dbConnect( drv , monet.url , "monetdb", "monetdb")

year <- 2010

tf <- tempfile() ; td <- tempdir()

# run this to fake the .jcall function.. just so read.SAScii.monetdb() works
.jcall <- function( x , y , z ) TRUE


# remove the temporary file (defined waaaay above) from the local disk, if it exists
#file.remove( tf )
#unlink(tf)

# the zipped filename and sas importation script fit this pattern:
fn <- paste0( "ftp://ftp.cdc.gov/pub/data/brfss/CDBRFS" , substr( year , 3 , 4 ) , "ASC.ZIP" )
sas_ri <- paste0( "http://www.cdc.gov/brfss/technical_infodata/surveydata/" , year , "/SASOUT" , substr( year , 3 , 4 ) , ".SAS" )

# read the entire sas importation script into memory
z <- readLines( sas_ri,encoding="latin1" )

# replace all underscores in variable names with x's
z <- gsub( "_" , "x" , z , fixed = TRUE )

# throw out these three fields, which overlap other fields and therefore are not supported by SAScii
# (see the details section at the bottom of page 9 of http://cran.r-project.org/web/packages/SAScii/SAScii.pdf for more detail)
z <- z[ !grepl( "SEQNO" , z ) ]
z <- z[ !grepl( "IDATE" , z ) ]
z <- z[ !grepl( "PHONENUM" , z ) ]

# remove all special characters
z <- gsub( "\t" , " " , z , fixed = TRUE )
z <- gsub( "\f" , " " , z , fixed = TRUE )

# re-write the sas importation script to a file on the local hard drive
writeLines( z , tf )

# actually run the read.SAScii.monetdb() function
# and import the current fixed-width file into the monet database
read.SAScii.monetdb (
		fn ,
		tf ,
		beginline = 70 ,
		zipped = T ,						# the ascii file is stored in a zipped file
		tl = TRUE ,							# convert all column names to lowercase
		tablename = paste0( 'b' , year ) ,	# the table will be stored in the monet database as bYYYY.. for example, 2010 will be stored as the 'b2010' table
		connection = db
)

# create a data frame containing all weight, psu, and stratification variables for each year
survey.vars <-
		data.frame(
				year = 1984:2011 ,
				weight = c( rep( 'x_finalwt' , 10 ) , rep( 'xfinalwt' , 17 ) , 'xllcpwt' ) ,
				psu = c( rep( 'x_psu' , 10 ) , rep( 'xpsu' , 18 ) ) ,
				strata = c( rep( 'x_ststr' , 10 ) , rep( 'xststr' , 18 ) )
		)

# convert all columns in the survey.vars table to character strings,
# except the first
survey.vars[ , -1 ] <- sapply( survey.vars[ , -1 ] , as.character )

# hey why not take a look?
print( survey.vars )



# create four new variables containing character strings that point to..

# the table name within the monet database
tablename <- paste0( "b" , year )

# the taylor-series linearization columns used in the complex sample survey design
strata <- survey.vars[ survey.vars$year == year , 'strata' ]
psu <- survey.vars[ survey.vars$year == year , 'psu' ]
weight <- survey.vars[ survey.vars$year == year , 'weight' ]

# add a column containing all ones to the current table
dbSendUpdate( db , paste0( 'alter table ' , tablename , ' add column one int' ) )
dbSendUpdate( db , paste0( 'UPDATE ' , tablename , ' SET one = 1' ) )

# add a column containing the record (row) number
dbSendUpdate( db , paste0( 'alter table ' , tablename , ' add column idkey int auto_increment' ) )

# create a sqlsurvey complex sample design object
brfss.design <-
		sqlsurvey(
				weight = weight ,			# weight variable column (defined in the character string above)
				nest = TRUE ,				# whether or not psus are nested within strata
				strata = strata ,			# stratification variable column (defined in the character string above)
				id = psu ,					# sampling unit column (defined in the character string above)
				table.name = tablename ,	# table name within the monet database (defined in the character string above)
				key = "idkey" ,				# sql primary key column (created with the auto_increment line above)
				# check.factors = 10 ,		# defaults to ten
				database = monet.url ,		# monet database location on localhost
				driver = drv ,				# monet driver location on the local disk
				user = "monetdb" ,			# username
				password = "monetdb" 		# password
		)

# save the complex sample survey design
# into a single r data file (.rda) that can now be
# analyzed quicker than anything else.
save( brfss.design , file = paste( tablename , 'design.rda' ) )

dbListTables( db )		# print the tables stored in the current monet database to the screen

dbSendUpdate( db , "CREATE TABLE recoded_b2010 AS SELECT * FROM b2010 WITH DATA" )
dbSendUpdate( db , "ALTER TABLE recoded_b2010 ADD COLUMN drinks_per_month VARCHAR( 255 )" )
dbSendUpdate( db , "UPDATE recoded_b2010 SET drinks_per_month = '01' WHERE xdrnkmo3 = 0" )
dbSendUpdate( db , "UPDATE recoded_b2010 SET drinks_per_month = '02' WHERE xdrnkmo3 >= 1 AND xdrnkmo3 < 11" )
dbSendUpdate( db , "UPDATE recoded_b2010 SET drinks_per_month = '03' WHERE xdrnkmo3 >= 11 AND xdrnkmo3 < 26" )
dbSendUpdate( db , "UPDATE recoded_b2010 SET drinks_per_month = '04' WHERE xdrnkmo3 >= 26 AND xdrnkmo3 < 51" )
dbSendUpdate( db , "UPDATE recoded_b2010 SET drinks_per_month = '05' WHERE xdrnkmo3 >= 51" )
dbGetQuery( db , "SELECT drinks_per_month , xdrnkmo3 , COUNT(*) as number_of_records from recoded_b2010 GROUP BY drinks_per_month , xdrnkmo3 ORDER BY xdrnkmo3" )



brfss.recoded.design <-
		sqlsurvey(
				weight = 'xfinalwt' ,
				nest = TRUE ,
				strata = 'xststr' ,
				id = 'xpsu' ,
				table.name = 'recoded_b2010' ,				# note: this is the solitary change
				# the weight, strata, and id variables are hard-coded in this sqlsurvey() function call,
				# but their values haven't changed from the original 1984 - 2011 download all microdata.R script
				key = "idkey" ,
				# check.factors = 10 ,						# defaults to ten
				database = monet.url ,
				driver = drv ,
				user = "monetdb" ,
				password = "monetdb" 
		)

save( brfss.recoded.design , file = "recoded b2010 design.rda" )

dbDisconnect( db )


