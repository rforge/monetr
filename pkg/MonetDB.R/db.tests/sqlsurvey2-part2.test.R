
# restart R

require(sqlsurvey)		# load sqlsurvey package (analyzes large complex design surveys)
require(MonetDB.R)		# load the MonetDB.R package (connects r to a monet database)
require(foreign) 		# load foreign package (converts data files into R)
require(downloader)		# downloads and then runs the source() function on scripts from github


dbname <- "acs"
dbport <- 50000
drv <- dbDriver("MonetDB")
monet.url <- paste0( "monetdb://localhost:" , dbport , "/" , dbname )



#setwd( "V:/temp/" )

load( "recoded b2010 design.rda" )


# connect the recoded complex sample design to the monet database #
brfss.r <- open( brfss.recoded.design , driver = drv , user = "monetdb" , password = "monetdb" )	# recoded

# works
svymean( ~drinks_per_month , brfss.r ) 

# works
svymean( ~drinks_per_month , brfss.r , se = TRUE ) 

# breaks
svytotal( ~drinks_per_month , brfss.r , se = TRUE )
#
## works
svymean( ~drinks_per_month , brfss.r , byvar = ~sex , se = TRUE )

svytotal( ~drinks_per_month , brfss.r , byvar = ~sex , se = TRUE )

print("SUCCESS")