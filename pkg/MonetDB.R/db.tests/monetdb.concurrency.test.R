library(MonetDB.R)
drv <- dbDriver("MonetDB")
con <- dbConnect(drv, "monetdb://localhost:50000/acs", "monetdb", "monetdb",timeout=100)

# basic MAPI/SQL test, sanity
stopifnot(identical(dbGetQuery(con,"SELECT 'DPFKG!'")[[1]],"DPFKG!"))

# write test table iris
data(iris)


testThing <- function(con) {
	dbWriteTable(con,"monetdbtest1",iris)
	dbWriteTable(con,"monetdbtest2",iris)
	dbWriteTable(con,"monetdbtest3",iris)
	stopifnot(identical(dbExistsTable(con,"monetdbtest1"),TRUE))
	stopifnot(identical(dbExistsTable(con,"monetdbtest2"),TRUE))
	stopifnot(identical(dbExistsTable(con,"monetdbtest3"),TRUE))
	
	res <- dbSendQuery(con,"SELECT * FROM monetdbtest1")
	
	on.exit(dbSendUpdate(con, "drop table monetdbtest1"),add=TRUE)
	on.exit(dbSendUpdate(con, "drop table monetdbtest2"),add=TRUE)
	on.exit(dbSendUpdate(con, "drop table monetdbtest3"),add=TRUE)
	
	data <- fetch(res,-1)
	#str(data)
}
testThing(con)
testThing(con)
testThing(con)

stopifnot(identical(dbExistsTable(con,"monetdbtest1"),FALSE))
stopifnot(identical(dbExistsTable(con,"monetdbtest2"),FALSE))
stopifnot(identical(dbExistsTable(con,"monetdbtest3"),FALSE))


dbDisconnect(con)