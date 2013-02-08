library("MonetR")
con <- dbConnect(dbDriver("MonetDB"), "monetdb://localhost:50000/acs", "monetdb", "monetdb")

Rprof("/tmp/rprof",memory.profiling=TRUE,interval=0.01)
for (i in seq.int(1)) {
	res <- dbGetQuery(con, "select count(*) from alabama where pwgtp>0")
	str(res)
	}
Rprof()
summaryRprof("/tmp/rprof",memory="both")
