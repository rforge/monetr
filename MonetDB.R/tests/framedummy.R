source("../R/class.R")
source("../R/monet.frame.R")

con <- dbConnect(dbDriver("MonetDB"), "monetdb://localhost:50000/acs", "monetdb", "monetdb")


d <- monet.frame(con,"SELECT * FROM lineitem LIMIT 1000")

str(as.data.frame(d[4:5, 1:3])) # select rows and column
stop()

# works:
#str(as.data.frame(d))
#str(as.data.frame(d$l_partkey))



#str(as.data.frame(d[1:3]))      # select columns
#str(as.data.frame(d[,1:3]))    # same
#str(as.data.frame(d[2,drop=FALSE]))      # one column
#str(as.data.frame(d[,2,drop=FALSE]))  # the same
#str(as.data.frame(d[,2,drop=TRUE]))  # warning
#str(as.data.frame(d[,2]))  # warning
#str(as.data.frame(d[2,drop=TRUE]))  # warning
#str(as.data.frame(d[2]))  # warning
#
#
#str(as.data.frame(d[c("l_partkey","l_suppkey")]))
#str(as.data.frame(d["l_suppkey",drop=FALSE]))
#str(as.data.frame(d["l_suppkey",drop=TRUE])) #warning
#str(as.data.frame(d["l_suppkey"])) #warning




#str(as.data.frame(d[4:5, 1:3])) # select rows and column
#str(as.data.frame(d[4:5,])) # select only rows
#
#str(as.data.frame(d[1,,drop=FALSE]))        # a one-row data frame
#
#str(as.data.frame(d[1,]))					# a one-row data frame with warning
#str(as.data.frame(d[1,,drop=TRUE]))        # a one-row data frame with warning
#
#
#str(as.data.frame(d[c(1,4,6,7),]))        # gives a warning and has no effect
#
