source("../R/monetdb.R")
con <- dbConnect(dbDriver("MonetDB"), "monetdb://localhost:50000/acs", "monetdb", "monetdb",timeout=100)

#teststr <- paste(rep("0123456789#",500),collapse="")
#identical(teststr,dbGetQuery(con,paste("SELECT '",teststr,"'",sep=""))[[1]])

# manually
#res <- dbSendQuery(con,"CREATE TABLE dummy (a varchar(5), b varchar(5))")
#res <- dbSendQuery(con,"INSERT INTO dummy (a,b) VALUES('FOO','BAR')")
#dbReadTable(con,"dummy")
#res <- dbSendQuery(con,"UPDATE dummy SET b='BAT' WHERE a='FOO'")
#dbReadTable(con,"dummy")
#res <- dbSendQuery(con,"delete from dummy")
#dbReadTable(con,"dummy")
#dbRemoveTable(con,"dummy")


# automagically
c1 <- c("foo1","foo2","ev\"i'l")
c2 <- as.integer(c(1,2,3))
c3 <- c(1.1,2.2,3.3)
c4 <- c(TRUE,FALSE,NA)
data <- data.frame(c1,c2,c3,c4)
names(data) <- c("by","select","group","type")
data
dbWriteTable(con,"dummy",data)
dbReadTable(con,"dummy")
dbRemoveTable(con,"dummy")

