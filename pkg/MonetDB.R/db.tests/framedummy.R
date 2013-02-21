source("R/monetdb.R")
source("R/monetframe.R")

con <- dbConnect(dbDriver("MonetDB"), "monetdb://localhost:50000/acs", "monetdb", "monetdb")
almf <- monet.frame(con,"alabama")

repweights<-almf[,200:279]
sum(repweights[,1]*almf[,"agep"])