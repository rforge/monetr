# run on TPC-H SF1 in MonetDB

source("/ufs/hannes/workspace/MonetR/R/class.R")

con <- dbConnect(dbDriver("MonetDB"), "monetdb://localhost:50000/mydb", "monetdb", "monetdb")

options(scipen=999)
r <- data.frame()

for (n in seq(1000,50000,by=1000)) {
	t <- system.time(dbGetQuery(con,paste("select * from partsupp limit ",n)))
	time <- t[3]
	rs <- n/time
	d <- data.frame(n=n,rs=rs,time=time)
	r <- rbind(r,d)
	print(r)
}

write.table(r,file="r.tsv",sep="\t",row.names=FALSE,col.names=FALSE)
warnings()