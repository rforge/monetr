library(RMonetDB)
monetdriver<-MonetDB(classPath="/export/scratch1/hannes/MonetDB-11.13.3/java/monetdb-jdbc-2.6.jar")
monet <- dbConnect(monetdriver, "jdbc:monetdb://localhost:50000/acs", "monetdb", "monetdb")

#monet.read.csv(monet,"/ufs/hannes/Downloads/ss10pal.csv","alabama",nrows=50000,locked=TRUE)

data2 <- dbReadTable(monet,"alabama")

str(data2)
