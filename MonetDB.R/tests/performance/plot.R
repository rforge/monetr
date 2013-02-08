library(ggplot2)

rres <-read.table(file="r.tsv",sep="\t",header=F)
rjava <-read.table(file="rjava.tsv",sep="\t",header=F)

rres1 <-read.table(file="r1.tsv",sep="\t",header=F)
rres2 <-read.table(file="r2.tsv",sep="\t",header=F)
rres3 <-read.table(file="r3.tsv",sep="\t",header=F)

phpres <-read.table(file="php.tsv",sep="\t",header=F)
pyres <-read.table(file="py.tsv",sep="\t",header=F)
names(rres) <-names(rjava) <- names(rres1) <- names(rres2) <- names(rres3) <- names(pyres) <- names(phpres) <- c("num","rs","time")

phpres$test <- "PHP"
pyres$test  <- "Python"
rres$test   <- "R-trunk"
rres1$test  <- "R-v1"
rres2$test  <- "R-v2"
rres3$test  <- "R-v3"
rjava$test  <- "R+JDBC"

data <- rbind(rres,phpres,rjava)

pdf("time.pdf",width=8,height=5)
p  <- qplot(num, time, data=data, colour=as.factor(test), group= test, type="l", geom="path")
p + labs(title = "MonetDB and R - Total Time",x="Tuples (#)",y="Time (s)", colour="Implementation")
dev.off()

pdf("tps.pdf",width=8,height=5)
p <- qplot(num, rs, data=data, colour=as.factor(test), group= test, type="l", geom="path")
p + labs(title = "MonetDB and R - Tuples Per Second",x="Tuples (#)",y="Tuples/Second", colour="Implementation")
dev.off()