library(sqlsurvey)
library(MonetR)

## this takes a long time, but only needs to be done once
alacs<-sqlrepsurvey(weight="pwgtp", repweights=paste("pwgtp",1:80,sep=""), scale=4/80, rscales=rep(1,80), mse=TRUE, database="monetdb://localhost:50000/acs",
		driver=dbDriver("MonetR"), key="idkey", table.name="acs3yr", user="monetdb",password="monetdb")

## do stuff

## totals by age and sex
svytotal(~sex,alacs)
svytotal(~cut(agep,c(4,9,14,19,24,34,44,54,59,64,74,84)),alacs)

## wage income by working hours
svyplot(wagp~wkhp,alacs, style="hex")
plot(svysmooth(wagp~wkhp,alacs, sample.bandwidth=5000))
svymean(~wagp,alacs,byvar=~sex)

## non-citizens more likely to be male
svytable(~sex+cit,alacs)
svychisq(~sex+cit,alacs)