# this wraps a sql database (in particular MonetDB) with a DBI connector 
# to have it appear like a data.frame

DEBUG_REWRITE   <- FALSE


# can either be given a query or simply a table name
monet.frame <- function(conn,thingy,...) {
	if(missing(conn)) stop("'conn' must be specified")
	if(missing(thingy)) stop("a sql query or a table name must be specified")
	
	obj = new.env()
	class(obj) = "monet.frame"
	attr(obj,"conn") <- conn
	query <- thingy
	
	if (dbExistsTable(conn,thingy)) {
		query <-  paste0("SELECT * FROM ",make.db.names(conn,thingy,allow.keywords=FALSE))
	}
	res <- dbSendQuery(conn, query)
	if(!res@env$success)
		stop(paste0("Unable to execute (constructed) query '",query,"'. Server says '",res@env$message,"'."))
	attr(obj,"resultSet") <- res
	return(obj)
}

.element.limit <- 10000000

as.data.frame.monet.frame <- function(x, row.names, optional,warnSize=TRUE,...) {
	# check if amount of tuples/fields is larger than some limit
	# raise error if over limit and warnSize==TRUE
	if (ncol(x)*nrow(x) > .element.limit && warnSize) 
		stop(paste0("The total number of elements to be loaded is larger than ",.element.limit,". This is probably very slow. Consider dropping columns and/or rows, e.g. using the [] function. If you really want to do this, call as.data.frame() with the warnSize parameter set to FALSE."))
	# get result set object from frame
	resultSet <- attr(x,"resultSet")
	# fetch results
	res <- fetch(resultSet,-1)
	dbClearResult(resultSet)
	# return
	res
}

# this is the fun part. this method has infinity ways of being invoked :(
# http://stat.ethz.ch/R-manual/R-patched/library/base/html/Extract.data.frame.html

# TODO: handle negative indices and which() calls. which() like subset!

"[.monet.frame" <- function(x, k, j,drop=TRUE) {	
	nquery <- query <- attr(x,"resultSet")@env$query
	
	cols <- NA
	rows <- NA
	
	# biiig fun with nargs to differentiate d[1,] and d[1]
	# all in the presence of the optional drop argument, yuck!
	args <- nargs()
	if (!missing(drop)) {
		args <- args-1
	}
	if (args == 2 && missing(j)) cols <- k
	if (args == 3 && !missing(j)) cols <- j
	if (args == 3 && !missing(k)) rows <- k
		
	if (length(cols) > 1 || !is.na(cols)) { # get around an error if cols is a vector...
		# if we have a numeric column spec, find the appropriate names
		if (is.numeric(cols)) {
			if (min(cols) < 1 || max(cols) > ncol(x)) 
				stop(paste0("Invalid column specification '",cols,"'. Column indices have to be in range [1,",ncol(x),"].",sep=""))			
			cols <- names(x)[cols]
		}
		if (!all(cols %in% names(x)))
			stop(paste0("Invalid column specification '",cols,"'. Column names have to be in set {",paste(names(x),collapse=", "),"}.",sep=""))			
		
		nquery <- sub("SELECT.+FROM",paste0("SELECT ",paste0(make.db.names(attr(x,"conn"),cols),collapse=", ")," FROM "),query)
	}
	
	if (length(rows) > 1 || !is.na(rows)) { # get around an error if cols is a vector...
		if (min(rows) < 1 || max(rows) > nrow(x)) 
			stop("Invalid row specification. Row indices have to be in range [1,",nrow(x),"].",sep="")			
		
		if (.is.sequential(rows)) {
			# find out whether we already have limit and/or offset set
			# our values are relative to them
	
			oldLimit <- 0
			oldOffset <- 0
			
			oldOffsetStr <- gsub("(.*offset[ ]+)(\\d+)(.*)","\\2",nquery,ignore.case=TRUE)
			if (oldOffsetStr != nquery) {
				oldOffset <- as.numeric(oldOffsetStr)
			}
			
			offset <- oldOffset + min(rows)-1 # offset means skip n rows, but r lower limit includes them
			limit <- max(rows)-min(rows)+1

			# remove old limit/offset from query
			# TODO: is this safe? UNION queries are particularly dangerous, again...
			nquery <- gsub("limit[ ]+\\d+|offset[ ]+\\d+","",nquery,ignore.case=TRUE)
			nquery <- sub(";? *$",paste0(" LIMIT ",limit," OFFSET ",offset),nquery,ignore.case=TRUE)
		}
		else 
			warning(paste("row specification has to be sequential, but ",paste(rows,collapse=",")," is not. Try as.data.frame(x)[c(",paste(rows,collapse=","),"),] instead.",sep=""))
	}
	
	# this would be the only case for column selection where the 'drop' parameter has an effect.
	# we have to create a warning, since drop=TRUE is default behaviour and might be expected by users
	if (((!is.na(cols) && length(cols) == 1) || (!is.na(rows) && length(rows) == 1)) && drop) 
		warning("drop=TRUE for one-column or one-row results is not supported. Overriding to FALSE")
	
	# clear previous result set to free db resources waiting for fetch()
	dbClearResult(attr(x,"resultSet"))
	
	# construct and return new monet.frame for rewritten query

	if (DEBUG_REWRITE)  cat(paste0("RW: '",query,"' >> '",nquery,"'\n",sep=""))	

	monet.frame(attr(x,"conn"),nquery)
}

.is.sequential <- function(x, eps=1e-8) {
	if (length(x) && isTRUE(abs(x[1] - floor(x[1])) < eps)) {
		all(abs(diff(x)-1) < eps)
	} else {
		FALSE
	}
}

# shorthand for frame[columnname/id,drop=FALSE]
"$.monet.frame"<-function(x,i) {
	x[i,drop=FALSE]
}

# returns a single row with one index/element with two indices
"[[.monet.frame"  <- function(x, j, ...) {
	print("[[.monet.frame - Not implemented yet.")
	FALSE
}

str.monet.frame <- summary.monet.frame <- function(object, ...) {
	cat("MonetDB-backed data.frame surrogate\n")
	cat(paste0("Query: ",attr(object,"resultSet")@env$query,"\n"))	
	str(as.data.frame(object[1:6,,drop=FALSE]))
}

print.monet.frame <- function(x, ...) {
	print(as.data.frame(x))
}

names.monet.frame <- function(x) {
	attr(x,"resultSet")@env$info$names
}

dim.monet.frame <- function(x) {
	c(attr(x,"resultSet")@env$info$rows,attr(x,"resultSet")@env$info$cols)
}


# http://stat.ethz.ch/R-manual/R-patched/library/base/html/subset.html
subset.monet.frame<-function(x,subset,...){
	query <- attr(x,"resultSet")@env$query
	subset<-substitute(subset)
	restr <- sqlexpr(subset)

	if (length(grep(" where ",query,ignore.case=TRUE)) > 0) {
		nquery <- sub("where (.*?) (group|having|order|limit|;)",paste0("where \\1 AND ",restr," \\2"),query,ignore.case=TRUE)
	}
	else {
		nquery <- sub("(group|having|order|limit|;|$)",paste0(" where ",restr," \\1"),query,ignore.case=TRUE)
	}
	# clear previous result set to free db resources waiting for fetch()
	dbClearResult(attr(x,"resultSet"))
	
	if (DEBUG_REWRITE)  cat(paste0("RW: '",query,"' >> '",nquery,"'\n",sep=""))	
	
	# construct and return new monet.frame for rewritten query
	monet.frame(attr(x,"conn"),nquery)	
}

#   ‘"+"’, ‘"-"’, ‘"*"’, ‘"/"’, ‘"^"’, ‘"%%"’, ‘"%/%"’
#   ‘"&"’, ‘"|"’, ‘"!"’
#   ‘"=="’, ‘"!="’, ‘"<"’, ‘"<="’, ‘">="’, ‘">"’
Ops.monet.frame <- function(e1,e2) {
	unary <- nargs() == 1L
	lclass <- nzchar(.Method[1L])
	rclass <- !unary && (nzchar(.Method[2L]))
	
	# this will be the next SELECT x thing
	nexpr <- ""
	
	left <- right <- query <- queryresult <- conn <- NA
	
	# both values are monet.frame
	if (lclass && rclass) {
		if (any(dim(e1) != dim(e2))) 
			stop(.Generic, " only defined for equally-sized frames")
		e1c <- names(e1)[[1]]
		e2c <- names(e2)[[1]]
		
		# TODO: check whether both frames are from same table? how to do this otherwise? join?
		# something like that, grep the FROM part and compare?
	}

	# TODO: really close result set here? If this fails, fetch() will fail...
	# TODO: check data types of db columns, have to be something numerical
	
	# left operand is monet.frame
	else if (lclass) {
		if (length(names(e1)) != 1) 
			stop(.Generic, " only defined for one-column frames, consider using $ first")
		if (!is.numeric(e2))
			stop(e2," is not a numeric operand.")
		query <- attr(e1,"resultSet")@env$query
		conn <- attr(e1,"conn")
		dbClearResult(attr(e1,"resultSet"))
		left <- make.db.names(attr(e1,"resultSet")@env$conn,names(e1)[[1]])
		right <- e2
	}
	# right operand is monet.frame
	else {
		if (length(names(e2)) != 1) 
			stop(.Generic, " only defined for one-column frames, consider using $ first")
		if (!is.numeric(e1))
			stop(e1," is not a numeric operand.")
		query <- attr(e2,"resultSet")@env$query
		conn <- attr(e2,"conn")
		dbClearResult(attr(e2,"resultSet"))
		left <- e1
		right <- make.db.names(attr(e2,"resultSet")@env$conn,names(e2)[[1]])
	}
			
	if (.Generic %in% c("+", "-", "*", "/")) {
		nexpr <- paste0(left,.Generic,right)
	}
	if (.Generic == "^") {
		nexpr <- paste0("POWER(",left,",",right,")")
	}
	if (.Generic == "%%") {
		nexpr <- paste0(left,"%",right)
	}
	if (nexpr == "") 
		stop(.Generic, " not supported (yet). Sorry.")
	
	# replace the thing between SELECT and WHERE with the new value and return new monet.frame
	nquery <- sub("select (.*?) from",paste0("select ",nexpr," from"),query,ignore.case=TRUE)
	
	# clear previous result set to free db resources waiting for fetch()
	
	if (DEBUG_REWRITE)  cat(paste0("RW: '",query,"' >> '",nquery,"'\n",sep=""))	
	
	# construct and return new monet.frame for rewritten query
	monet.frame(conn,nquery)	
}



# TODO: implement
#   ‘all’, ‘any’
#   ‘prod’
#   ‘range’ (?)

# TODO: how to handle na.rm? Does SQL consider NULLs?

Summary.monet.frame <- function(x,na.rm=FALSE) {
	col <- make.db.names(attr(x,"resultSet")@env$conn,names(x)[[1]])
	query <- attr(x,"resultSet")@env$query
	conn <- attr(x,"conn")
	
	if (.Generic %in% c("min", "max", "sum")) {
		# TODO: check if column is numeric
		dbClearResult(attr(x,"resultSet"))
		nexpr <- paste0(.Generic,"(",col,")")
	}
	if (nexpr == "") 
		stop(.Generic, " not supported (yet). Sorry.")
	
	# replace the thing between SELECT and WHERE with the new value and return new monet.frame
	nquery <- sub("select (.*?) from",paste0("select ",nexpr," from"),query,ignore.case=TRUE)
	
	# clear previous result set to free db resources waiting for fetch()
	
	if (DEBUG_REWRITE)  cat(paste0("RW: '",query,"' >> '",nquery,"'\n",sep=""))	
	
	# construct and return new monet.frame for rewritten query
	as.data.frame(monet.frame(conn,nquery))[[1]]	
}


#   ‘abs’, ‘sign’, ‘sqrt’, ‘floor’, ‘ceiling’, ‘trunc’, ‘round’, ‘signif’
#   ‘exp’, ‘log’, ‘expm1’, ‘log1p’, ‘cos’, ‘sin’, ‘tan’, ‘acos’, ‘asin’, ‘atan’, ‘cosh’, ‘sinh’, ‘tanh’, ‘acosh’, ‘asinh’, ‘atanh’
#   ‘lgamma’, ‘gamma’, ‘digamma’, ‘trigamma’
#   ‘cumsum’, ‘cumprod’, ‘cummax’, ‘cummin’
Math.monet.frame <- function(x) {
	# TODO not now.
}


# 'borrowed' from sqlsurvey, translates a subset() argument to sqlish
sqlexpr<-function(expr, design){
	nms<-new.env(parent=emptyenv())
	assign("%in%"," IN ", nms)
	assign("&", " AND ", nms)
	assign("=="," = ",nms)
	assign("|"," OR ", nms)
	assign("!"," NOT ",nms)
	assign("I","",nms)
	assign("~","",nms)
	out <-textConnection("str","w",local=TRUE)
	inorder<-function(e){
		if(length(e) ==1) {
			cat(e, file=out)
		} else if (e[[1]]==quote(is.na)){
			cat("(",file=out)
			inorder(e[[2]])
			cat(") IS NULL", file=out)
		} else if (length(e)==2){
			nm<-deparse(e[[1]])
			if (exists(nm, nms)) nm<-get(nm,nms)
			cat(nm, file=out)
			cat("(", file=out)
			inorder(e[[2]])
			cat(")", file=out)
		} else if (deparse(e[[1]])=="c"){
			cat("(", file=out)
			for(i in seq_len(length(e[-1]))) {
				if(i>1) cat(",", file=out)
				inorder(e[[i+1]])
			}
			cat(")", file=out)
		} else if (deparse(e[[1]])==":"){
			cat("(",file=out)
			cat(paste(eval(e),collapse=","),file=out)
			cat(")",file=out)
		} else{
			cat("(",file=out)
			inorder(e[[2]])
			nm<-deparse(e[[1]])
			if (exists(nm,nms)) nm<-get(nm,nms)
			cat(nm,file=out)
			inorder(e[[3]])
			cat(")",file=out)
		}
		
	}
	inorder(expr)
	close(out)
	paste("(",str,")")
	
}

`[<-.monet.frame` <- `dim<-.monet.frame` <- `dimnames<-.monet.frame` <- `names<-.monet.frame` <- function(x, j, k, ..., value) {
	stop("write operators not (yet) supported for monet.frame")
}
