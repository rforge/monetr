# this wraps a sql database (in particular MonetDB) with a DBI connector 
# to have it appear like a data.frame

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
"[.monet.frame" <- function(x, k, j,drop=TRUE) {	
	query <- attr(x,"resultSet")@env$query
	
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
				stop("Invalid column specification. Column indices have to be in range [1,",ncol(x),"].",sep="")			
			cols <- names(x)[cols]
		}
		if (!all(cols %in% names(x)))
			stop("Invalid column specification. Column names have to be in set {",paste(names(x),collapse=", "),"}.",sep="")			
		
		query <- sub("SELECT.+FROM",paste0("SELECT ",paste0(make.db.names(attr(x,"conn"),cols),collapse=", ")," FROM "),query)
	}
	
	if (length(rows) > 1 || !is.na(rows)) { # get around an error if cols is a vector...
		if (min(rows) < 1 || max(rows) > nrow(x)) 
			stop("Invalid row specification. Row indices have to be in range [1,",nrow(x),"].",sep="")			
		
		if (.is.sequential(rows)) {
			# find out whether we already have limit and/or offset set
			# our values are relative to them
	
			oldLimit <- 0
			oldOffset <- 0
			
			oldOffsetStr <- gsub("(.*offset[ ]+)(\\d+)(.*)","\\2",query,ignore.case=TRUE)
			if (oldOffsetStr != query) {
				oldOffset <- as.numeric(oldOffsetStr)
			}
			
			offset <- oldOffset + min(rows)-1 # offset means skip n rows, but r lower limit includes them
			limit <- max(rows)-min(rows)+1

			# remove old limit/offset from query
			# TODO: is this safe? UNION queries are particularly dangerous, again...
			query <- gsub("limit[ ]+\\d+|offset[ ]+\\d+","",query,ignore.case=TRUE)
			query <- sub(";? *$",paste0(" LIMIT ",limit," OFFSET ",offset),query)
		}
		else 
			warning(paste("row specification has to be sequential, but ",paste(rows,collapse=",")," is not. Try as.data.frame(x)[c(",paste(rows,collapse=","),"),] instead.",sep=""))
	}
	
	# this would be the only case for column selection where the 'drop' parameter has an effect.
	# we have to create a warning, since drop=TRUE is default behaviour and might be expected by users
	if ((length(cols) == 1 || length(rows) == 1) && drop) 
		warning("drop=TRUE for one-column or one-row results is not supported. Overriding to FALSE")
	
	# clear previous result set to free db resources waiting for fetch()
	dbClearResult(attr(x,"resultSet"))
	
	# construct and return new monet.frame for rewritten query
	monet.frame(attr(x,"conn"),query)
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

# TODO: how the hell...
subset.monet.frame <- function (x, subset, select, drop = FALSE, ...) {
	print("subset.monet.frame - Not implemented yet.")
	FALSE
}

# TODO: WHAT IS THIS?
Ops.monet.frame = function(e1,e2) {
	print("Ops.monet.frame - Not implemented yet.")
	FALSE
}

#TODO subset.monet.frame
# ?subset
# http://stat.ethz.ch/R-manual/R-patched/library/base/html/subset.html

`[<-.monet.frame` <- `dim<-.monet.frame` <- `dimnames<-.monet.frame` <- `names<-.monet.frame` <- function(x, j, k, ..., value) {
	stop("write operators not (yet) supported for monet.frame")
}
