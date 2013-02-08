monet.frame <- function(conn,query,...) {
	if(missing(conn)) stop("'conn' must be specified")
	if(missing(query)) stop("'query' must be specified")
	
	# TODO: set initial result set size to something small, like 100 or 1000 tuples
	obj = new.env()
	class(obj) = "monet.frame"
	
	attr(obj,"resultSet") <- dbSendQuery(conn, query, ...)
	attr(obj,"conn") <- conn
	
	return(obj)
}


# First method to be almost done
as.data.frame.monet.frame <- function(x) {
	# get result set object from frame
	resultSet <- attr(x,"resultSet")
	# fetch results
	res <- fetch(resultSet,-1)
	dbClearResult(resultSet)
	# return
	res
}

# shorthand for frame[columnname/id,drop=FALSE]
"$.monet.frame"<-function(x,i) {
	x[i,drop=FALSE]
}

names.monet.frame <- function(x) {
	# heh...
	attr(x,"resultSet")@env$info$names
}

# this is the fun part. this method has infinity ways of being invoked :(
# http://stat.ethz.ch/R-manual/R-patched/library/base/html/Extract.data.frame.html
`[.monet.frame` <- function(x, k, j,drop=TRUE) {	
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
		# TODO: check these values for validity
		if (is.numeric(cols)) {
			cols <- names(x)[cols]
		}
		query <- sub("SELECT.+FROM",paste0("SELECT ",paste0(make.db.names(attr(x,"conn"),cols),collapse=",")," FROM "),query)
	}
	
	if (length(rows) > 1 || !is.na(rows)) { # get around an error if cols is a vector...
		if (is.sequential(rows)) {
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

is.sequential <- function(x, eps=1e-8) {
	if (length(x) && isTRUE(abs(x[1] - floor(x[1])) < eps)) {
		all(abs(diff(x)-1) < eps)
	} else {
		FALSE
	}
}

`[[.monet.frame` = function(x, j, ...)
{
	print("[[.monet.frame")
	if(!missing(j)) print(paste0("j=",j))
	return(NULL)
	
}

#
#sw <- swiss[1:5, 1:4]  # select a manageable subset
#
#sw[1:3]      # select columns
#sw[, 1:3]    # same
#sw[4:5, 1:3] # select rows and columns
#sw[1]        # a one-column data frame
#sw[, 1, drop = FALSE]  # the same
#sw[, 1]      # a (unnamed) vector
#sw[[1]]      # the same
#
#sw[1,]       # a one-row data frame
#sw[1,, drop = TRUE]  # a list

# TODO: WHAT IS THIS?
Ops.monet.frame = function(e1,e2) {
	print("Ops.monet.frame")
	FALSE
#	col = e1$which
#	e1$which = NULL
#	NP = tryCatch(
#			as.integer(options("lazy.frame.threads")),
#			error=function(e) 2L)
#	if(is.null(NP) || is.na(NP) || length(NP)<1) NP = 2L
#	OP <- switch(.Generic,"=="=1L,
#			"!="=2L,
#			">="=3L,
#			"<="=4L,
#			">"= 5L,
#			"<"= 6L)
#	if(!inherits(e1,"lazy.frame")) stop("Left-hand side must be lazy.frame object")
#	if(is.null(col)) stop("Can only compare a single column")
#	w = .Call("WHICH",e1$data,
#			as.integer(col),
#			as.integer(e1$row.names),
#			as.integer(e1$internalskip),
#			as.character(e1$sep),
#			OP, e2, NP)
#	w[w>0]
}

`dim.monet.frame` = function(x)
{
	print("dim.monet.frame")
}



`dimnames.monet.frame` = function(x)
{
	x$dimnames
}


`head.monet.frame` = function(x, n=6L, ...)
{
	print("head.monet.frame")
}

`tail.monet.frame` = function(x, n=6L, ...)
{
	print("tail.monet.frame")
}

`str.monet.frame` = function(object, ...)
{
	print("str.monet.frame")
#	cat("Str summary of the file.object internals:\n")
#	print(ls.str(object))
#	cat("\nStr summary of the data head:\n")
#	str(object[1:min(nrow(object),6),,drop=FALSE])
#	cat("The complete data set consists of",object$dim[[1]],"rows.\n")
}

print.monet.frame = function(x, ...)
{
	print("print.monet.frame")
#	cat("\nLazy person's file-backed data frame for",x$call$file,"\n\n")

}


#TODO: rework this, $ will not work any more!
`ncol.monet.frame` = function(x) x$dim[2]
`nrow.monet.frame` = function(x) x$dim[1]
`dim.monet.frame` = function(x) x$dim

#TODO suubset.monet.frame
# ?subset
# http://stat.ethz.ch/R-manual/R-patched/library/base/html/subset.html

summary.monet.frame = function(x)
{
	print("summary.monet.frame")
#	warning("Not yet supported")
#	invisible()
}

`[<-.monet.frame` <- `dim<-.monet.frame` <- `dimnames<-.monet.frame` <- `names<-.monet.frame` <- function(x, j, k, ..., value) {
	stop("write operators not (yet) supported for monet.frame")
}
