monetdb.server.start <-
		function( bat.file ){
	
	if ( .Platform$OS.type != "windows" ) {
		
		stop( "mserver.start() not implemented yet on non-windows platforms. To come." )
		
	} else {
		
		# capture the result of a `tasklist` system call
		before.win.tasklist <- system2( 'tasklist' , stdout = TRUE )
		
		# store all pids before running the process
		before.pids <- substr( before.win.tasklist[ -(1:3) ] , 27 , 35 )
		
		# run the process
		shell.exec( bat.file )
		
		# capture the result of a `tasklist` system call
		after.win.tasklist <- system2( 'tasklist' , stdout = TRUE )
		
		# store all tasks after running the process
		after.tasks <- substr( after.win.tasklist[ -(1:3) ] , 1 , 25 )
		
		# store all pids after running the process
		after.pids <- substr( after.win.tasklist[ -(1:3) ] , 27 , 35 )
		
		# store the number in the task list containing the PIDs you've just initiated
		initiated.pid.positions <- which( !( after.pids %in% before.pids ) )
		
		# remove whitespace
		after.tasks <- gsub( " " , "" , after.tasks )
		
		# find the pid position that matches the executable file name
		correct.pid.position <- 
				intersect(
						grep( "mserver" , after.tasks ) ,
						initiated.pid.positions 
				)
		
		
		# remove whitespace
		correct.pid <- gsub( " " , "" , after.pids[ correct.pid.position ] )
		
		# return the correct process ID
		return( correct.pid )
		# the process ID will then be used inside monet.kill() to end the mserver5.exe from within R
	}
	
}


monetdb.server.stop <-
		function( correct.pid ){
	
	if ( .Platform$OS.type != "windows" ) {
		
		stop( "mserver.stop() not implemented yet on non-windows platforms. To come." )
		
	} else {
		
		# write the taskkill command line
		taskkill.cmd <- 
				paste( 
						"taskkill" , 
						"/PID" , 
						correct.pid ,
						"/F"
				)
		
		# kill the same process that was loaded
		system( taskkill.cmd )
		
	}
	
}


monetdb.server.setup <-
		function( 
				
				# set the path to the directory where the initialization batch file and all data will be stored
				database.directory ,
				# must be empty or not exist
				
				# find the main path to the monetdb installation program
				monetdb.program.path = "C:/Program Files/MonetDB/MonetDB5" ,
				
				# choose a database name
				dbname = "demo" ,
				
				# choose a database port
				# this port should not conflict with other monetdb databases
				# on your local computer.  two databases with the same port number
				# cannot be accessed at the same time
				dbport = 50000
){
	
	
	if ( .Platform$OS.type != "windows" ) 
		stop( "mserver.setup() not implemented yet on non-windows platforms. To come." )
	# in other cases, create a shellscript... (*.sh) with the same effect as the .bat file
	
	
	# switch all slashes to match windows
	monetdb.program.path <- normalizePath( monetdb.program.path , mustWork = FALSE )
	database.directory <- normalizePath( database.directory , mustWork = FALSE )
	
	
	# determine that the monetdb.program.path has been correctly specified #
	
	# first find the alleged path of mclient.exe
	mcl <- file.path( monetdb.program.path , "bin" , "mclient.exe" )
	
	# then confirm it exists
	if( !file.exists( mcl ) ) stop( paste( mcl , "does not exist.  are you sure monetdb.program.path has been specified correctly?" ) )
	
	
	# confirm that the database directory is either empty or does not exist
	
	# if the database directory does not exist, print that it's being created
	if ( !file.exists( database.directory ) ) {
		
		# create it
		dir.create( database.directory )
		# and say so
		message( paste( database.directory , "did not exist.  now it does" ) )
		
	} else {
		
		# otherwise confirm there's nothing in it
		if( length( list.files( database.directory , include.dirs = TRUE ) ) > 0 ) stop( 
					paste( 
							database.directory , 
							"must be empty.  either delete its contents or choose a different directory to store your database" 
					) 
			)
		
	}
	
	
	# determine the batch file's location on the local disk
	bfl <- normalizePath( file.path( database.directory , paste0( dbname , ".bat" ) ) , mustWork = FALSE )
	
	# determine the dbfarm's location on the local disk
	dbfl <- normalizePath( file.path( database.directory , dbname ) , mustWork = FALSE )
	
	# create the dbfarm's directory
	dir.create( dbfl )
	
	# store all file lines for the .bat into a character vector
	bat.contents <-
			c(
					'@echo off' ,
					
					'setlocal' ,
					
					'rem figure out the folder name' ,
					paste0( 'set MONETDB=' , monetdb.program.path ) ,
					
					'rem extend the search path with our EXE and DLL folders' ,
					'rem we depend on pthreadVC2.dll having been copied to the lib folder' ,
					'set PATH=%MONETDB%\\bin;%MONETDB%\\lib;%MONETDB%\\lib\\MonetDB5;%PATH%' ,
					
					'rem prepare the arguments to mserver5 to tell it where to put the dbfarm' ,
					
					'if "%APPDATA%" == "" goto usevar' ,
					'rem if the APPDATA variable does exist, put the database there' ,
					paste0( 'set MONETDBDIR=' , database.directory , '\\' ) ,
					paste0( 'set MONETDBFARM="--dbpath=' , dbfl , '"' ) ,
					'goto skipusevar' ,
					':usevar' ,
					'rem if the APPDATA variable does not exist, put the database in the' ,
					'rem installation folder (i.e. default location, so no command line argument)' ,
					'set MONETDBDIR=%MONETDB%\\var\\MonetDB5' ,
					'set MONETDBFARM=' ,
					':skipusevar' ,
					
					'rem the SQL log directory used to be in %MONETDBDIR%, but we now' ,
					'rem prefer it inside the dbfarm, so move it there' ,
					
					'if not exist "%MONETDBDIR%\\sql_logs" goto skipmove' ,
					paste0( 
							'for /d %%i in ("%MONETDBDIR%"\\sql_logs\\*) do move "%%i" "%MONETDBDIR%\\' ,
							dbname , 
							'"\\%%~ni\\sql_logs'
					) ,
					'rmdir "%MONETDBDIR%\\sql_logs"' ,
					':skipmove' ,
					
					'rem start the real server' ,
					paste0( 
							'"%MONETDB%\\bin\\mserver5.exe" --set "prefix=%MONETDB%" --set "exec_prefix=%MONETDB%" %MONETDBFARM% %* --set mapi_port=' ,
							dbport 
					) ,
					
					'if ERRORLEVEL 1 pause' ,
					
					'endlocal'
			
			)
	
	
	
	# write the .bat contents to a file on the local disk
	writeLines( bat.contents , bfl )
	
	# return the filepath to the batch file
	bfl
}