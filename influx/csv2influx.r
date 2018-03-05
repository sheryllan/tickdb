#!/usr/bin/env Rscript

suppressMessages(library(doParallel))
registerDoParallel()

suppressMessages(library(bit64))
suppressMessages(library(Rcpp))
suppressMessages(library(readr))
suppressMessages(library(stringr))
suppressMessages(library(compiler))
suppressMessages(library(plyr))
suppressMessages(library(foreach))
suppressMessages(library(iterators))
suppressMessages(library(httr))
suppressMessages(library(influxdbr2))
suppressMessages(library(jsonlite))
suppressMessages(library(data.table))

j=enableJIT(3)
options(readr.show_progress=F)
WAIT = 3 # seconds between each attempt to write the same chunk in InfluxDB
COUNT= 100# Number of tries before we give up on a chunk of data
NBCORES=8

sourceCpp("cpaste.cpp")

printf <- function(...) invisible(cat(sprintf(...)))
rien <- function(...) NULL

colclass <- function(c)
{
	unname(
		sapply(unlist(strsplit(c,'')),
			   function(c)
			   {
				   switch(c,
						  'c' = 'character',
						  'n' = 'numeric',
						  'i' = 'integer'
						  )
			   }))
}

read_csv_file <- function(f)
{
	csvnames <- c('otype','recv','exch','bid1','bid2','bid3','bid4','bid5',
				  'bidv1','bidv2','bidv3','bidv4','bidv5',
				  'nbid1','nbid2','nbid3','nbid4','nbid5',
				  'ask1','ask2','ask3','ask4','ask5',
				  'askv1','askv2','askv3','askv4','askv5',
				  'nask1','nask2','nask3','nask4','nask5')
	
	# Check if old style header
	x = xzfile(f)
	header = all(c("series","strike","call_put") %in% unlist(str_split(readLines(x,1),',')))
	if(header) # old style
	{
		csvnames = c(csvnames,"market","type","prod","series","strike","call_put","version")
		csvformat <- "cccnnnnniiiiiiiiiinnnnniiiiiiiiiiccccccc"
		style = "old"
	} else { # new style
		csvnames = c(csvnames,'product','state')
		csvformat <- "cccnnnnniiiiiiiiiinnnnniiiiiiiiiicc"
		style = "new"
	}

	close(x)

	# Read file
	xz = paste("xz -cdk ",f)
	result = fread(xz,sep=',',colClasses=colclass(csvformat),
				   skip=1, col.names=csvnames,
				   blank.lines.skip=T,
				   fill=T, showProgress=F,data.table=F) # read data

	# Set 'style' and column name
	attr(result,"style")=style
	if(attr(result,"style")=="old")
	{
		names(result)[which(names(result)=="call_put")]="cp"
	}

	return(result)
}

# generate split indices of the data.frame (for // execution)
generate_df_split <- function(N,gen='by',size=NBCORES)
{
	if(gen=='by') i = seq(1,N,by=size)
	else i = round(seq(1,N,length.out=size))

	j = i-1
	j = c(j[2:length(j)],N)
	data.frame(i,j)
}

decompose_filename <- function(f)
{
	l <- rev(unlist(str_split(f,"[.-]"))) # decompose filename
	compression = l[1]
	# l[2] = "csv"
	capture_time = l[3]
	date = l[4]
	if(str_detect(l[5],"JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC"))
	{
		expiry = l[5]
		type = l[6]
		product = l[7]
		named.list(product,type,expiry,date,capture_time,compression)
	}
	else {
		type = l[5]
		product = l[6]
		named.list(product,type,date,capture_time,compression)
	}
}

# Parallel conversion of big data.frames to influx format
df2influx <- function(df,param,measurement)
{
	if(measurement=='trade' & param$type=='O') # filter out bad trades (bugs, etc...)
	{
		df = df[ !is.na(df$product), ] # keep only trades with a product name
	}

	if(nrow(df)==0)
		return(character(0))

	# parse option instrument strings
	if(param$type=='O') # options
	{
		if(attr(df,"style")=="new")
		{
			x = str_split(df$product,fixed('.'),simplify=T)
			opt_tags = v2paste( str_c("strike=", str_replace(x[,5],fixed(','),'.')), # strike
						str_c('cp=',x[,6],''), # option types
						',') # generate tags for options
		} else {
			opt_tags = v2paste(str_c('strike=',df$strike), str_c('cp=',df$cp),',')
		}
		ofield=T
	} else ofield=F

	# header used for each line
	if(param$type=='O' | param$type=='F')
	{
		header=sprintf('%s,type=%s,product=%s,expiry=%s',measurement, param$type, param$product,param$expiry)
	} else 
	{
		header=sprintf('%s,type=%s,product=%s',measurement, param$type, param$product)
	}

	if(measurement=="book")
	{
		book = df[ , 
			c('bid1','bid2','bid3','bid4','bid5', 'bidv1','bidv2','bidv3','bidv4','bidv5',
			  'nbid1','nbid2','nbid3','nbid4','nbid5', 'ask1','ask2','ask3','ask4','ask5',
			  'askv1','askv2','askv3','askv4','askv5', 'nask1','nask2','nask3','nask4','nask5')]
	} else {
		book = df[ , c('bid1','bid2','bid3')]
		names(book)=c('price','volume','side')
	}

	# add double-quotes to otype
	otype=str_c('otype="',df$otype,'"')
	# add integer tag to exch timestamps
	exch = str_c("exch=",df$exch,'i')

	# add bid1=value etc... to each column
	previous_nb_cores = options("cores")$cores
	options(cores=NBCORES) # temporarily boost the number of cores here
	body = foreach(i=1:ncol(book),.combine='cbind') %dopar% {str_c(names(book)[i],'=',book[,i])}
	# collapse each row into a single string like bid1=12,bid2=11, etc...
	split = generate_df_split(nrow(book),gen='length')
	body <- foreach(s=iter(split,by='row'),.combine='c') %dopar% { cpaste(body[s$i:s$j,,drop=F]) }
	body <- v3paste(otype,exch,body,',') # add the mandatory fields for each record
	options(cores=previous_nb_cores)

	# add timestamps and options tags if needed
	if(ofield)
	{
		body <- v3paste(opt_tags,body,df$recv,' ')
		body <- fullpaste(header,body,',')
	} else {
		body <- v2paste(body,df$recv,' ')
		body <- fullpaste(header,body,' ')
	}

	return(body[!is.na(body)])
}

# Influx HTTP protocal returns 204 when things are good
test_response <- function(r,file,log,measurement)
{
	if(r$status_code!=204)
	{
		printf("%s - Error writing %s %s %s\n",Sys.time(),file,r$status_code,measurement)
		return(FALSE)
	} else return(TRUE)
}

# Send a vector 'vec' of Influx line protocal to the server
# Split in smaller blocks to avoid server congestion
write2influx <- function(vec,file,log,measurement,DBNAME,max_size=10000)
{
	loopwrite<-function(chunk)
	{
		count = COUNT
		repeat
		{
			response = httr::POST(url="", httr::timeout(60), scheme="http", hostname="127.0.0.1", port=8086, 
								  path="write", query=list(db=DBNAME,u="",p=""),
								  body=paste0(chunk,collapse='\n'))
			if(test_response(response,file,log,measurement)) # write OK
				break
			else { # write not OK
				printf("%s - Retrying to write %s chunk of %s\n",Sys.time(),measurement,file)
				Sys.sleep(WAIT) # if error, wait 3 seconds
				count = count - 1
				if(count==0)
				{
					printf("%s - Giving up on %s chunk of %s\n",Sys.time(),measurement,file)
					break
				}
			}
		}
	}

	if(length(vec)>max_size)
	{
		split = generate_df_split(length(vec),gen='by',size=max_size)
		for(i in 1:nrow(split))
		{
			loopwrite(vec[split$i[i]:split$j[i]])
		}
	} else {
			loopwrite(vec)
	}
}

# Read list of processed files from Influx
read_processed_file <- function(cfg,con)
{
	if(!is.null(con))
	{
		response = httr::GET(url="",scheme=con$scheme, hostname=con$host, port=con$port,
				  path="query",
				  query=list(db=cfg$dbname,u=con$user,p=con$pass,
							 q="select * from data_files"),
				  add_headers(Accept="application/csv"))
		donefiles = character(0)
		if(status_code(response)==200 & length(response$content)>0)
		{
			text = rawToChar(response$content)
			donefiles = readr::read_csv(text,col_types='cccc')$file
		}
	}

	printf("%s - found %d already processed files in the database\n",Sys.time(),length(donefiles))

	return(donefiles)
}

# Find new data files from MDrec repository
find_data_file <- function(cfg,con)
{
	donefiles = read_processed_file(cfg,con)

	if(length(cfg$contracts>0))
	{
		p = paste0(paste0(cfg$contracts,collapse='|'),collapse='|')
	}
	else p='*.csv.xz'

	# list files from the pattern	
	files = dir(cfg$capturedir, pattern = p,
				full.names=T, recursive=T) # get all the data files

	# Filter out ref and stats files
	files = grep("Reference",files,value=T,invert=T)
	files = grep("Statistic",files,value=T,invert=T)

	# Make the diff with already processed files
	newfiles = setdiff(files,donefiles)

	return(newfiles)
}

# Add processed files to Influx (after completely processing them)
add_file_to_db <- function(cfg,con,f,error=F)
{
	x = str_c("data_files file=\"",f,"\"")
	response=httr::POST(url="",httr::timeout(60),scheme="http",
			hostname=cfg$host,port=8086,path="write",
			query=list(db=cfg$dbname,u='',p=''),
			body=x)

	if(response$status_code!=204)
		printf("%s - Error writing data file name\n",Sys.time())
}

# Update the Influx db with new files
update_database <- function(config)
{
	# Read config and set logging file
	cfg = read_json(config,simplifyVector=T)
	if(!file.exists(cfg$logfile))
	{
		sink(cfg$logfile)
	} else sink(cfg$logfile,append=T)

	# create a database
	printf("%s - Starting database session\n",Sys.time())
	con = influxdbr2::influx_connection(host="127.0.0.1",port=8086)
	response = create_database(con,cfg$dbname) 
	
	# find new files and process them
	newfiles = find_data_file(cfg,con) 
	N = length(newfiles)
	printf("%s - %d new files will be added\n",Sys.time(),N)

	options(cores=8)
#	foreach(f=newfiles) %dopar%
	for(f in newfiles)
	{
		t0=Sys.time()
		printf("%s - processing %s\n",Sys.time(),f)
		param = decompose_filename(basename(f)) # get parameters from file name
		if(file.access(f,mode=0)==0 & file.access(f,mode=4)==0) # ignore non-readable files
		{
			df = tryCatch(read_csv_file(f),error=function(e) F)  # read data
			gc()
			if(is.data.frame(df))
			{
				printf("%s - read %s: %d rows\n",Sys.time(),f,nrow(df))
				# clean up data
				df = df[!is.na(as.integer64(df$recv)) , ] # remove bad recv
				df = df[!is.na(as.integer64(df$exch)) , ] # remove bad exch
	
				if(nrow(df)>0)
				{
					# split by trades and quotes
					trades = df$otype=='S' | df$otype=='T'
					quotes = !trades
	
					dft = df[trades, ]
					dfq = df[quotes, ]
					rm(df)
	
					# convert data.frame to line protocol
					if(nrow(dft)>0)
					{
						influxt=df2influx(dft,param,"trade") # convert mdrec to Influx line protocol
						rm(dft); gc()
						# Populate InfluxDB
						if(length(influxt)>0)
							write2influx(influxt,f,log,"trade",cfg$dbname)
						rm(influxt); gc()
					}
					if(nrow(dfq)>0)
					{
						influxq=df2influx(dfq,param,"book")
						rm(dfq);gc()
						# Populate InfluxDB
						if(length(influxq)>0)
							write2influx(influxq,f,log,"book",cfg$dbname)
						rm(influxq); gc()
					}
	
					msg<-sprintf("%s : %d trades %d quotes\n",basename(f),sum(trades),sum(quotes))
				} else {
					msg<-sprintf("%s : empty file\n",basename(f))
				}
				add_file_to_db(cfg,con,f) # update list of processed files in the db
				printf("%s - %s",Sys.time(), msg)
			} else {
				# Even if the file has an error, it's added to the list of processed files
				# Most of the time, the problem is with the old version of MDRec capture
				# which was not recording options correctly. We have approx. 6 months of
				# useless data (01-06 2016).
				add_file_to_db(cfg,con,f,error=T)
				printf("%s - Error reading %s\n",Sys.time(),f)#
			}
		} else {
			# non-readable files are not written into processed files list
			printf("%s - Error: %s not readable\n",Sys.time(),basename(f))
		}
	}

	sink()
}

# Main program
if(!interactive())
{
	# detect if another instance is running. In this case, wait for 5 minutes
	repeat
	{
		x=grep('R .*csv2influx',system2('ps','x',T,T),value=T)
		if(length(x)<=1)
		{
			break
		} else {
			Sys.sleep(60)
		}
	}
	
	args <- commandArgs(trailingOnly=T)
	if(length(args)<1)
	{
		printf("Usage: csv2influx.r <config file>\n")
		quit("no")
	}

	config <- args[1] # file to process

	update_database(config)
}
