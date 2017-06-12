#!/usr/bin/env Rscript

suppressMessages(library(Rcpp))
suppressMessages(library(readr))
suppressMessages(library(stringr))
suppressMessages(library(compiler))
suppressMessages(library(plyr))
suppressMessages(library(foreach))
suppressMessages(library(doMC))
suppressMessages(library(iterators))
suppressMessages(library(httr))
suppressMessages(library(influxdbr2))
suppressMessages(library(jsonlite))

j=enableJIT(3)
registerDoMC(6)
options(readr.show_progress=F)
sourceCpp("cpaste.cpp")

printf <- function(...) invisible(cat(sprintf(...)))
rien <- function(...) NULL

csvnames <- c('otype','recv','exch','bid1','bid2','bid3','bid4','bid5','bidv1','bidv2','bidv3','bidv4','bidv5','nbid1','nbid2','nbid3','nbid4','nbid5','ask1','ask2','ask3','ask4','ask5','askv1','askv2','askv3','askv4','askv5','nask1','nask2','nask3','nask4','nask5','product','state')

csvformat <- "cccnnnnniiiiiiiiiinnnnniiiiiiiiiicc"

# generate split indices of the data.frame (for // execution)
generate_df_split <- function(N,gen='by',size=parallel::detectCores())
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
	if(measurement=='trade') # filter out bad trades (bugs, etc...)
	{
		i=which(is.na(df$product)) # product with NA values
		df = df[-i , ]
	}

	if(nrow(df)==0)
		return(character(0))

	# parse option instrument strings
	if(param$type=='O') # options
	{
		x = str_split(df$product,fixed('.'),simplify=T)
		opt_tags = v2paste( str_c("strike=", str_replace(x[,5],fixed(','),'.')), # strike
							str_c('cp=',x[,6],''), # option types
							',') # generate tags for options
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
	body = foreach(i=1:ncol(book),.combine='cbind') %dopar% {str_c(names(book)[i],'=',book[,i])}
	# collapse each row into a single string like bid1=12,bid2=11, etc...
	split = generate_df_split(nrow(book),gen='length')
	body <- foreach(s=iter(split,by='row'),.combine='c') %dopar% { cpaste(body[s$i:s$j,,drop=F]) }
	body <- v3paste(otype,exch,body,',') # add the mandatory fields for each record

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

test_response <- function(r,file,log,measurement)
{
	if(r$status_code!=204)
	{
		printf("%s - %s %s %s %s %s\n",Sys.time(),file,r$status,http_error(r)$reason,http_error(r)$message,measurement)
	}
}

# Send a vector 'vec' of Influx line protocal to the server
# Split in smaller blocks to avoid server congestion
write2influx <- function(vec,file,log,measurement,DBNAME,max_size=100000)
{
	if(length(vec)>=max_size)
	{
		split = generate_df_split(length(vec),gen='by',size=max_size)
		for(i in 1:nrow(split))
		{
			response = httr::POST(url="", httr::timeout(60), scheme="http", hostname="127.0.0.1", port=8086,
				path="write", query=list(db=DBNAME,u="",p=""), 
				body=paste0(vec[split$i[i]:split$j[i]],collapse='\n'))
			test_response(response,file,log,measurement)
		}
	} else {
			response = httr::POST(url="", httr::timeout(60), scheme="http", hostname="127.0.0.1", port=8086,
				path="write", query=list(db=DBNAME,u="",p=""), 
				body=paste0(vec,collapse='\n'))
			test_response(response,file,log,measurement)
	}
}

# Find new data file in the raw capture
find_data_file <- function(config)
{
	cfg = read_json(config,simplifyVector=T)
	donefiles = tryCatch(read_csv(cfg$processed_data_files,col_types='c',col_names='file'),
						 error=function(e) return(data.frame(file=character(0))))
	donefiles = donefiles$file

	if(length(cfg$contracts>0))
	{
		p = paste0(paste0('-',cfg$contracts),collapse='|')
		files = dir(cfg$capturedir,
					pattern = p,
					full.names=T, recursive=T) # get all the data files
	}
	else files=dir(cfg$capturedir,pattern='*.csv.xz', full.names=T,recursive=T)

	# Filter out ref and stats files
	files = grep("Reference",files,value=T,invert=T)
	files = grep("Statistic",files,value=T,invert=T)

	# Make the diff with already processed files
	newfiles = setdiff(files,donefiles)

	return(newfiles)
}

# Update the Influx db with new files
update_database <- function(config)
{
	# First create a database
	cfg = read_json(config,simplifyVector=T)

	con = influxdbr2::influx_connection(host="127.0.0.1",port=8086)
	response = create_database(con,cfg$dbname) 
	pdf = file(cfg$processed_data_files,'a')
	sink(cfg$logfile,append=T)
	
	# find new files and process them
	newfiles = find_data_file(config) 
	N = length(newfiles)
	printf("%s - Processing %d files\n",Sys.time(),N)
	for(f in newfiles)
	{
		printf("%s - processing %s\n",Sys.time(),f)
		t0=Sys.time()
		param = decompose_filename(basename(f)) # get parameters from file name
		if(file.access(f,mode=0)==0 & file.access(f,mode=4)==0) # ignore non-readable files
		{
			df = suppressWarnings(as.data.frame(
					read_csv(f, col_types=csvformat, col_names=csvnames, skip=1,))) # read data

			if(nrow(df)>0)
			{
				# split by trades and quotes
				trades = df$otype=='S' | df$otype=='T'
				quotes = !trades

				dft = df[trades, ]
				dfq = df[quotes, ]

				# convert data.frame to line protocol
				if(nrow(dft)>0)
				{
					influxt=df2influx(dft,param,"trade")
					# Populate InfluxDB
					if(length(influxt)>0)
						write2influx(influxt,f,log,"trade",cfg$dbname)
				}
				if(nrow(dfq)>0)
				{
					influxq=df2influx(dfq,param,"book")
					# Populate InfluxDB
					if(length(influxq)>0)
						write2influx(influxq,f,log,"book",cfg$dbname)
				}

				msg<-sprintf("%s - %s : %d trades %d quotes in %s\n",
							 Sys.time(),basename(f), sum(trades), sum(quotes), format(Sys.time()-t0))
			} else {
				msg<-sprintf("%s - %s : empty file\n",Sys.time(),basename(f))
			}

			writeLines(f,pdf) 
			flush(pdf)
			printf("%s",msg)

		} else {
			# non-readable files are not written into processed files list
			printf("%s - %s to readable\n",Sys.time(),basename(f))
		}
	}

	close(pdf)
	sink()
}

# Main program
if(!interactive())
{
	args <- commandArgs(trailingOnly=T)
	if(length(args)<1)
	{
		printf("Usage: csv2influx.r <config file>\n")
		quit("no")
	}

	config <- args[1] # file to process

	update_database(config)
}
