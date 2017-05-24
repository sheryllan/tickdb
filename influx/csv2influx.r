#!/usr/bin/env Rscript

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
registerDoMC(parallel::detectCores())

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

# Convert one data frame to influx format
df2lineprotocol <- function(df,param,measurement)
{
	# parse option instrument strings
	if(param$type=='O') # options
	{
		x = str_split(df$product,fixed('.'),simplify=T)
		strike = str_replace(x[,5],fixed(','),'.')
		strike = paste0("strike=",strike)
		opt_type = x[,6] # C or P
		opt_type=paste0("cp=",opt_type)
		ofield = paste(strike,opt_type,sep=',')
	}
	else
		ofield=-1

	# header used for each line
	if(param$type=='O' | param$type=='F')
		header=sprintf('%s,type="%s",product="%s",expiry="%s" ',measurement, param$type, param$product,param$expiry)
		else header=sprintf('%s,type="%s",product="%s" ',measurement, param$type, param$product)

	# compute fields value
	hdr_trade=c('price','volume','side')
	data = alply(df,1,
			function(d)
			{
				if(d$otype!='S' & d$otype!='T')
				{
					idx=c(which(!is.na(d[4:33]))+3) # get fields to print
					paste( paste0(names(df)[idx],'=',d[idx],collapse=','),
						   sprintf('exch=%si,otype="%s"',d$exch,d$otype),sep=',')
				}
				else
				{
					idx=c(which(!is.na(d[4:6]))+3)
					paste( paste0(hdr_trade,'=',d[idx],collapse=','), 
							sprintf('exch=%si,otype="%s"',d$exch,d$otype),sep=',')
					
				}
			})

	data = paste(header,data,sep='')
	if(ofield!=-1)
		data = paste(data,ofield,sep=',')
	data = paste(data,df$recv)
	return(data)
}

# Parallel conversion of big data.frames to influx format
df2influx <- function(df,param,measurement)
{
	if(nrow(df)==0)
		return("")

	if(nrow(df)<1000)
		split=data.frame(i=1,j=nrow(df))
	else split = generate_df_split(nrow(df),gen='lo')

	# Parse data frame and convert to InfluxDB format
	foreach(d = iter(split,by='row'),.combine='c') %dopar%
	{
		df2lineprotocol(df[d$i:d$j, ],param, measurement)
	}
}

# Send a vector 'vec' of Influx line protocal to the server
# Split in smaller blocks to avoid server congestion
write2influx <- function(vec, max_size=200000)
{
	if(length(vec)>=max_size)
	{
		split = generate_df_split(length(vec),gen='by',size=max_size)
		for(i in 1:nrow(split))
		{
			response = httr::POST(url="", httr::timeout(60), scheme="http", hostname="127.0.0.1", port=8086,
				path="write", query=list(db="tickdb",u="",p=""), 
				body=paste0(vec[split$i[i]:split$j[i]],collapse='\n'))
		}
	}
	else {
			response = httr::POST(url="", httr::timeout(60), scheme="http", hostname="127.0.0.1", port=8086,
				path="write", query=list(db="tickdb",u="",p=""), 
				body=paste0(vec,collapse='\n'))
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
	con = influxdbr2::influx_connection(host="127.0.0.1",port=8086)
	response = create_database(con,"tickdb") 
	
	# find new files and process them
	newfiles = find_data_file(config)
	for(f in newfiles)
	{
		printf("Processing %s...",basename(f))
		param = decompose_filename(basename(f)) # get parameters from file name
		df = read_csv(f, col_types=csvformat, col_names=csvnames, skip=1) # read data

		# split by trades and quotes
		trades = df$otype=='S' | df$otype=='T'
		quotes = !trades

		dft = df[trades, ]
		dfq = df[quotes, ]

		# convert data.frame to line protocol
		influxt=df2influx(dft,param,"trade")
		influxq=df2influx(dfq,param,"book")

		# Populate InfluxDB
		write2influx(influxt)
		write2influx(influxq)
		printf("done\n")
	}

	# update list of processed files
	if(length(newfiles)>0)
	{
		cfg = read_json(config,simplifyVector=T)
		pdf = file(cfg$processed_data_files,'a')
		writeLines(newfiles,pdf)
		close(pdf)
	}
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
