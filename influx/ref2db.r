#!/usr/bin/env Rscript

suppressMessages(library(tickdb))
suppressMessages(library(readr))
suppressMessages(library(stringr))
suppressMessages(library(compiler))
suppressMessages(library(plyr))
suppressMessages(library(foreach))
suppressMessages(library(doMC))
suppressMessages(library(jsonlite))
suppressMessages(library(dplyr))
suppressMessages(library(purrr))
suppressMessages(library(Rcpp))
suppressMessages(library(bit64))

j=enableJIT(3)
registerDoMC(24)

printf <- function(...) invisible(cat(sprintf(...)))
rien <- function(...) NULL
sourceCpp("cpaste.cpp")

nam = c("ProductID","Product", "Type", "Exchange", "Currency", "Underlying",
		"ExpiryDate", "Strike", "PutOrCall", "ExerciseStyle", "MinPriceIncrement",
		"MinPriceIncrementAmount", "SecurityDesc", "PremiumDecimalPlace", "ProductSymbol", 
		"UnderlyingSymbol", "Partition", "PriceQuoteFactor", "SecurityID", 
		"UnderlyingSecurityID", "MarketSegmentID", "MarketSegment", "ProductComplex", 
		"SecurityGroup", "TickSizeDenominator", "DestinationExchange", 
		"DeltaProtectionLevel", "ContractThrottle")

get_fname_date <- function(f)
{
	l <- rev(unlist(str_split(f,"[.-]"))) # decompose filename
	return(l[4]) # return date of capture
}

generate_df_split <- function(N,gen='by',size=parallel::detectCores())
{
	if(gen=='by') i = seq(1,N,by=size)
	else i = round(seq(1,N,length.out=size))

	j = i-1
	j = c(j[2:length(j)],N)
	data.frame(i,j)
}

# Find new data file in the raw capture
find_ref_file <- function(cfg,con)
{
	if(!is.null(con))
	{
		response = httr::GET(url="",scheme=con$scheme, hostname=con$host, port=con$port,
				  path="query",
				  query=list(db=cfg$dbname,u=con$user,p=con$pass,
							 q="select * from ref_files"),
				  add_headers(Accept="application/csv"))
		donefiles = character(0)
		if(status_code(response)==200 & length(response$content)>0)
		{
			text = rawToChar(response$content)
			data = readr::read_csv(text,col_types='cccc')$file
			donefiles = data
		}
	}

	# find all Reference files
	files = dir(cfg$capturedir,pattern="Reference",full.names=T,recursive=T)

	# Make the diff with already processed files
	newfiles = setdiff(files,donefiles)

	list(newfiles=newfiles,donefiles=donefiles)
}

refit_df <- function(df)
{
	if(!is.null(df))
	{
		for(n in nam) # check all required columns are there
		{	 
			if(!(n %in% names(df)))
				df[n]=NA # add missing column
		}

		df = df[,nam] # re-order columns

		df$Exchange = as.character(df$Exchange)
		df$ExpiryDate = as.integer(df$ExpiryDate)
		df$MinPriceIncrement = as.numeric(df$MinPriceIncrement)
		df$MinPriceIncrementAmount = as.numeric(df$MinPriceIncrementAmount)
		df$PremiumDecimalPlace=as.numeric(df$PremiumDecimalPlace)
		df$Partition = as.numeric(df$Partition)
		df$PriceQuoteFactor = as.numeric(df$PriceQuoteFactor)
		df$TickSizeDenominator = as.numeric(df$TickSizeDenominator)
	}

	return(df)
}

read_old_data <- function(cfg,con)
{
	if(!is.null(con))
	{
		response = httr::GET(url="",scheme=con$scheme, hostname=con$host, port=con$port,
			path="query", query=list(db=cfg$dbname,u=con$user,p=con$pass,
			q="select * from refdata"),add_headers(Accept="application/csv"))

		old_data = NULL
		if(status_code(response)==200 & length(response$content)>0)
		{
			old_data = read.csv(text=rawToChar(response$content),colClasses="character")
			old_data = refit_df(old_data)
		}
	}

	return(old_data)
}

writefiles<-function(cfg,con,files)
{
	x = str_c("ref_files file=\"",files,"\"")
	x = paste(x,1:length(x)) # add a 1 to N timestamp to force Influx to overwrite old data
	max_size = 50000
	if(length(x)>max_size)
	{
		split = generate_df_split(length(x),gen='by',size=max_size)
	   for(i in 1:nrow(split))
	   {
		   response=httr::POST(url="",httr::timeout(60),scheme="http",
					hostname=cfg$host,port=8086,path="write",
					query=list(db=cfg$dbname,u='',p=''),
					body=paste0(x[split$i[i]:split$j[i]],collapse='\n'))
	   }
	} else {
		   response = httr::POST(url="",httr::timeout(60),scheme="http",
					hostname=cfg$host,port=8086,path="write",
					query=list(db=cfg$dbname,u='',p=''),
					body=paste(x,collapse='\n'))
	}

	if(response$status_code!=204)
		printf("%s - Error writing list of processed reference files\n",Sys.time())
}

writeinst <- function(cfg,con,data)
{
	data["filedate"]=NULL
	types = sapply(data,class)
	y = matrix("",nrow(data),ncol(data))

	for(i in 1:length(types))
	{
		if(types[i]=='character')
		{
			y[,i] = str_c(names(data)[i],'=\"',data[,i],'\"')
		} else if(types[i]=="integer")
		{
			y[,i] = str_c(names(data)[i],'=',data[,i],'i')
		} else if(types[i]=="numeric")
		{
			y[,i] = str_c(names(data)[i],'=',data[,i])
		}
	}

	z = cpaste(y) # convert to a list of influx protocol lines
	header=rep("refdata",length(z))
	z = v2paste(header,z,' ')
	ts = format(1:length(z))
	z = v2paste(z,ts,' ')

	max_size=5000
	if(length(z)>max_size)
	{
		split = generate_df_split(length(z),gen='by',size=max_size)
		for(i in 1:nrow(split))
		{
			response = httr::POST(url="",httr::timeout(60),scheme="http",
						hostname=cfg$host,port=8086,path="write",
						query=list(db=cfg$dbname,u='',p=''),
						body=paste0(z[split$i[i]:split$j[i]],collapse='\n'))
		} 
	} else {
		response = httr::POST(url="",httr::timeout(60),scheme="http",
					hostname=cfg$host,port=8086,path="write",
					query=list(db=cfg$dbname,u='',p=''),
					body=paste(z,collapse='\n'))
		}

	if(response$status_code!=204)
		printf("%s - Error writing reference data\n",Sys.time())
}

load_new_ref_data <- function(cfg,con,newfiles)
{
	# Columns to use, the rest will be discarded

	# Generate the updated instrument database
	data = foreach(f=newfiles,.combine=rbind) %dopar%
	{
		# read ref file
		df=NULL
		if(!file.access(f,mode=4)) # If I can read the file
		{
			line=readLines(xzfile(f),1) # read header
			len=length(unlist(str_split(line,' *, *'))) # nb of fields in header
			ct = paste0(rep('c',len),collapse='') # make a string a 'c' for read_csv
			df = suppressWarnings(read_csv(xzfile(f),progress=F,col_types=ct)) # read the file

			if(!is.null(df) & nrow(df)>0)
			{
				df = refit_df(df)
				df$filedate = as.integer(get_fname_date(f)) # add date for sorting later
			} else {
				if(nrow(df)==0)
					printf("%s - no data in %s\n",Sys.time(),f)
				else printf("%s - Error opening/reading %s\n",Sys.time(),f)
				df=NULL
			}
		}
		df # return the data.frame to foreach
	}

	if(!is.null(data))
	{
		printf("%s - Found %d instruments in the new files\n",Sys.time(),nrow(data))
	} else {
		printf("%s - Found 0 instrument in the new files\n",Sys.time())
	}

	# Merge with current database
	old_data = read_old_data(cfg,con)
	if(!is.null(old_data))
	{
		old_data$filedate = 0
		printf("%s - Found %d instruments in the Influx DB\n",Sys.time(),nrow(old_data))
	} else {
		printf("%s - Found 0 instrument in the Influx DB\n",Sys.time())
	}

	data = rbind(old_data,data)

	if(!is.null(data))
	{
		data=data%>%group_by(ProductID) %>% filter(filedate==max(filedate)) %>% arrange(ProductID) %>% distinct(ProductID,.keep_all=T)
		data=as.data.frame(data)
	}

	return(data)
}

# Update the Influx db with new files
update_ref_database <- function(config)
{
	cfg = read_json(config,simplifyVector=T) # read config
	con = influx.connection(cfg$host) # open Influx connection
	sink(cfg$logfile,append=T) # open log file

	# load list of new and old reference files
	frf = find_ref_file(cfg,con) # find new files on the disk
	printf("%s - Processing %d reference files\n",Sys.time(),length(frf$newfiles))

	# load new reference data
	data = load_new_ref_data(cfg,con,frf$newfiles)

	# Filter to eliminate redundant entries
	if(!is.null(data))
	{
		printf("%s - Inserting %d instruments\n",Sys.time(),nrow(data))
		writefiles(cfg,con, c(frf$donefiles,frf$newfiles))
		writeinst(cfg,con,data)

		printf("%s - instrument db has %d records\n",Sys.time(), nrow(data))
		sink()
	} else {
		printf("%s - error: no reference data available\n",Sys.time())
	}
}

# Main program
if(!interactive())
{
	args <- commandArgs(trailingOnly=T)
	if(length(args)<1)
	{
		printf("Usage: ref2db <config file>\n")
		quit("no")
	}

	config <- args[1] # file to process

	update_ref_database(config)
}
