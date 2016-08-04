#!/usr/bin/env Rscript

suppressMessages(library(plyr))
suppressMessages(library(data.table))
suppressMessages(library(readr))
suppressMessages(library(jsonlite))
suppressMessages(library(bit64))
suppressMessages(library(foreach))
suppressMessages(library(doMC))
suppressMessages(library(stringr))
suppressMessages(library(RPostgreSQL))

registerDoMC(cores=16)


# include daily_check source file
source("daily_check_and_stats.r")

#' Load a json config file
#' @param config_file json config file name
#' @return a json object
#' @seealso
#' @examples
#' config <- load_json_config("myfile.json")
load_json_config <- function(config_file)
{
	if(file.exists(config_file))
	{
		file <- file(config_file,'rb')
		if(isOpen(file))
		{
			config <- fromJSON(file)
			close(file)
			return(config)
		}
	}

	return(NULL)
}

find_new_files <- function(config,provider)
{
	dbfilename <- config[[provider]]$daily_db

	pf <- list.files(config[[provider]]$dbdir,recursive=T,full.names=T)
	if(file.exists(dbfilename))
		df <- read_csv(dbfilename)$files
	else df<-character(0)

	new_files <- pf[ !(pf %in% unique(df)) ]

	return(new_files)
}

get_session_data <- function(config,provider)
{
	# Open connection to PostgreSQL productd
	drv <- dbDriver("PostgreSQL")
	# connect to several DBs at the same time
	con <- c(
		"lon"= dbConnect(drv, dbname ="productd", host="192.168.12.190", user="rsprod"), 
		"hk" = dbConnect(drv, dbname ="productd", host="192.168.107.20", user="rsprod"),
		"syd"= dbConnect(drv, dbname ="productd", host="172.16.9.210",   user="rsprod"))

	# Retrieve trading session data
	if(provider=="liquid_capture")
	{
		query = paste("select rsdbproductfamily.productid,rsdbproductfamily.tradingsession,",
					  "rsdbtradingsession.timezonename,rsdbtradingsession.startweekday,",
					  "rsdbtradingsession.endweekday,  rsdbtradingsession.start,",
					  "rsdbtradingsession.end,rsdbtradingsession.status",
			   		  "from rsdbproductfamily inner join rsdbtradingsession",
				      "on rsdbproductfamily.tradingsession=rsdbtradingsession.name")
		x = ldply(con, function(c) dbGetQuery(c,query))
	} else if(provider=="qtg")
	{
	}

	return(x)
}

get_trading_session <- function(provider,filename,session)
{
	# Select product information
	if(provider=="liquid_capture")
	{
		l <- unlist(str_split(basename(filename),"[-.]")) # decompose file name
		date <- l[4]
		x <- session[grep(paste0("PROD\\.",l[2],"\\.",l[1],"$"), session$productid), ]
	}
	else if(provider=='qtg')
	{
	}

	if(nrow(x)==0)
		return(NULL)
	if(nrow(x)>1)
		x = x[1,] # HAAAAAAAACK !
	
	# extract information from the database
	w = wday(as.Date(date,"%Y%m%d")) # get week day
	startweekday=unlist(str_split(x$startweekday,"[{},]"))
	endweekday=unlist(str_split(x$endweekday,"[{},]"))
	start=unlist(str_split(x$start,"[{},]"))
	end=unlist(str_split(x$end,"[{},]"))
	status=unlist(str_split(x$status,"[{},]"))
	# status==2 means open session
	startweekday=startweekday[status==2]
	endweekday=endweekday[status==2]
	start=as.integer(start[status==2])
	end=as.integer(end[status==2])

	# Convert string date to Date in its own timezone
	# and add seconds for the start of the session
	start=as.POSIXct(paste(date," 00:00:00"),format="%Y%m%d",tz=x$timezonename)+start[startweekday==w] 
	start=as.integer(start,tz="UTC") # and convert to seconds from epoch
	start=as.integer64(start)*1e9 # and then to nanosecond integer64 format

	end=as.POSIXct(paste(date," 00:00:00"),format="%Y%m%d",tz=x$timezonename)+end[endweekday==w]
	end=as.integer(end,tz="UTC") # and convert to seconds from epoch
	end=as.integer64(end)*1e9 # and then to nanosecond integer64 format

	c(start=start,end=end)
}

get_product_name <- function(filename,provider)
{
	if(provider=="liquid_capture")
	{
		unlist(str_split(filename, '-'))[1]
	}
	else if(provider=="qtg")
	{
		return(NULL) # TODO
	}
	else return(NULL)
}

process_daily <- function(config,provider)
{
	# find new files
	new_files <- find_new_files(config,provider)
	if(length(new_files)==0)
		return(data.frame())
	trading_time <- get_session_data(config,provider)

	# process each file in parallel
	printf("processing %d files\n",length(new_files))
	result <- foreach(file=new_files,.combine=rbind,.errorhandling="remove") %dopar%
	{
		session <- get_trading_session(provider,file,trading_time) # get session time
		if(!is.null(session))
		{
			check = list()

			# Read CSV header
			cols <- names(fread(paste("bzip2 -cd",file,"|head -1"),verbose=F,showProgress=F,data.table=F))
			pr <- grep("bid[^v]|ask[^v]|strike",cols,value=T) # get price columns and number of orders
			# Read one day of data, forcing all prices nb orders to be double
			# nb orders are in double because for many exchange we only have NAs
			df<-fread(paste("bzip2 -cd",file),verbose=F,showProgress=F,data.table=F,colClasses=list(numeric=pr))
			nb_NA_timestamps = nrow(df)
			# remove line with NA timestamps
			df <- df[!is.na(df$recv) & !is.na(df$exch) & !rowAnys(df==999999999998,na.rm=T),]
			nb_NA_timestamps = nb_NA_timestamps - nrow(df)
			df <- df[ df$recv>=session["start"] & df$recv<=session["end"] , ] # restrict to trading session
			if(nrow(df)>0)
			{
				dfq <- df[ df$otype!='T' & df$otype!='S', ] # extract quotes only
				dft <- df[ df$otype=='T' | df$otype=='S', ] # extract trades only

				check[[length(check)+1]] <- c( has_data=nrow(df)>0)
				check[[length(check)+1]] <- c( has_monotonic_timestamps=has_monotonic_timestamps(df))
				check[[length(check)+1]] <- c( has_no_NA_in_timestamps=nb_NA_timestamps == 0)
				check[[length(check)+1]] <- c( has_only_non_zero_timestamps=has_only_non_zero_timestamps(df))

				check[[length(check)+1]] <- c( has_no_crossed_books=has_no_crossed_books(dfq))
				check[[length(check)+1]] <- c( has_no_zero_or_neg_qty=has_no_zero_or_neg_qty(dfq))
				check[[length(check)+1]] <- c( has_consistent_books_prices=has_consistent_books_prices(dfq))
				check[[length(check)+1]] <- c( has_valid_trades=has_valid_trades(dft))
				check[[length(check)+1]] <- c( is_day_valid = all(unlist(check)))

				stats <- test_and_stats_on_trades(df)

				printf("%s done\n",file)
				return(data.frame(t(unlist(c(check,stats))), files=file))
			}
			else printf("%s no data\n",file)
		}
		else printf("%s failed\n",file)
	}

	printf("result has %d entries\n",nrow(result))

	return(result)
}

write.daily_db <- function(config,provider,daily)
{
	if(nrow(daily)>0)
	{
		if(file.exists(config[[provider]]$daily_db))
			daily <- rbind.fill(read.csv(config[[provider]]$daily_db), daily)

		write.csv(daily , file=config[[provider]]$daily_db, row.names=F)
	}
}

if(!interactive())
{
	args <- commandArgs(trailingOnly=T)
	if(length(args) == 2)
	{
		cfg_file = args[1]
		provider = args[2]

		config <- load_json_config(cfg_file)
		result <- process_daily(config, provider)
		write.daily_db(config,provider,result)
		quit(status=0)
	}
	else {print("Usage: cmd cfg_file provider"); quit(status=1)}
}
