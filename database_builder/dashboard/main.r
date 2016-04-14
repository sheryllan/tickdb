library(jsonlite)
library(bit64)
library(data.table)
library(foreach)
library(doMC)
library(stringr)

registerDoMC(cores=16)

# include daily_check source file
if(!exists('has_nonmonotonic_timestamps', mode='function'))
	source("daily_check_and_stats.r")

load_json_config <- function(config_file)
{
	if(file.exists(config_file) & isOpen(file <- file(config_file,'rb')))
	{
		config <- fromJSON(file)
		return(config)
	}
	else return(NULL)
}

find_new_files <- function(config,provider)
{
	procfilename<-config[[provider]]$dbprocessed
	dbfilename <- config[[provider]]$daily_db

	pf <- read.csv(procfilename,header=F)
	df <- read.csv(dbfilename)

	new_files <- pf[ !(pf %in% unique(df$file)) ]

	return(new_files)
}

get_trading_session <- function(config,provider,product,ts)
{
	# TODO: do something to access product db and extract data points
	# indices which correspond to the trading session

	return(1:length(ts))
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
	# read inst db
	instdb <- read.csv(config[[provider]]$instdb)

	# find new files
	new_files <- find_new_files(config,provider)

	# process each file in parallel
	result <- foreach(file=new_files) %dopar%
	{
		check = list()

		df <- fread(file, verbose=F, showProgress=F, data.table=F) # read one day of data
		pname <- get_product_name(file,provider) # get its product name
		tsi <- get_trading_session(config,provider,pname,df$recv) # get only trading session
		df <- df[tsi,] # reduce the data set to the trading session

		dfq <- df[ df$otype!='T' | df$otype!='S', ] # extract quotes only
		dft <- df[ df$otype=='T' | df$otype=='S', ] # extract trades only

		check <- list(check, list( has_data=has_data(df)))
		check <- list(check, list( has_monotonic_timestamps=has_monotonic_timestamps(df)))
		check <- list(check, list( has_only_non_zero_timestamps=has_only_non_zero_timestamps(df)))

		check <- list(check, list( has_no_crossed_books=has_no_crossed_books(dfq)))
		check <- list(check, list( has_no_zero_or_neg_qty=has_no_zero_or_neg_qty(dfq)))
		check <- list(check, list( are_all_books_price_consistent=are_all_books_prices_consistent(dfq)))
		check <- list(check, list( have_valid_trades=have_valid_trades(dft)))

		stats <- test_and_stats_on_trades(df,dfq,dft)
	}

	return(result)
}
