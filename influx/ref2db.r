#!/usr/bin/env Rscript

suppressMessages(library(readr))
suppressMessages(library(stringr))
suppressMessages(library(compiler))
suppressMessages(library(plyr))
suppressMessages(library(foreach))
suppressMessages(library(doMC))
suppressMessages(library(jsonlite))
suppressMessages(library(dplyr))
suppressMessages(library(purrr))

j=enableJIT(3)
registerDoMC(24)

printf <- function(...) invisible(cat(sprintf(...)))
rien <- function(...) NULL

get_fname_date <- function(f)
{
	l <- rev(unlist(str_split(f,"[.-]"))) # decompose filename
	return(l[4]) # return date of capture
}

# Find new data file in the raw capture
find_ref_file <- function(config)
{
	cfg = read_json(config,simplifyVector=T)
	donefiles = tryCatch(read_csv(cfg$processed_ref_files,col_types='c',col_names='file'),
						 error=function(e) return(data.frame(file=character(0))))
	if(nrow(donefiles)==0)
		donefiles=character(0)
	else donefiles = donefiles$file

	# find all Reference files
	files = dir(cfg$capturedir,pattern="Reference",full.names=T,recursive=T)

	# Make the diff with already processed files
	newfiles = setdiff(files,donefiles)

	return(newfiles)
}

# Update the Influx db with new files
update_ref_database <- function(config)
{
	cfg = read_json(config,simplifyVector=T) # read config
	sink(cfg$logfile,append=T) # open log file

	newfiles = find_ref_file(config) # find new files on the disk
	printf("%s - Processing %d reference files\n",Sys.time(),length(newfiles))

	# Columns to use, the rest will be discarded
	nam = c("ProductID","Product", "Type", "Exchange", "Currency", "Underlying", "ExpiryDate", "Strike",
		"PutOrCall", "ExerciseStyle", "MinPriceIncrement", "MinPriceIncrementAmount", "SecurityDesc", "PremiumDecimalPlace", "ProductSymbol", "UnderlyingSymbol",
		"Partition", "PriceQuoteFactor", "SecurityID", "UnderlyingSecurityID", "MarketSegmentID", "MarketSegment", "ProductComplex", "SecurityGroup",
		"TickSizeDenominator", "DestinationExchange", "DeltaProtectionLevel", "ContractThrottle")

	# Generate the updated instrument database
	data = foreach(f= newfiles,.combine=rbind) %dopar%
	{
		# read ref file
		df=NULL
		if(!file.access(f,mode=4)) # If I can read the file
		{
			df = tryCatch( # load the data
					suppressMessages(suppressWarnings(read_csv(xzfile(f),progress=F))),
					error=function(e) NULL)

			if(!is.null(df) & nrow(df)>0)
			{
				for(n in nam) # check all columns we want exist
				{	 
					if(!(n %in% names(df)))
						df[n]=""
				}

				df = df[ , nam] # restrict to the set of columns only
				df$filedate = as.integer(get_fname_date(f)) # add date for sorting later
			} else 
			{
				if(nrow(df)==0)
					printf("%s - no data in %s\n",Sys.time(),f)
				else printf("%s - Error opening/reading %s\n",Sys.time(),f)
				df=NULL
			}
		}
		df # return the data.frame to foreach
	}

	# Merge with current database
	if(!file.access(cfg$instdb,mode=4))
	{
		olddata = read_csv(cfg$instdb,progress=F)
		data = rbind(data,olddata)
	}

	# Filter to eliminate redundant entries
	data = data %>% group_by(ProductID) %>% filter(filedate==max(filedate)) %>% arrange(ProductID) %>% distinct(ProductID,.keep_all=T)

	# Overwrite inst db with new instruments
	write_csv(data,cfg$instdb)

	# Update list of processed files
	write_csv(data.frame(file=newfiles),cfg$processed_ref_files,append=T)

	printf("%s - instrument db has %d records\n",Sys.time(), nrow(data))
	sink()
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
