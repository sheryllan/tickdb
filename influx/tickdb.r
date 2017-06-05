suppressMessages(library(foreach))
suppressMessages(library(readr))
suppressMessages(library(lubridate))
suppressMessages(library(stringr))
suppressMessages(library(dplyr))
suppressMessages(library(jsonlite))
suppressMessages(library(lubridate))
suppressMessages(library(bizdays))
suppressMessages(library(bit64))
suppressMessages(library(nanotime))
suppressMessages(library(iterators))

load_db <- function(config)
{
	cfg = read_json(config)
	suppressWarnings(suppressMessages(read_csv(cfg$instdb)))
}

# Convert our exchange name to QuantLib exchange name
exch2ql <- function(exchange)
{
	switch(exchange,
		   EUREX = "QuantLib/Germany/Eurex"
		   )
}

#' Convert our exchange name to its timezone
exch2tz <- function(exchange)
{
	switch(exchange,
		   EUREX = "Europe/Berlin"
		   )
}

#' generate sequence of contract and business date for a product, and the nanoseconds timestamps of required periods
#' @param product the product's name like FDAX or ODAX
#' @param type the product's type like F,O,E,S or C
#' @param front 1 for front month, 2 for back month, 3 for 3rd month, etc...
#' @param rolldays the number of days to roll the contract before its expiry date
#' @param from the start date of the period
#' @param to the end date of the period
#' @param periods a data.frame with 2 columns, from and to, of trading time period like "08:00" "16:30"
#' @param idb the instrument database or the json config file used to load the database
seq.contracts <- function(product,type,front,rolldays,from,to,periods,idb)
{
	if(is.character(idb)) # load instrument database
		idb = load_db(idb)

	s = seq(ymd(from), ymd(to), by='1 day') # sequence of days [from,to]
	regex = str_c("PROD\\.",type,"\\.",product)
	x=idb[grepl(regex,idb$ProductID) & idb$Type=="F",]
	x$expdate = ymd(x$ExpiryDate) - days(rolldays)

	# Get exchange name. It must be unique
	exch = unique(x$Exchange)
	if(length(exch)!=1)
		NULL

	# Load QuantLib calendars
	from = as.character(from)
	to = as.character(to)
	# change format from yyyymmdd to yyyy-mm-dd
	from.s = str_c(str_sub(from,1,4),str_sub(from,5,6),str_sub(from,7,8),sep='-')
	to.s   = str_c(str_sub(to,1,4),  str_sub(to,5,6),  str_sub(to,7,8),sep='-')
	if(!has.calendars(exch2ql(exch))) # load bizness calendars
	{
		load_quantlib_calendars(from=from.s,to=to.s)
	}
	bizseq = bizdays(from.s,to.s,exch2ql(exch)) # generate sequence of biz days for exchange

	# Generate the sequence of contract per business day
	contracts = foreach(d=s,.combine=rbind) %do%
	{
		if(is.bizday(d,exch2ql(exch)))
		{
			x$dist = x$expdate - d # time to expiry
			x = x%>% filter(dist>=0) # remove expired contracts
			x$rank = rank(x$dist) # get rank of distance to expiry
			x$date = d
			x %>% filter(rank==front) # select front contract
		} else NULL
	}

	# Generate for each day a data.frame of from/to nanoseconds timestamps
	timestamps = foreach(d=iter(contracts,by='row'),.combine=rbind) %do%
	{
		# for each period, convert to UTC nanosecond timestamps
		periods %>%
		rowwise() %>%
		mutate(date=d$date[1],
			  nanofrom=as.integer64(nanotime(with_tz(as.POSIXct(d$date[1]+hm(from),tz=exch2tz(exch))))),
			  nanoto  =as.integer64(nanotime(with_tz(as.POSIXct(d$date[1]+hm(to),  tz=exch2tz(exch)))))
			  )
	}

	list(contracts=contracts,timestamps=timestamps)
}
