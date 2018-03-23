decode.header <- function(h)
{
	types = rep('c',length(h)) # by default all types are character
	for(i in 1:length(h)) # then adjust each type
	{
		if(stringr::str_detect(h[i],"^price|^bid[0-9]|^ask[0-9]"))
			types[i]='d'
		if(stringr::str_detect(h[i],"^volume|^size|^bidv|^askv|^nbid|^nask"))
			types[i]='i'
	}
	stringr::str_c(types,collapse='')
}

# convert a response to a data frame with market data
convert.influx <- function(response,sample,stype)
{
	foreach(r=response) %dopar%
	{
		if(r$status_code==200 & length(r$content)>0)
		{
			text = rawToChar(r$content) # convert to text
			header = decode.header(unlist(stringr::str_split(readLines(textConnection(text),1),',')))
			if(!sample) # tick data
			{
				data = readr::read_csv(text,col_types=header)
				# convert or remove some columns
				idx = which(stringr::str_detect(names(data),"name")) # remove column "name"
				if(length(idx)>0)
					data = data[ , -idx]
				idx = which(stringr::str_detect(names(data),"time")) # convert "time" to recv bit64
				if(length(idx)>0)
				{
					data$time = as.integer64(data$time)
					names(data)[idx]='recv'
					data$date = as.POSIXct(nanotime::nanotime(data$recv))
				}
				idx = which(stringr::str_detect(names(data),"exch")) # convert "exch" to bit64
				if(length(idx)>0)
					data$exch = as.integer64(data$exch)

			} else # sample data
			{
				if(stype=='book')
				{
					data = readr::read_csv(text,col_types='ccccddddiiiiddddiiii',
					col_names=c('name','tags','time','close_exch','open_bid','max_bid','min_bid','close_bid',
								'open_bidv','max_bidv','min_bidv','close_bidv',
								'open_ask','max_ask','min_ask','close_ask',
								'open_askv','max_askv','min_askv','close_askv'),skip=1)
				} else if(stype=='trade')
				{
					data = readr::read_csv(text,col_types='ccccddddiiiii',
					col_names=c('name','tags','time','close_exch','open_price','max_price',
								'min_price','close_price','open_volume','max_volume',
								'min_volume','close_volume','sum_volume'),skip=1)
				}

				idx = which(stringr::str_detect(names(data),"name")) # remove column "name"
				if(length(idx)>0)
					data = data[ , -idx]
				idx = which(stringr::str_detect(names(data),"tags")) # remove column "tags"
				if(length(idx)>0)
					data = data[ , -idx]
				idx = which(stringr::str_detect(names(data),"time")) # convert "time" to bit64
				if(length(idx)>0)
					data$time = as.integer64(data$time)
				idx = which(stringr::str_detect(names(data),"close_exch")) # convert "exch" to bit64
				if(length(idx)>0)
					data$close_exch = as.integer64(data$close_exch)
			}

			data
		} else { NULL } # remove failed responses
	}
}

# Convert our exchange name to QuantLib exchange name
exch2ql <- function(exchange)
{
	switch(exchange,
		   EUREX = "QuantLib/Germany/Eurex",
		   XEUR = "QuantLib/Germany/Eurex"
		   )
}

# Convert our exchange name to its timezone
exch2tz <- function(exchange)
{
	switch(exchange,
		   EUREX = "Europe/Berlin",
		   XEUR = "Europe/Berlin"
		   )
}

create.query <- function(sc,fields,measurement,group='')
{
	a = do.call(rbind,stringr::str_split(sc$contracts$ProductID,stringr::fixed('.')))
	a = as.tibble(a)
		
		a %>% 
		transmute(contracts=sc$contracts$ProductID,from=sc$timestamps$nanofrom,
				  to=sc$timestamps$nanoto, type=V2,product=V3,expiry=V4) %>%
		rowwise() %>%
		(function(row)
			stringr::str_c(
				"select ",fields," from ",measurement," where ", "product='",row$product,
				"' and expiry='",row$expiry,"' and type='",row$type,"' and ",
				stringr::str_c("time>=",row$from, " and time<=",row$to),
				ifelse(stringr::str_length(group)>0, stringr::str_c(" group by ",group), "")))
}

# Load the instrument database
load_idb <- function(db,con,product,type)
{
	# Get data from Influx
	query = paste0('select * from refdata where ProductID=~ /',
				   stringr::str_c("PROD.",type,".",product),
				   '/')

	response = httr::GET(url = "", scheme = con$scheme, hostname = con$host, port = con$port, 
						 path = "query",
						 query = list(db=db, u=con$user, p=con$pass, q=query),
						 httr::add_headers(Accept="application/csv"))
	if(response$status_code==200)
	{
		text = rawToChar(response$content)
		options(readr.show_progress=F)
		data = suppressWarnings(readr::read_csv(text,progress=F))
		return(data)
	} else {
		return(NULL)
	}
}

# generate sequence of contract and business date for a product, and the nanoseconds timestamps of required periods
seq.contracts <- function(db,con,product,type,front,rolldays,from,to,periods)
{
	# Load the database of all instruments
	regex = stringr::str_c("PROD\\.",type,"\\.",product)
	idb = load_idb(db,con,product,type)
	# Reduce to only the product name and type we're interested in
	x=idb[grepl(regex,idb$ProductID) & idb$Type==type, ]
	x$expdate = lubridate::ymd(x$ExpiryDate) - lubridate::days(rolldays) ##########

	# Get exchange name. It must be unique
	if("Exchange" %in% names(x))
		exch = unique(x$Exchange)
	else exch = unique(x$exchange)

	if(length(exch)!=1)
		NULL

	# Load QuantLib calendars
	from = as.character(from)
	to = as.character(to)
	# change format from yyyymmdd to yyyy-mm-dd
	from.s = stringr::str_c(stringr::str_sub(from,1,4),stringr::str_sub(from,5,6),stringr::str_sub(from,7,8),sep='-')
	to.s   = stringr::str_c(stringr::str_sub(to,1,4),  stringr::str_sub(to,5,6),  stringr::str_sub(to,7,8),sep='-')
	suppressMessages(bizdays::load_quantlib_calendars(from=from.s,to=to.s))

	# Generate the sequence of contract per business day
	contracts = list()
	for(d in seq(lubridate::ymd(from), lubridate::ymd(to), by='1 day')) # sequence of days [from,to]
	{
		y=x # make a copy of x
		if(bizdays::is.bizday(d,exch2ql(exch)))
		{
			y$dist = y$expdate - d # time to expiry
			y = y[y$dist>=0, ] # Rule to select the appropriate contract ##########
			y=do.call(rbind,lapply( # collapsing down to only one contract per product
							lapply(unique(y$ExpiryDate),
								   function(e) y[y$ExpiryDate==e,]), function(x) x[1,]))
			#######

			y$rank = rank(y$dist) # get rank of distance to expiry
			y$date = d
			contracts[[length(contracts)+1]] = y[y$rank==front,] # select front contract
		}
	}
	contracts=do.call(rbind,contracts)
	contracts=contracts[, c('name','time','ExpiryDate','Product','ProductID','expdate','dist','rank','date')]

	# Generate for each day a data.frame of from/to nanoseconds timestamps
	timestamps = foreach(d=iterators::iter(contracts,by='row'),.combine=rbind) %do%
	{
		# for each period, convert to UTC nanosecond timestamps
		periods %>%
		rowwise() %>%
		mutate(date=d$date[1],
			  nanofrom=as.integer64(nanotime::nanotime(lubridate::with_tz(as.POSIXct(d$date[1]+lubridate::hm(from),tz=exch2tz(exch))))),
			  nanoto  =as.integer64(nanotime::nanotime(lubridate::with_tz(as.POSIXct(d$date[1]+lubridate::hm(to),  tz=exch2tz(exch)))))
			  )
	}

	list(contracts=contracts,timestamps=timestamps, tz=exch2tz(exch))
}

# Create a connection object to an InfluxDB
influx.connection <- function(host='127.0.0.1', port=8086, scheme="http", user="", pass="")
{
  # create list of server connection details
  influxdb_srv <- list(host = host, port = port, scheme = scheme, user = user, pass = pass)

  # submit test ping
  response <- httr::GET(url = "", scheme = influxdb_srv$scheme,
                        hostname = influxdb_srv$host,
                        port = influxdb_srv$port,
                        path = "ping", httr::timeout(5))

  # Check for communication errors
  if(response$status_code != 204)
  {
    if(length(response$content) > 0)
		warning(rawToChar(response$content))

    stop("Influx connection failed with HTTP status code ", response$status_code)
  }

  # print server response
  message(httr::http_status(response)$message)
  return(influxdb_srv)
}

#' Generate a query from specifications
make.query <- function(db,con,measurement,product,type,from,to,periods,
					   front,rolldays,fields,extended_result=F)
{
	# generate field's names
	if(is.null(fields))
	{
		if(measurement=='l1book')
		{
			fields = "bid1,bidv1,ask1,askv1"
			measurement = 'book'
		} else if(measurement=='l2book')
		{
			fields = 'bid1,bid2,bid3,bid4,bid5,bidv1,bidv2,bidv3,bidv4,bidv5,ask1,ask2,ask3,ask4,ask5,askv1,askv2,askv3,askv4,askv5'
			measurement = 'book'
		} else if(measurement=='l2.5book')
		{
			fields = 'bid1,bid2,bid3,bid4,bid5,bidv1,bidv2,bidv3,bidv4,bidv5,nbid1,nbid2,nbid3,nbid4,nbid5,ask1,ask2,ask3,ask4,ask5,askv1,askv2,askv3,askv4,askv5,nask1,nask2,nask3,nask4,nask5'
			measurement = 'book'
		} else if(measurement=='trade')
		{
			fields = 'price,volume,side'
		}

		# add option fields
		if(type=='O')
			fields = stringr::str_c('otype,exch',fields,'strike,cp',sep=',')
		else
			fields = stringr::str_c('otype,exch',fields,sep=',')
	}

	# generate queries
	if(!extended_result)
	{
		return(
			   create.query(
					seq.contracts(db,con,product,type,front,rolldays,from,to,periods),
					fields,measurement)
			)
	} else {
		sequence = seq.contracts(db,con,product,type,front,rolldays,from,to,periods)
		query = create.query(sequence,fields,measurement)
		return(list(sequence=sequence$contracts,query=query,tz=sequence$tz))
	}
}

#' Run a TickDB query
#' @param Influx DB connection (use influx.connection)
#' @param an InfluxDB query for the Tick database
#' @return a data.frame
run.query <- function(query,db,sample=F,stype='',con)
{
	# submit queries and convert to data.frame
	convert.influx(
		foreach(q = query) %do%
		{
			httr::GET(url = "", scheme = con$scheme, hostname = con$host, port = con$port, path = "query",
					  query = list(db=db, u=con$user, p=con$pass, q=q), httr::add_headers(Accept="application/csv"))
		}, sample,stype)
}

###########
# Interface
###########

#' Make a 'periods' data.frame like 08:00,17:00 with 2 integers only
#' @param from start hour
#' @param to end hour
#' @export period
period <- function(from,to)
{
	f1 = from[1]
	f2 = if(length(from)==2) from[2] else 0
	t1 = to[1]
	t2 = if(length(to)==2) to[2] else 0

	data.frame(from=sprintf("%02d:%02d",f1,f2),to=sprintf("%02d:%02d",t1,t2))
}

#' Sample price series
#' @param measurement l1book,l2book,l2.5book,trade
#' @param product Product name
#' @param type Product type
#' @param from start date
#' @param to end date
#' @param periods trading hours
#' @param frequency sampling frequency
#' @param front 1 for front month, 2 for back month, etc...
#' @param rolldays days to roll the contract before expiry
#' @param config json config file
#' @export tick_data
tick_data <- function(measurement,product,type,from,to,periods,
					 front=1,rolldays=5,fields=NULL,
					 config="~/recherche/tickdatabase/influx/config.json",
					 verbose=F)
{
	# Initialize Influx connection
	cfg = jsonlite::read_json(config)
	con = influx.connection(cfg$host)
	if(verbose) message("Connection to database established")

	# Create a query
	db = cfg$dbname
	query = make.query(db,con,measurement,product,type,from,to,periods,front,rolldays,fields)
	if(verbose) message(sprintf("Generated %d individual queries",length(query)))

	# Run query
	data = run.query(query,db,con=con)
	if(verbose) message(sprintf("Received %d results from %d queries",length(data),length(query)))

	class(data)=c("tickdata",class(data))
	return(data)
}

#' Print a tickdata object
#' @param x an object of class tickdata, usually, a result of a call to 'tick_data'
#' @export print.tickdata
print.tickdata <- function(x,...)
{
}

#' Produce result summaries on tick data
#' @param x an object of class tickdata, usually, a result of a call to 'tick_data'
#' @export summary.tickdata
summary.tickdata <- function(x,...)
{
}

#' Plot a tickdata object
#' @param x an object of class tickdata, usually, a result of a call to 'tick_data'
#' @export plot.tickdata
plot.tickdata <- function(x,...)
{
}

#' Run a simple query with a group by argument
#' @param measurement book,trade
#' @param product Product name
#' @param type Product type
#' @param from UTC nanosecond
#' @param to UTC nanosecond
#' @param group influx group by clause
#' @param config json config file
#' @export raw_sample
raw_sample <- function(measurement,product,type,expiry,from,to,group,config)
{
	if(measurement=='book')
	{
		fields="last(exch),first(bid1),max(bid1),min(bid1),last(bid1),first(bidv1),max(bidv1),min(bidv1),last(bidv1),first(ask1),max(ask1),min(ask1),last(ask1),first(askv1),max(askv1),min(askv1),last(askv1)"
		measurement = 'book'
	} else if(measurement=='trade')
	{
		fields="last(exch),first(price),max(price),min(price),last(price),first(volume),max(volume),min(volume),last(volume),sum(volume)"
	}

	# Initialize Influx connection
	cfg = jsonlite::read_json(config)
	con = influx.connection(cfg$host)

	# Create a query
	query = sprintf("select %s from %s where product='%s' and expiry='%s' and type='%s' and time>=\'%s\' and time<=\'%s\' group by time(%s)",
					fields,measurement,product,expiry,type,from,to,group)

	# run the sampling query
	data = run.query(query,cfg$dbname,sample=T,stype=measurement,con=con)

	return(data)
}

#' Sample price series
#' @param measurement book,trade
#' @param product Product name
#' @param type Product type
#' @param from start date
#' @param to end date
#' @param periods trading hours
#' @param frequency sampling frequency
#' @param front 1 for front month, 2 for back month, etc...
#' @param rolldays days to roll the contract before expiry
#' @param config json config file
#' @export sample_price
sample_price <- function(measurement,product,type,from,to,periods,
						 frequency='1s',front=1,rolldays=5,
						 config="~/recherche/tickdatabase/influx/config.json")
{
	if(measurement=='book')
	{
		fields="last(exch),first(bid2),max(bid1),min(bid1),last(bid1),first(bidv1),max(bidv1),min(bidv1),last(bidv1),first(ask1),max(ask1),min(ask1),last(ask1),first(askv1),max(askv1),min(askv1),last(askv1)"
		measurement = 'book'
	} else if(measurement=='trade')
	{
		fields="last(exch),first(price),max(price),min(price),last(price),first(volume),max(volume),min(volume),last(volume),sum(volume)"
	}

	# Initialize Influx connection
	cfg = jsonlite::read_json(config)
	con = influx.connection(cfg$host)

	# Create a query
	db = cfg$dbname
	equery = make.query(db,con,measurement,product,type,from,to,periods,front,rolldays,fields,
					   extended_result=T)
	query = equery$query
	sequence = equery$sequence
	query = foreach(q=query,.combine='c') %do% paste(q," group by time(",frequency,")")

	# run the sampling query
	data = run.query(query,db,sample=T,stype=measurement,con=con)
	
	# Add extra information to sampled data
	for(i in 1:length(data))
	{
		if(inherits(data[[i]],"data.frame"))
		{
			data[[i]]$contract=sequence$ProductID[i]
			data[[i]]$ExpiryDate=sequence$ExpiryDate[i]
			data[[i]]$date = sequence$date[i]
			data[[i]]$hour = lubridate::hour  (lubridate::with_tz(as.POSIXct(nanotime::nanotime(data[[i]]$time)),tz=equery$tz))
			data[[i]]$min  = lubridate::minute(lubridate::with_tz(as.POSIXct(nanotime::nanotime(data[[i]]$time)),tz=equery$tz))
			data[[i]]$sec  = lubridate::second(lubridate::with_tz(as.POSIXct(nanotime::nanotime(data[[i]]$time)),tz=equery$tz))
		}
	}

	class(data) = c("sampleprice",class(data))
	return(data)
}

#' Print a sampleprice object
#' @param x an object of class sampleprice, usually, a result of a call to 'sample_price'
#' @export print.sampleprice
print.sampleprice <- function(x,...)
{
}

#' Produce result summaries on tick data
#' @param x an object of class sampleprice, usually, a result of a call to 'sample_price'
#' @export summary.sampleprice
summary.sampleprice <- function(x,...)
{
}

#' Plot a sampleprice object
#' @param x an object of class sampleprice, usually, a result of a call to 'sample_price'
#' @export plot.sampleprice
plot.sampleprice <- function(x,...)
{
}
