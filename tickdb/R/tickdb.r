decode.header <- function(h)
{
	types = rep('c',length(h)) # by default all types are character
	for(i in 1:length(h)) # then adjust each type
	{
		if(str_detect(h[i],"^price|^bid[0-9]|^ask[0-9]"))
			types[i]='d'
		if(str_detect(h[i],"^volume|^size|^bidv|^askv|^nbid|^nask"))
			types[i]='i'
	}
	str_c(types,collapse='')
}

# convert a response to a data frame with market data
convert.influx <- function(response,sample,stype)
{
	foreach(r=response) %dopar%
	{
		if(r$status_code==200 & length(r$content)>0)
		{
			text = rawToChar(r$content) # convert to text
			header = decode.header(unlist(str_split(readLines(textConnection(text),1),',')))
			if(!sample) # tick data
			{
				data = readr::read_csv(text,col_types=header)
				# convert or remove some columns
				idx = which(str_detect(names(data),"name")) # remove column "name"
				if(length(idx)>0)
					data = data[ , -idx]
				idx = which(str_detect(names(data),"time")) # convert "time" to recv bit64
				if(length(idx)>0)
				{
					data$time = as.integer64(data$time)
					names(data)[idx]='recv'
					data$date = as.POSIXct(nanotime(data$recv))
				}
				idx = which(str_detect(names(data),"exch")) # convert "exch" to bit64
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
					data = readr::read_csv(text,col_types='ccccddddiiii',
					col_names=c('name','tags','time','close_exch','open_price','max_price',
								'min_price','close_price','open_volume','max_volume',
								'min_volume','close_volume'),skip=1)
				}

				idx = which(str_detect(names(data),"name")) # remove column "name"
				if(length(idx)>0)
					data = data[ , -idx]
				idx = which(str_detect(names(data),"tags")) # remove column "tags"
				if(length(idx)>0)
					data = data[ , -idx]
				idx = which(str_detect(names(data),"time")) # convert "time" to bit64
				if(length(idx)>0)
					data$time = as.integer64(data$time)
				idx = which(str_detect(names(data),"close_exch")) # convert "exch" to bit64
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
	do.call(rbind, str_split(sc$contracts$ProductID,fixed('.'))) %>% 
		as_tibble %>% 
		transmute(contracts=sc$contracts$ProductID,from=sc$timestamps$nanofrom,
				  to=sc$timestamps$nanoto, type=V2,product=V3,expiry=V4) %>%
		rowwise() %>%
		(function(row)
			str_c(
				"select ",fields," from ",measurement," where ", "product='",row$product,
				"' and expiry='",row$expiry,"' and type='",row$type,"' and ",
				str_c("time>=",row$from, " and time<=",row$to),
				ifelse(str_length(group)>0, str_c(" group by ",group), "")))
}

# generate sequence of contract and business date for a product, and the nanoseconds timestamps of required periods
seq.contracts <- function(product,type,front,rolldays,from,to,periods,idb)
{
	s = seq(ymd(from), ymd(to), by='1 day') # sequence of days [from,to]
	regex = str_c("PROD\\.",type,"\\.",product)
	x=idb[grepl(regex,idb$ProductID) & idb$Type=="F",]
	x$expdate = ymd(x$ExpiryDate) - days(rolldays)

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
	from.s = str_c(str_sub(from,1,4),str_sub(from,5,6),str_sub(from,7,8),sep='-')
	to.s   = str_c(str_sub(to,1,4),  str_sub(to,5,6),  str_sub(to,7,8),sep='-')
	#if(!has.calendars(exch2ql(exch))) # load bizness calendars
	#{
		suppressMessages(load_quantlib_calendars(from=from.s,to=to.s))
	#}
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
	timestamps = foreach(d=iterators::iter(contracts,by='row'),.combine=rbind) %do%
	{
		# for each period, convert to UTC nanosecond timestamps
		periods %>%
		rowwise() %>%
		mutate(date=d$date[1],
			  nanofrom=as.integer64(nanotime::nanotime(with_tz(as.POSIXct(d$date[1]+hm(from),tz=exch2tz(exch))))),
			  nanoto  =as.integer64(nanotime::nanotime(with_tz(as.POSIXct(d$date[1]+hm(to),  tz=exch2tz(exch)))))
			  )
	}

	list(contracts=contracts,timestamps=timestamps, idb=idb)
}

# Load the instrument database
load_idb <- function(db,con)
{
	# Get data from Influx
	response = httr::GET(url = "", scheme = con$scheme, hostname = con$host, port = con$port, 
						 path = "query",
						 query = list(db=db, u=con$user, p=con$pass, q="select * from refdata"),
						 add_headers(Accept="application/csv"))
	if(response$status_code==200)
	{
		text = rawToChar(response$content)
		options(readr.show_progress=F)
		data = readr::read_csv(text,progress=F)
		return(data)
	} else {
		return(NULL)
	}
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
make.query <- function(db,measurement,product,type,from,to,periods,
					   front,rolldays,fields,con,extended_result=F)
{
	if(!exists("idb")) # load instrument database
		idb <<- load_idb(db,con)

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
			fields = str_c('otype,exch',fields,'strike,cp',sep=',')
		else
			fields = str_c('otype,exch',fields,sep=',')
	}

	# generate queries
	if(!extended_result)
	{
		return(
			   seq.contracts(product,type,front,rolldays,from,to,periods,idb) %>%
				create.query(fields,measurement)
			)
	} else {
		sequence = seq.contracts(product,type,front,rolldays,from,to,periods,idb)
		query = sequence %>% create.query(fields,measurement)
		sequence = sequence$contracts
		return(list(sequence=sequence,query=query))
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
					  query = list(db=db, u=con$user, p=con$pass, q=q), add_headers(Accept="application/csv"))
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
	data.frame(from=sprintf("%2d:00",from),to=sprintf("%2d:00",to))
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
					 config="~/recherche/tickdatabase/influx/config.json")
{
	# Initialize Influx connection
	cfg = read_json(config)
	con = influx.connection(cfg$host)

	# Create a query
	db = cfg$dbname
	query = make.query(db,measurement,product,type,from,to,periods,front,rolldays,fields,con)

	# Run query
	data = run.query(query,db,con=con)

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
		fields="last(exch),first(bid1),max(bid1),min(bid1),last(bid1),first(bidv1),max(bidv1),min(bidv1),last(bidv1),first(ask1),max(ask1),min(ask1),last(ask1),first(askv1),max(askv1),min(askv1),last(askv1)"
		measurement = 'book'
	} else if(measurement=='trade')
	{
		fields="last(exch),first(price),max(price),min(price),last(price),first(volume),max(volume),min(volume),last(volume)"
	}

	# Initialize Influx connection
	cfg = read_json(config)
	con = influx.connection(cfg$host)

	# Create a query
	db = cfg$dbname
	equery = make.query(db,measurement,product,type,from,to,periods,front,rolldays,fields,con,
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
		}
	}

	return(data)
}

# get_qtg_inst <- function(db,con)
# {
# 	response = httr::GET(url="",scheme=con$scheme,hostname=con$host,port=con$port,
# 						 path="query",
# 						 query=list(db=db,u=con$user,p=con$pass,q="select * from qtg_instruments"),
# 						 add_headers(Accept="application/csv"))
# 	if(response$status_code==200)
# 	{
# 		text=rawToChar(response$content)
# 		options(readr.show_progress=F)
# 		data=readr::read_csv(text,progress=F)
# 		return(data)
# 	} else {
# 		return(NULL)
# 	}
# }
# 
# get_qtg_prod_map <- function(db,con)
# {
# 	response = httr::GET(url="",scheme=con$scheme,hostname=con$host,port=con$port,
# 						 path="query",
# 						 query=list(db=db,u=con$user,p=con$pass,q="select * from product_name_mapping"),
# 						 add_headers(Accept="application/csv"))
# 	if(response$status_code==200)
# 	{
# 		text=rawToChar(response$content)
# 		options(readr.show_progress=F)
# 		data=readr::read_csv(text,progress=F)
# 		return(data)
# 	} else {
# 		return(NULL)
# 	}
# }
# 
# 