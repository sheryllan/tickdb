suppressMessages(library(httr))
suppressMessages(library(Rcpp))
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
suppressMessages(library(tibble))

options(readr.show_progress=F)

#' Create a connection object to an InfluxDB
#' @param host hostname
#' @param port numerical. port number
#' @param scheme NA
#' @param user username
#' @param pass password
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

#' Query an influxdb server
#' @param con An influx_connection object
#' @param db Sets the name of the database
#' @param queries The Influxdb queries to send
influx.query <- function(queries,con,db='tickdb')
{
	# submit queries
	foreach(q = queries) %do%
	{
		httr::GET(url = "",
			scheme = con$scheme,
			hostname = con$host,
			port = con$port,
			path = "query",
			query = list(db=db, u=con$user, p=con$pass, q=q),
			add_headers(Accept="application/csv"))
	}
}

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
convert.influx <- function(response)
{
	foreach(r=response) %do%
	{
		if(r$status_code==200)
		{
			text = rawToChar(r$content) # convert to text
			header = decode.header(unlist(str_split(readLines(textConnection(text),1),',')))
			data = read_csv(text,col_types=header)
		} else { NULL} # remove failed responses
	}
}

load_idb <- function(config)
{
	cfg = read_json(config)
	suppressWarnings(suppressMessages(read_csv(cfg$instdb,progress=F)))
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
		idb = load_idb(idb)

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

	list(contracts=contracts,timestamps=timestamps, idb=idb)
}

# create.query <- function(sc,fields,measurement,group='')
# {
# 	df = bind_cols(sc$contracts,sc$timestamps)
# 	product=as_data_frame(do.call(rbind,str_split(df$ProductID,fixed('.'))))
# 	names(product)=c('x','type','product','expiry')
# 
# 	r=rle(product$expiry) # group similar expiry to generate one query only per expiry
# 	i=1
# 	foreach(k=r$lengths) %do%
# 	{
# 		t = i:(k+(i-1))
# 		nfrom = df$nanofrom[t]
# 		nto   = df$nanoto[t]
# 
# 		str_c(
# 			"select ",fields," from ",measurement," where ", "product='",product$product[t][1],
# 			"' and expiry='",expiry[t][1],"' and type='",type[t][1],"' and ",
# 			str_c("time>=",nfrom, " and time<=",nto, collapse=' or '),
# 			ifelse(str_length(group)>0, str_c("group by ",group), ""))
# 	}
# }
