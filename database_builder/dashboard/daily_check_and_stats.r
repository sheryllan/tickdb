if(!exists("named.list",mode="function"))
	named.list <- function(...) 
	{  
		    setNames( list(...) , as.character( match.call()[-1]) ) 
	}

# Test if timestamps are increasing non-monotonically
# return true any timestamps go back in time
has_monotonic_timestamps <- function(x)
{
	if(class(x)=="numeric")
		ts = x
	else if(class(x)=="data.frame" && "recv" %in% names(x))
		ts = x$recv
	else return(F)

	ts = ts[!is.na(ts)]
	if(length(ts)>1)
	{
		test = diff(ts[!is.na(ts)]) < 0 # test if any timestamp goes back in time
		return ( sum(test) == 0) # there should be none
	}
	else return(T) # one of zero timestamps is monotonic
}

# Check if there are zero timestamps
# return true if found any
has_only_non_zero_timestamps <- function(x)
{
	if(class(x)=="numeric")
		ts = x
	else if(class(x)=="data.frame" && "recv" %in% names(x))
		ts = x$recv
	else return(F)

	ts = ts[!is.na(ts)]
	if(length(ts)>0)
	{
		test = ts==0 # test if any timestamp is zero
		return ( sum(test) == 0) # there should be none
	}
	else return(F)
}

# Check if order books are crossed ask < bid 
# return true if any book is crossed
has_no_crossed_books <- function(x)
{
	if(class(x)=="data.frame" && "bid1" %in% names(x) && "ask1" %in% names(x))
	{
		test = x$ask1 <= x$bid1
		return(sum(test,na.rm=T)>0)
	}
	else return(F)
}

# Check for zero or negative quantity
# return true if any quantity is invalid
has_no_zero_or_neg_qty <- function(x)
{
	if(class(x)=="data.frame")
	{
		vol_names=grep("bidv|askv",names(x)) # get volume column names
		vol = as.matrix(x[ , vol_names]) # extract volume as a matrix
		
		return(sum(!is.na(vol) & vol <= 0 ) == 0)
	}
	else return(F)
}

# Check if the prices are sorted in the right order for every book
has_consistent_books_prices <- function(x)
{
	if(class(x)=="data.frame" & nrow(x)>0)
	{
		bid = as.matrix(x[ , grep("^bid[0-9]",names(x))]) # get bid prices
		ask = as.matrix(x[ , grep("^ask[0-9]",names(x))]) # get ask prices

		# a valid book has at least one value
		valid_bid_rows = which(rowAnys(!is.na(bid)))
		valid_ask_rows = which(rowAnys(!is.na(ask)))
		if(length(valid_bid_rows)>0)
		{
			dbid <- rowDiffs(bid, rows = valid_bid_rows)
			# diffs must all be negative or NA for bid
			test_bid = all(rowAlls(dbid < 0 | is.na(dbid)))
		}
		else test_bid = T # no books at all are consistent books anyway

		if(length(valid_ask_rows)>0)
		{
			dask <- rowDiffs(ask, rows = valid_ask_rows)
			# diffs must all be negative or NA for ask
			test_ask = all(rowAlls(dask > 0 | is.na(dask)))
		}
		else test_ask = T # no books at all are consistent books anyway

		# 1. take the diffs between each level (dbid, dask)
		#    missing values give NA
		# 2. test if the diffs are all positive (or neg. for bid) or NA
		#    NA is when the book has less than MAX levels (MAX=5 in general)
		# 3. rowAlls() check that if each quote has a well-formed book according to rule 2
		# 4. all() check that all the given quotes are good in the data set
		# 5. return true if both bid and ask have only well-formed books

		return(test_bid & test_ask)
	}
	else return(F)
}

# test if trades all have valid prices and volumes (>0)
has_valid_trades <- function(x)
{
	if(class(x)=="data.frame" & nrow(x) >= 0)
	{
		# columns 4 and 5 are always price and quantity.
		# columns 6 to 10 are not always used depending on what the
		# exchange provides.
		# We test only 4 and 5

		price <- x[,4]
		qty <- x[,5]
		return(all(sum(is.na(price))==0 & sum(is.na(qty))==0 & price >0 & qty > 0))
	}
	else return(F)
}

# ###################################
# 			Stats functions
# ###################################

# Return 
# - numbers of trades
# - number of quotes
# - ratio nb trades / nb quotes
# - average volume per trade
# - average traded price
# - average weighted traded price
# - average time between trades
# - average time of a mixture of gaussians: min_avg, max_avg
# - average time between the last quote and a trade
# - ratio of reconciled trades over total number of trades
#
# Note on the mixture: trades are sent in batch so we have period during which
# many trade reports arrives and period during which the market evolves without trade
# Not all the exchange send a trade summary, which would be a good estimator for the time
# between trades. Therefore we assume the time between trades is roughly distributed according
# to a mixture of Gaussians and we report the mean value for the 2 components of this mixture.
# On certain market, these 2 means can be very different than the simple average trade 
# between trade
test_and_stats_on_trades <- function(x)
{
	idx = which(x$otype=='T') # get trade indices

	# stats
	nb_trades = length(idx)
	nb_quotes = sum(x$otype!='T' & x$otype!='S')
	ratio_trades_quotes = nb_trades/nb_quotes

	if(length(idx)>0)
	{
		if(idx[1]==1) # avoid problems with the following idx-1 expressions ;-)
			idx = idx[2:length(idx)]

		# get prices and volumes
		trade_price = x[idx , 4] 
		trade_qty   = x[idx , 5] 
		trade_time  = x[idx,"recv"]
		time_quote_before_trade = x[idx-1,"recv"]

		bid = x[idx-1, 'bid1']
		ask = x[idx-1, 'ask1']
		bidv= x[idx-1, 'bidv1']
		askv= x[idx-1, 'askv1']

		# more stats
		avg_trade_vol = mean(trade_qty)
		avg_trade_price = mean(trade_price)
		avg_weighted_trade_price = sum(as.numeric(trade_price)*as.numeric(trade_qty))/sum(trade_qty)

		if(length(trade_time)>1)
		{
			z=diff(trade_time)
			q=quantile(z,seq(0,1,0.01))
			z = as.numeric(z[z<q[101]]) # remove values when there is an interruption in the trading session 
			avg_time_between_trade_millisecs <- mean(z)/1e6  # in milliseconds
			avg_time_quote_to_next_trade_millisecs <- mean(trade_time-time_quote_before_trade)/1e6
		}
		else {
			avg_time_between_trade_millisecs <- NA
			avg_time_quote_to_next_trade_millisecs <- NA
		}
	
		# test if trades' prices are equal to either the bid or ask prices
		test_price = trade_price == bid | trade_price == ask
		# test if trades' qties are less or equal than the top of the book volume
		test_qty   = trade_qty <= bidv | trade_qty <= askv

		# test if trades reconcile
		test_trade = test_price & test_qty
		ratio_rec_trades = sum(test_trade)/length(idx)


		named.list(nb_trades,nb_quotes,ratio_trades_quotes,
				   avg_trade_vol,avg_trade_price,avg_weighted_trade_price,
				   avg_time_between_trade_millisecs, avg_time_quote_to_next_trade_millisecs,
				   ratio_rec_trades)
	}
	else list(nb_trades=nb_trades,nb_quotes=nb_quotes,ratio_trades_quotes=ratio_trades_quotes,
				   avg_trade_vol=NA,avg_trade_price=NA,avg_weighted_trade_price=NA,
				   avg_time_between_trade_millisecs=NA, avg_time_quote_to_next_trade_millisecs=NA,
				   ratio_rec_trades=1)
}

guess_ticksize=function(df)
{
    x = df$bid1; y=df$ask1
    b = abs(x[2:length(x)] - x[1:(length(x)-1)])
    a = abs(y[2:length(y)] - y[1:(length(y)-1)])
	z=c(b,c)
	min(z[z!=0],na.rm=T)
}

stats_on_quotes <- function(x,quotes,trades)
{
	# Assume x is a data.frame of quotes only
	delta=c(1,5,10,20,50,100,500,1000,2000,5000,10000) # delta in number of ticks
	ts=quotes$recv # timestamps

	avg_time_between_quotes = lapply(delta, function(d) mean(diff(ts,lag(d))))
	names(avg_time_between_quotes) = paste0("avg_time_between_quotes_",delta) 

	# avg time between mid price changes
	mid = (quotes$bid1+quotes$ask1)/2
	idx=c(1,which(c(F,diff(mid)!=0))) # indices of each new prices
	avg_delta_time_between_midprice_changes = mean(diff(ts[idx]))
	avg_delta_tick_between_midprice_changes = mean(diff(idx))

	# quotes/second
	duration_secs = (ts[length(ts)]-ts[1])/1e9 # convert to seconds
	quotes_per_second = nrow(quotes) / duration_secs
	# trades/second
	tts = trades$recv
	trades_per_second = nrow(trades) / ((tts[length(tts)]-tts[1])/1e9)

	# quotes/minutes
	duration_mins = ((ts[length(ts)]-ts[1])/1e9)/60 # convert to seconds
	quotes_per_minute = nrow(quotes) / duration_mins
	# trades/minutes
	trades_per_minutes = nrow(trades) / (((tts[length(tts)]-tts[1])/1e9)/60)

	# avg bid/ask spread
	tick_size = guess_ticksize(quotes)
	avg_bid_ask_spread = mean(quotes$ask1-quotes$bid1)/tick_size

	result = named.list(avg_delta_time_between_midprice_changes,
	   avg_delta_tick_between_midprice_changes,quotes_per_second,trades_per_second,
	   quotes_per_minute,trades_per_minutes,avg_bid_ask_spread)

	list(result, avg_time_between_quotes)
}
