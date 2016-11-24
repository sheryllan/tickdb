#include <cmath>
#include <vector>
#include <limits>
#include <algorithm>
#include <boost/algorithm/string/classification.hpp>
#include <Return.h>

Eigen::VectorXd Return::compute(const std::vector<char>& otype, const time_vector& time,
				const price_vector& bid, const price_vector& ask,
				return_event event, return_calc calc,
				int interval)
{
	using namespace Eigen;
	bool is_log = calc==log_mid_return||calc==log_bid_return||calc==log_ask_return;

	// Get a price vector
	size_t N = otype.size();

	VectorXd price(N);
	switch(calc)
	{
		case mid_return:	price = (bid.array()+ask.array())*0.5; break;
		case log_mid_return:	price = log((bid.array() + ask.array())*0.5); break;
		case bid_return:	price = bid; break;
		case log_bid_return:	price = log(bid.array()); break;
		case ask_return:	price = ask; break;
		case log_ask_return:	price = log(ask.array()); break;
	}

	// Compute returns
	VectorXd ret(N);
	double p1,p2;
	const auto& is_quote = boost::is_any_of("ADMLCQ");
	const auto& is_trade = boost::is_any_of("T");

	switch(event)
	{
		default: break;
		case tick_interval:
			for(size_t i=0; i<N-interval; i++)
			{
				int i1=-1,i2=-2;
				if(is_quote(otype[i]))
				       i1=i;
				else if(is_trade(otype[i]))
				{
					i1=i-1;
					while(i1>=0 and is_trade(otype[i1]))
						i1--;
				}

				if(is_quote(otype[i+interval]))
					i2 = i+interval;
				else if(is_trade(otype[i+interval]))
				{
					i2 = i+interval-1;
					while(i2>i1 and i2>=0 and is_trade(otype[i2]))
						i2--;
					if(i1==i2)
						i2=-1;
				}

				if(i1!=-1 and i2!=-2)
				{
					if(is_log)
						ret[i] = price[i2]-price[i1]; // price is already a log here
					else
						ret[i] = price[i2]/price[i1];
				}
				else
					ret[i] = NAN;
			}

			// end of day when we don't have tick anymore to compute the return
			for(size_t i=N-interval; i<N; i++)
				ret[i]=NAN;

			break;
	}
//exit(1);
	return ret;
}

/*
Eigen::VectorXi Return::find_uid(const uid_vector& uid_vect, const uid_type& uid)
{
	using namespace Eigen;

	// Find uid
	Matrix<bool,Dynamic,1> c = uid_vect.array()==uid;

	// Vector of indices
	VectorXi I = VectorXi::LinSpaced(c.size(),0,c.size()-1);

	I.conservativeResize(std::stable_partition(I.data(), I.data()+I.size(),
				[&c](int i) { return c[i]; }) - I.data());

	return I;
}
*/
