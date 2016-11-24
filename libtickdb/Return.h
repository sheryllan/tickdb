#ifndef _RETURN_H
#define _RETURN_H

#include <Eigen/Dense>
#include <Base.h>

class Return : public Base
{
public:
	// --------------------
	// Basic type defitions
	// --------------------

	typedef ob_type::uid_type	uid_type;
	typedef ob_type::time_type	time_type;
	typedef ob_type::price_type	price_type;
	typedef ob_type::volume_type	volume_type;

	typedef Eigen::Matrix<uid_type,Eigen::Dynamic,1> uid_vector;
	typedef Eigen::Matrix<time_type,Eigen::Dynamic,1> time_vector;
	typedef Eigen::Matrix<price_type,Eigen::Dynamic,1> price_vector;
	typedef Eigen::Matrix<volume_type,Eigen::Dynamic,1> volume_vector;

	// ----------------------
	// Available computations
	// ----------------------

	enum return_event
	{
		tick_interval,
		time_interval,
		front_signal,
		mid_price_change,
		bid_up,
		bid_down,
		ask_up,
		ask_down,
		top_in_window
	};

	enum return_calc
	{
		mid_return =		1<<0,
		log_mid_return =	1<<1,
		bid_return =		1<<2,
		log_bid_return =	1<<3,
		ask_return = 		1<<4,
		log_ask_return =	1<<5,
	};


	static Eigen::VectorXd compute(const std::vector<char>& otype, const time_vector& time,
					const price_vector& bid, const price_vector& ask,
					return_event event, return_calc calc,
					int interval);

private:
//	static Eigen::VectorXi find_uid(const uid_vector& uid_vect, const uid_type& uid);
//	static Eigen::VectorXd compute_price(const price_vector& bid, const price_vector& ask, return_calc calc);
};

#endif
