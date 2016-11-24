#ifndef _MIXER_H
#define _MIXER_H

#include <tuple>
#include <vector>
#include <Eigen/Dense>
#include <Base.h>

class Mixer : public Base
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

	static std::tuple<std::vector<size_t>, std::vector<size_t>, time_vector> 
	mix(const std::vector<time_vector *>& time);
};

#endif
