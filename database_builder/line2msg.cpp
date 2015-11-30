#include <unordered_map>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/python.hpp>

namespace py=boost::python;
py::dict line2msg(const std::string& line)
{
	std::vector<std::string> l;
	//split(l,line,boost::algorithm::is_any_of(":"));
	boost::split(l,line,[](char c){return c==':';});
	l.back().pop_back();

	std::unordered_map<std::string, std::vector<boost::iterator_range<std::string::iterator>>> data;

	for(size_t i=0; i<l.size(); i+=2)
		data[l[i]].push_back(l[i+1]);

	py::dict d1;
	for(auto it=begin(data); it!=end(data); ++it)
	{
		if(it->second.size()>1)
		{
			for(size_t i=0; i<it->second.size(); i++)
				d1[it->first+"."+std::to_string(i)] = 
					std::string((it->second)[i].begin(),(it->second)[i].end());
		}
		else
		{
			d1[it->first] = std::string(it->second.front().begin(), it->second.front().end());
		}
	}

	return d1;
}

BOOST_PYTHON_MODULE(line2msg)
{
	py::def("line2msg", line2msg);
}
