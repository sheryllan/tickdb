#include <unordered_map>
#include <vector>
#include <sstream>
#include <string>
#include <cstdio>
#include <cstring>
#include <boost/python.hpp>

namespace py=boost::python;

py::dict line2msg(const std::string& line)
{
	std::unordered_map<std::string, std::vector<std::string>> data;
	std::stringstream ss(line);
	std::string key;
	std::string value;
	while(std::getline(ss,key,':'))
	{
		std::getline(ss,value,':');
		if(value.back()=='\n')
			value.pop_back();
		data[key].push_back(value);
	}

	py::dict d1;
	for(const auto& obj : data)
	{
		if(obj.second.size()>1)
		{
			for(size_t i=0; i<obj.second.size(); i++)
				d1[obj.first+"."+std::to_string(i)] = obj.second[i];
		}
		else
		{
			d1[obj.first] = obj.second.front();
		}
	}

	return d1;
}

BOOST_PYTHON_MODULE(line2msg)
{
	py::def("line2msg", line2msg);
}
