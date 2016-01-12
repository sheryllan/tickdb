#include <iostream>
#include <string>
#include <cmath>
//#include <boost/algorithm/string.hpp>

using namespace std;
//using namespace boost;

int main()
{
	string line;
	while(getline(cin,line))
	{
		char c=line[0];
		if(c==0)
			continue;
		else if( (c>='a' && c<='z') || (c>='A' && c<='Z') )
			cout << '\n' << line;
		else
		{
			// quick parsing of the line
			// key name
			unsigned long int x1{line.find_first_not_of(" \t")};
			unsigned long int x2{line.find_first_of(" \t(",x1)};
			unsigned long int x3{line.find_first_of('(',x2)};
			unsigned long int x4{line.find_first_of(')',x3)};
			unsigned long int x5{line.find_first_of('[',x4)};
			unsigned long int x6{line.find_last_of(']')};

//cout << ';'<<line.substr(x1,x2-x1) << ';'<<endl;
//cout << ';'<<line.substr(x3+1,x4-x3-1) << ';'<<endl;
//cout << ';'<<line.substr(x5+1,x6-x5-1) << ';'<<endl;
			cout << line.substr(x1,x2-x1) << ':';
			
			string type{line.substr(x3+1,x4-x3-1)};
			string val{line.substr(x5+1,x6-x5-1)};

			if(type == "byteVector")
			{
				unsigned long int y1{val.find_first_of('|')};
				cout << stoull(string(begin(val)+y1+1, end(val)), 0,16) << ':';
			}
			else if(type == "decimal")
			{
				unsigned long int y1{val.find_first_of('|')};
				long int pw{stol(string(begin(val)+1,begin(val)+y1))};
				long int mt{stol(string(begin(val)+y1+1,end(val)-1))};
				cout << fixed << pow(10.0,pw)*mt << ':';
			}
			else
			{
				cout << val << ':';
			}
		}
	}
}
