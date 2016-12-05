#include <iterator>
#include <exception>
#include <stdexcept>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <regex>
#include <vector>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/tokenizer.hpp>
#include <boost/spirit/include/qi.hpp>
#include <Database.h>

#define DEBUG
#include <debug.h>

const size_t OBSIZE=5;

// -----------------------------
// TickFileReader implementation
// -----------------------------

TickFileReader::TickFileReader(const std::string& filename)
{
	// Read database file
	if(!read_file(filename))
		throw "error reading database file";
}

bool TickFileReader::read_file(const std::string& filename)
{
	using namespace std;
	using namespace boost::filesystem;
	using namespace boost::iostreams;

	if(exist(path(filename)) && extension(path(filename))==".bz2")
	{
		filtering_istream bzf;
		bzf.push(bzip2_decompressor{}); // Add a bzip2 decompressor
		bzf.push(file_source(filename));

		std::string line;
		while(std::getline(bzf, line)) // read file
		{
			if(bzf.fail()) // check read was good
				break;
			else
				try
				{
					tick_file.emplace_back(csv_split(line)); // parse csv line
				}
				catch(...) // catch parsing error
				{
					break;
				}
		}
		if(!bzf.eof()) // early end of the while loop
		{
			tick_file.clear();
			bzf.close();
			return false;
		}
		else
			bzf.close();

		// Decode date from filename
		vector<string> x;
		boost::algorithm::split(x,begin(filename), boost::is_any_of(".-_"));
		date = boost::gregorian::from_undelimited_string(*(x.end()-2));

		return true;
	}
	else
		return false;
}

std::vector<std::string> TickFileReader::csv_split(const std::string& line)
{
	namespace qi = boost::spirit::qi;

	// csv grammar
	using namespace std;
	// always the same variables, so static them to speed up a bit
	static qi::rule<string::const_iterator, string()> quoted_string = '"'>>(qi::char_-'"')>>'"';
	static qi::rule<string::const_iterator, string()> valid_characters = qi::char_ - '"' - '-';
	static qi::rule<string::const_iterator, string()> item = *(quoted_string | valid_characters);
	static qi::rule<string::const_iterator, vector<string>()> csv_parser = item %  ',';

	vector<string> result;
	if(!qi::parse(begin(line), end(line), csv_parser, result))
		throw 1;

	return result;
}

TickData TickFileReader::get_tickdata()
{
	using namespace std;

	tick.pop_front(); // remove header line from the CSV file
	size_t i=0;
	TickData td(tick_file.size(), date);
	stringstream error;

	enum field {otype=0,recv,exch,
		bid1,bid2,bid3,bid4,bid5,
		bidv1,bidv2,bidv3,bidv4,bidv5,
		nbid1,nbid2,nbid3,nbid4,nbid5,
		ask1,ask2,ask3,ask4,ask5,
		askv1,askv2,askv3,askv4,askv5,
		nask1,nask2,nask3,nask4,nask5,
		product};

	for(const auto& v : tick_file)
	{
		error.str(string()); // clear error message

		// decode line
		td.otype[i] = v[otype];
		td.recv[i]  = s2val<uint64_t>(v[recv]);
		if(td.recv[i]<=0) { error << "wrong recv timestamp at line "<< i+1; log.emplace_back(error.str()); }
		td.exch[i] = s2val<uint64_t>(v[exch]);
		if(td.exch[i]<=0) { error << "wrong exch timestamp at line "<< i+1; log.emplace_back(error.str()); }

		// Convert market data
		for(size_t j=0; j<OBSIZE; j++)
			td.bid(i,j) = s2val<double>(v[bid1+j]);

		for(size_t j=0; j<OBSIZE; j++)
			td.bidv(i,j) = s2val<int>(v[bidv1+j]);

		for(size_t j=0; j<OBSIZE; j++)
			td.nbid(i,j) = s2val<int>(v[nbid1+j]);
				
		for(size_t j=0; j<OBSIZE; j++)
			td.ask(i,j) = s2val<double>(v[ask1+j]);

		for(size_t j=0; j<OBSIZE; j++)
			td.askv(i,j) = s2val<int>(v[askv1+j]);

		for(size_t j=0; j<OBSIZE; j++)
			td.nask(i,j) = s2val<int>(v[nask1+j]);

		if(v.size()==34) // a product is present
		auto it = find(begin(inst),end(inst), [&v](const Instrument& ins){return ins.name==v[product];});
		if(it!=inst.end()) // product already exist
			td.product[i] = it;
		else // else create it
		{
			Instrument newi;
			newi.name = v[product];
			td.inst.push_back(newi);
			td.product[i] = td.inst.end()-1;
		}
		i++;
	}

	return rd;
}

// -----------------------
// TickData implementation
// -----------------------

TickData::TickData(size_t nrow,const boost::gregorian::date& date) : date(date)
{
	otype.resize(nrow);
	recv.resize(nrow);
	exch.resize(nrow);
	bid.resize(nrow,OBSIZE);
	bidv.resize(nrow,OBSIZE);
	nbid.resize(nrow,OBSIZE);
	ask.resize(nrow,OBSIZE);
	askv.resize(nrow,OBSIZE);
	nask.resize(nrow,OBSIZE);
	product.resize(nrow);
}

// -----------------------
// Database implementation
// -----------------------

Database::Database(const std::string& config_filename, const std::string& provider="liquid_capture")
{
}

const std::list<FElement>& Database::get_tick_data(
				const std::string& contract,
				unsigned int offset,
				const std::string& from,
				const std::string& to,
				unsigned int roll_period)
{
}

const std::list<FElement>& Database::get_tick_data(
		const std::string& contract,
		unsigned int offset,
		const std::string& from,
		unsigned int length,
		unsigned int roll_period)
{
}

const std::list<FElement>& Database::get_sample_data(
		const std::string& contract,
		unsigned int offset,
		const std::string& from,
		const std::string& to,
		unsigned int roll_period)
{
}

const std::list<FElement>& Database::get_sample_data(
		const std::string& contract,
		unsigned int offset,
		const std::string& from,
		unsigned int length,
		unsigned int roll_period)
{
}

std::list<std::string> Database::get_selected_files()
{
	return selected_files;
}

std::list<Product>::const_iterator Database::find_product(const std::string& pattern)
{
}

// -------------------------
// Internal Database methods
// -------------------------

bool Database::update()
{
	using namespace boost::posix_time;

	ptime now{second_clock::universal_time()};
	// update after initial or bad status or if last update if 10 minutes old
	if(!last_update_status or ( (now-last_update_time)>time_duration(0,10,0,0)))
	{
		last_update_time = second_clock::universal_time();
		last_update_status = read_instdb() and find_db_files();
		return last_update_status;
	}
	else
	{
		return last_status_update;
	}
}

bool Database::find_db_file()
{
	// check dbdir is valid
	if(!exists(dbdir) or !is_directory(dbdir))
		return false;

	// find files
	files.clear();
	for(recursive_directory_iterator it(dbdir),end; it!=end; ++it)
	{
		if(!is_directory(path()) and
			(it->path().extension()==".bz2" or it->path().extension()==".xz" or
			it->path().extension()==".gz"))
		{
			// store file name
			files.emplace_back(decode_db_filename(it->path().filename.string(),provider));
		}
	}

	return true;
}

DBFile decode_db_filename(const std::string& filename, const std::string& provider)
{
	if(provider=="qtg")
		return decode_qtg_db_filename(filename,provider);
	else if(provider=="liquid_capture")
		return decode_liquid_capture_db_filename(filename,provider);
	else
		return DBFile();
}

DBFile decode_qtg_db_filename(const std::string& filename, const std::string& provider)
{
	// decompose file name
	std::vector<std::string> v;
	split(v,it->path().filename().string(), boost::is_any_of("-_."));
}

DBFile decode_qtg_db_filename(const std::string& filename, const std::string& provider)
{
	// decompose file name
	std::vector<std::string> v;
	split(v,it->path().filename().string(), boost::is_any_of("-_."));
}

bool Database::read_instdb()
{
	using namespace std;
	using namespace boost::spirit;
	namespace qi = boost::spirit::qi;

	// Open main instrument database csv file
	ifstream file;
	file.open(instdb.string().c_str());
	if(!file.is_open())
		return false;

	// Parse csv file
	qi::rule<string::const_iterator, string()> quoted_string = '"'>>(qi::char_-'"')>>'"';
	qi::rule<string::const_iterator, string()> valid_characters = qi::char_ - '"' - '-';
	qi::rule<string::const_iterator, string()> item = *(quoted_string | valid_characters);
	qi::rule<string::const_iterator, vector<string>()> csv_parser = item %  ',';
	
	enum DBInstFields {ProductID=0,Product,Type,Exchange,Currency,Underlying,ExpiryDate,Strike,PutOrCall,
		ExerciseStyle,MinPriceIncrement,MinPriceIncrementAmount,SecurityDesc,PremiumDecimalPlace,
		SecurityID,UnderlyingSecurityID,MarketSegmentID,MarketSegment,DestinationExchange};

	std::string line;
	size_t l=0;
	while(std::getline(file, line)) // read file
	{
		if(!file.fail())
		{
			if(l) // ignore header line
			{
				vector<string> result;
				if(qi::parse(begin(line),end(line),csv_parser,result)) // decompose csv line
					products.emplace_back(result[0],decode_instdb_line(result,provider));
				l++;
			}
		}
		else break;
	}

	if(!file.eof()) // early end of the while loop due to read error
	{
		products.clear();
		file.close();
		return false;
	}
	else
	{
		file.close();
		return true;
	}
}

Product Database::decode_instdb_line(const std::vector<std::string>& csv_line, const std::string& provider)
{
	if(provider=="qtg")
		return decode_qtg_instdb_line(csv_line,provider);
	else if(provider=="liquid_capture")
		return decode_liquid_capture_instdb_line(csv_line,provider);
	else return Product();
}

Product Database::decode_qtg_instdb_line(const std::vector<std::string>& csv_line, const std::string& provider)
{
	Product p;
}

Product Database::decode_liquid_capture_instdb_line(const std::vector<std::string>& csv_line, const std::string& provider)
{
	Product p;
}
