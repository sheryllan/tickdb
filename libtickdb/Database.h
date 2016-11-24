#ifndef _DATABASE_H
#define _DATABASE_H

#include <cstdint>
#include <cmath>
#include <list>
#include <utility>
#include <map>
#include <memory>
#include <string>
#include <tuple>

#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/filesystem.hpp>
#include <boost/numeric/ublas/matrix.hpp>
#include <boost/numeric/ublas/vector.hpp>
#include <boost/spirit/include/qi.hpp>

#include <config.h>
#include <Instrument.h>
#include <json.hpp>
#include <debug.h>

using json = nlohmann::json;
using qi   = boost::spirit::qi;

namespace tickdatabase
{

/** Read and interpret a tick data file from the database
 */
class TickFileReader
{
	public:
		/** Constructor
		 * Read the tick data file from \c filename. Throw an exception if needed
		 * \param filename tick data file name
		 */
		TickFileReader(const std::string& filename);

		/** Return a TickData object after interpreting the tick data file
		 */
		TickData get_tickdata();

	private:
		json config;
		std::list<std::vector<std::string>> tick_file;
		std::list<std::pair<size_t,std::string>> log;

		// Internals
		bool read_file(const std::string& filename);
		std::vector<std::string> csv_split(const std::string& line);

		template<typename T>
		T s2val(const std::string& str)
		{
			typedef typename std::conditional<
			std::is_floating_point<T>::value,
				boost::spirit::terminal<boost::spirit::tag::double_>,
				boost::spirit::terminal<boost::spirit::tag::int_>>::type conv_type;

			if(str!="NA")
			{
				T result;
				if(boost::spirit::qi::parse(begin(str), end(str), conv_type(), result))
					return result;
				else
					return NAN;
			}
			else return NAN;
		}
};

/*** One day of tick data at level 2 (order books and trades)
 */
class TickData
{
public:
	TickData(size_t nrow,const boost::gregorian::date& date);

	using boost::numeric;

	boost::gregorian::date date;
	std::vector<Instrument> inst;

	// Data of the day
	std::vector<char>		otype;
	std::vector<uint64_t>	recv,exch;
	ublas::matrix<double>	bid,ask;
	ublas::matrix<int>		bidv,askv;
	ublas::matrix<int>		nbid,nask;
	// for options, several instruments are encoded within the same dataset
	// product points to entries of the object 'inst' defined above
	std::vector<std::vector<Instrument>::iterator>		product;

};

/// Sequence of daily tick data 
typedef std::map<boost::gregorian::date,TickData> TickDataSeq;

/** Time series of sampled data for one instrument only
 */
struct Sampled
{
	/** bid and ask are sampling on the order book
	 * last is the sampling on the traded price
	 */
	ublas::matrix<double> bid, ask, last;
	ublas::vector<int> tvol; /// traded volume
	std::vector<boost::posix_time::ptime> time;
};

/** A sequence of sampled data
 * When the sampling period is less than 12 hours, it contains one day of data
 * otherwise, it contains one month of data.
 * Matrix columns represent open, high, low, close in this order.
 * This class can store data for several instruments which happens for options all the time.
 * For simpler products (futures, stocks), the vector samples will only have one entry.
 */
class SampleData
{
public:
	
	using boost::numeric;

	/** date of sampling or first day of the month for period greater than 12 hours */
	boost::gregorian::date date;
	std::vector<Instrument> inst;
	std::vector<size_t> samples; /// the data for each instrument
};

/// Sequence of daily or monthly sample data depending on the sampling period
typedef std::map<boost::gregorian::date,SampleData> SampleDataSeq;

/** Contract information as recorded in the instruments' database */
struct Product
{
	std::string product_id; // unique exact name: FDAX201603, PROD.F.A50.APR2016
	std::string product; // generic name: FDAX, A50
	int lotsize;
	double ticksize;
	double minpriceinc; // value of one tick in contract's currency
	std::string currency;
	std::string exchange;
	boost::gregorian::date expirydate;
	std::string optiontype;
	double strike;
	std::string contract_type;
};

/** Internally used by the Database class to compute which files to load and roll out contracts */
struct DBFile
{
	boost::filesystem::path path;
	std::string contract;
	boost::gregorian::date date;
};

class Database
{
	public:
		Database(const std::string& config_filename, const std::string& provider="liquid_capture");
		const TickDataSeq& get_tick_data(
				const std::string& contract,
				unsigned int offset,
				const std::string& from,
				const std::string& to,
				unsigned int roll_period);

		const TickDataSeq& get_tick_data(
				const std::string& contract,
				unsigned int offset,
				const std::string& from,
				unsigned int length,
				unsigned int roll_period);

		const SampleDataSeq& get_sample_data(
				const std::string& contract,
				unsigned int offset,
				const std::string& from,
				const std::string& to,
				unsigned int roll_period);

		const SampleDataSeq get_sample_data(
				const std::string& contract,

				std::list<std::string> get_selected_files();
				unsigned int offset,
				const std::string& from,
				unsigned int length,
				unsigned int roll_period);

		std::list<std::string> get_selected_files();

		std::list<Product> find_product(const std::string& pattern);

	private:
		json config;
		boost::filesystem::path dbdir,instdb,daily_db;
		std::string provider;
		std::map<std::string, Product> products;
		std::list<DBFile> files;
		std::list<std::string> selected_files;
		boost::posix_time::ptime last_update_time;
		bool last_update_status=false;

		// Internal functions

		/// Do a full update if the 
		bool update();
		bool read_instdb();
		Product decode_instdb_line(const std::vector<std::string>& csv_line, const std::string& provider);
		Product decode_qtg_instdb_line(const std::vector<std::string>& csv_line, const std::string& provider);
		Product decode_liquid_capture_instdb_line(const std::vector<std::string>& csv_line, const std::string& provider);
};

#endif
