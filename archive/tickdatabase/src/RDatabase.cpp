// [[Rcpp::plugins("cpp11")]]
// [[Rcpp::depends(BH)]]

#include <vector>
#include <Rcpp.h>
#include <simulator/Database.h>

using namespace Rcpp;

class RDatabase : public Database
{
	public:
		using Database::Database;

		DataFrame select_files( std::string contract,
								int from, int to,
								int front, int roll)
		{
			// the following trick is due to some limitation of Rcpp with overloaded methods.
			// In fact, R itself can't overload functions based solely on parameters' types
			// because there is no strong typing in R
			// And I'm almost sure I don't have data before the year 1000. However, it's not
			// impossible we fill in the database one day with very old companies like
			// Kongo Gumi, which has been trading for 1428 years !
			// http://www.investopedia.com/financial-edge/0711/5-of-the-worlds-oldest-companies.aspx

			if(to < 10000000) // 1000-00-00 is ... not a date ;-)
				return make_dataframe(Database::select_files(contract,front,std::to_string(from),to,roll)); // period=to
			else // but 20151017 is a date
				return make_dataframe(Database::select_files(contract,front,std::to_string(from),std::to_string(to),roll));
		}

	private:
		DataFrame make_dataframe(const Flist& l1)
		{
			CharacterVector file(l1.size());
			CharacterVector date(l1.size());

			// Build 2 vectors for the data.frame
			size_t i=0;
			for(const auto& x : l1)
			{
				file[i] = std::get<0>(x).string();
				date[i] = boost::gregorian::to_iso_string(std::get<2>(x));
				i++;
			}

			return DataFrame::create(Named("file")=file, Named("date")=date);
		}

		
};

// Exposing the class to R
RCPP_MODULE(tickdatabase)
{
    using namespace Rcpp ;

    class_<RDatabase>("Database")
    .constructor<std::string, std::string>()
	.method("select_files", &RDatabase::select_files);
}
