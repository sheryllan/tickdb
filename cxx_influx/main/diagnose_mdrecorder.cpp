#include <boost/filesystem.hpp>
#include "Log.h"
#include "Configuration.h"
#include "CSV_To_Influx_Msg.h"
#include "Find_MDRecorder_Files.h"
#include "Tick_To_Influx.h"
#include <boost/algorithm/string.hpp>
#include "Poco/Exception.h"
#include <set>
#include <iostream>
#include <regex>

using namespace boost::filesystem;
using namespace std;
using namespace cxx_influx;
namespace {
void print_msg(const Influx_Msg& msg)
{
    std::cout << *msg._msg << std::endl;
}

void print_file(const DateFileMap& files_)
{
    for (auto& pair : files_)
    {
        for (auto& pair2 : pair.second)
        {
            std::cout << pair2.second._file_path.native() << std::endl;
        }
    }
}

bool parse_reactor_source_file(const std::string& file_path_, std::unordered_map<std::string, std::string>& reactor_source_config_)
{
    std::fstream file(file_path_);
    if (!file)
    {
        std::cout << "Failed to open reactor source config file " << file_path_ << std::endl;
        return false;
    }
    std::string line;
    while(std::getline(file, line))
    {
        boost::algorithm::trim(line);
        if (line.empty()) continue;            
        std::vector<std::string> cols;
        boost::algorithm::split(cols, line, boost::algorithm::is_any_of(","));
        for (auto& str : cols)
        {
            boost::algorithm::trim(str);
        }
        if (cols.size() < static_cast<size_t>(ReactorSourceColumn::count))
        {
            std::cout << "Invalid format of reactor source config file " << file_path_ << std::endl;
            return false;
        }
        if (cols[static_cast<size_t>(ReactorSourceColumn::product)] == "product")
        {
            continue;//header;
        }
        reactor_source_config_[cols[static_cast<size_t>(ReactorSourceColumn::product)]] = cols[static_cast<size_t>(ReactorSourceColumn::source)];
    }
    return true;
}
void generate_influx_msg(int argc, char * argv[])
{
    if (argc <= 4)
    {
        std::cout << "incorrect parameter provided. generate_influx_msg <mdrecorder file> <source> <tick count in one influx msg>" << std::endl;
        return;
    }
    path tick_file(argv[2]);
    std::string file_name(tick_file.filename().string());
    std::string source(argv[3]);
    CSVToInfluxMsg cti(atoi(argv[4]));
    cti.generate_points(TickFile{tick_file, file_size(tick_file), 0, source}, &print_msg);
        
}

void dump_tick_files(int argc, char * argv[])
{
    if (argc <= 2)
    {
        std::cout << "incorrect parameter provided. dump_tick_files  <mdrecorder files' dir> <product types> <product names> <begin date> <end date>, the last foure parameters are optional." << std::endl;
        return;
    }
    std::string types, names;
    if (argc > 3) types = argv[3];
    if (argc > 4) names = argv[4];
    std::set<std::string> product_names;
    if (!names.empty())
    {
        std::vector<std::string> name_vec;
        boost::algorithm::split(name_vec, names, boost::algorithm::is_any_of(","));
        for (auto& str : name_vec)
        {
            product_names.insert(str);
        }
    }
    Valid_Reactor_Product valid_product = [&types, &product_names](const char type_, const std::string& product_) -> bool
                                          {
                                              if (!types.empty()) if (types.find(type_) == std::string::npos) return false;
                                              if (!product_names.empty()) if (product_names.find(product_) == product_names.end()) return false;
                                              return true;
                                          };
    Get_Source get_source = [](const std::string& product_)
                            {
                                return "doesnot matter";
                            };
    std::string dir(argv[2]);
    Date_Range range;
    if (argc > 5) range._begin = atoi(argv[5]);
    if (argc > 6) range._end = atoi(argv[6]);
    Find_MD_Files_In_Parallel fmfip(dir, valid_product, get_source, range);
    fmfip.find_files();
    print_file(fmfip.files());
}
void generate_influx_msg_m(int argc, char * argv[])
{
    if (argc <= 4)
    {
        std::cout << "incorrect parameter provided. generate_influx_msg_m <files that contains full paths to multiple mdrecorder files, one tick file per line> \
             <source config file> <tick count in one influx msg> <decode thread count, default 4> <write thread count default 4> last two are optional" << std::endl;
        return;
    }

    path tick_file(argv[2]);
    std::fstream file(tick_file.native(), std::ios::in);
    if (!file)
    {
        std::cout << "Cannot open file " << tick_file.native() << std::endl;
        return;
    }
    std::unordered_map<std::string, std::string> source_config;
    if (!parse_reactor_source_file(argv[3], source_config)) return;
    Get_Source get_source = [&source_config](const std::string& product_)
                            {
                                return source_config[product_];
                            };
    size_t batch_count = atoi(argv[4]);
    Generate_Points generate_points = [batch_count](const TickFile& file_, const Msg_Handler& handler_)
                                    {
                                        CSVToInfluxMsg cti(batch_count);
                                        cti.generate_points(file_, handler_);
                                    };

    std::string line;
    DateFileMap tick_files;
    while(std::getline(file, line))
    {
        path tick_file(line);
        std::string file_name(tick_file.filename().string());
        std::string product;
        char type;
        int32_t date;
        if (!Find_MD_Files::extract_product_type_date(file_name, product, type, date))
        {
            std::cout << "Invalid mdrecorder file " << file_name << std::endl;
            continue;
        }
        //date does not matter here
        tick_files[0][file_name] = TickFile{tick_file, file_size(tick_file), 0/*make all messages write to one file on one thread*/, source_config[product]};
    }
    setenv("WRITE_TO_FILE", "TRUE", 1);
    Tick_To_Influx ttf("", 0, "", generate_points);
    size_t decode_thread_cnt = 4;
    size_t write_thread_cnt = 4;
    if (argc > 5) decode_thread_cnt = atoi(argv[5]);
    if (argc > 6) write_thread_cnt = atoi(argv[6]);
    ttf.process_files(tick_files, decode_thread_cnt, write_thread_cnt);
}
void add_processed_files(int argc, char * argv[])
{
    if (argc <= 5)
    {
        std::cout << "incorrect parameter provided. add_processed_files <files that contains full paths to multiple mdrecorder files, one tick file per line> <influx http host> <http port> <influx db>" << std::endl;
        return;
    }
    std::string http_host(argv[3]);
    int32_t http_port = atoi(argv[4]);
    std::string influx_db(argv[5]);
    std::fstream file(argv[2], std::ios::in);
    if (!file)
    {
        std::cout << "Cannot open file " << argv[2] << std::endl;
        return;
    }
    Tick_To_Influx::Processed_Files processed_files;
    Tick_To_Influx::get_processed_files(processed_files, http_host, http_port, influx_db, Date_Range());
    std::string line;
    while(std::getline(file, line))
    {
        path tick_file(line);
        std::string file_name(tick_file.filename().string());
        std::string product;
        char type;
        int32_t date;
        if (!Find_MD_Files::extract_product_type_date(file_name, product, type, date))
        {
            std::cout << "Invalid mdrecorder file " << file_name << std::endl;
            continue;
        }
        processed_files[date].insert(file_name);
    }
    Tick_To_Influx::update_processed_files(processed_files, http_host, http_port, influx_db);        
    std::cout << "Done." << std::endl;   
}
struct Product
{
    std::string _name;//FDAX, ODAX etc...
    char _type;//E, F, O ....
    std::string to_string() const
    {
        return "-" + _name + "-" + _type + "-";
    }
};
bool operator<(const Product& l, const Product& r)
{
    return (l._type < r._type || l._name < r._name);
}

class FindFilesForValidation
{
public:
    FindFilesForValidation(const std::set<Product>& products_)
        : _products(products_)
    {
    }   
    void find_files(const DateFileMap& tick_files_, int32_t date_, int32_t days_in_month_)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Finding files on date : " << date_;
        const int32_t start_day = date_ % 100 - 1;
        for (int32_t i = 0; i < days_in_month_; ++i)
        {
            int32_t day = (start_day + i) % days_in_month_ + 1;                
            int32_t date = date_ / 100 * 100 + day;
            auto it = tick_files_.find(date);
            if (it != tick_files_.end())
            {
                find_files(it->second);
            }
            if (_products.empty()) break;
        }
        if (!_products.empty())
        {
            std::ostringstream os;
            for (auto& p : _products) os << p._name << ':' << p._type << ';';
            CUSTOM_LOG(Log::logger(), logging::trivial::info) << "No mdrecorder files found for " << os.str();
        }
    }
    const std::vector<string>& get_files() const { return _files; }
private:
    void find_files(const TickFileMap& files_)
    {
        auto it = _products.begin();
        while (it != _products.end())
        {
            auto it_file = std::find_if(files_.begin(), files_.end(), [it](const TickFileMap::value_type& pair){ return pair.first.find(it->to_string()) != std::string::npos;});
            if (it_file != files_.end())
            {
                _files.push_back(it_file->second._file_path.native());
                it = _products.erase(it);
            }
            else ++it;
        }
    }
    std::vector<string> _files;
    std::set<Product> _products;        
};

int get_day_in_month(int max_day_)
{   
    std::random_device rd;  //Will be used to obtain a seed for the random number engine
    std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<> dis(1, max_day_);
    return dis(gen);
}
boost::gregorian::date get_date(int32_t date_)
{
    return boost::gregorian::date(date_ / 10000, (date_ % 10000) / 100, date_ % 100);
}

int32_t get_date(boost::gregorian::date date_)
{
    return static_cast<int32_t>(date_.year()) * 10000 + date_.month().as_number() * 100 + static_cast<int32_t>(date_.day());
}
void select_files_for_validation(const DateFileMap& tick_files_, const std::set<Product>& products_)
{
    using namespace boost::gregorian;
    if (tick_files_.empty())
    {
        std::cout << "no tick files found." << std::endl;
        return;
    }
    int32_t begin = tick_files_.begin()->first;
    int32_t end = tick_files_.rbegin()->first;
    std::set<std::string> files_for_validation;
    while (begin <= end)
    {
        date begin_date = get_date(begin); 
        int32_t days_in_month = static_cast<int>(begin_date.end_of_month().day());
        int32_t date = begin / 100 * 100 + get_day_in_month(days_in_month);
        FindFilesForValidation fffv(products_);
        fffv.find_files(tick_files_, date, days_in_month);
        for (auto& str : fffv.get_files()) files_for_validation.insert(str);    
        begin_date += boost::gregorian::months(1);
        begin = get_date(begin_date);
    }
    for (auto& str: files_for_validation)
    {
        std::cout << str << std::endl;
    }
}
void select_files_for_validation(int argc, char * argv[])
{
    if (argc < 4)
    {
        std::cout << "incorrect parameter provided. select_files_for_validation <products. one example is GOOG:F,GOOG:E. GOOG is the product name,F,E are the product type> <dir for md recorder files> <begin date> <end date>. the last two are optional." << std::endl;
        return;
    }
    std::string products_arg = argv[2];
    std::vector<std::string> products;
    boost::algorithm::split(products, products_arg, boost::algorithm::is_any_of(","));
    std::vector<std::string> product_vec;
    std::set<Product> filter_products;
    for (auto& str : products)
    {
        boost::algorithm::split(product_vec, str, boost::algorithm::is_any_of(":"));
        if (product_vec.size() < 2)
        {
            std::cout << "invalid product given : " << str << std::endl;
            return;
        }        
        filter_products.insert(Product{product_vec[0], product_vec[1][0]});
        product_vec.clear();
    }
    Valid_Reactor_Product valid_product = [](const char type_, const std::string& product_) -> bool
                                          {
                                              return true;
                                          };
    Get_Source get_source = [](const std::string& product_)
                            {
                                return "doesnot matter";
                            };
    std::string dir(argv[3]);
    Date_Range range;
    if (argc > 4) range._begin = atoi(argv[4]);
    if (argc > 5) range._end = atoi(argv[5]);
    Find_MD_Files_In_Parallel fmfip(dir, valid_product, get_source, range);
    fmfip.find_files();
    select_files_for_validation(fmfip.files(), filter_products);
}
void command_format()
{
    std::cout << "following command input supported." << std::endl;
    std::cout << "add_processed_files <files that contains full paths to multiple mdrecorder files, one tick file per line> <influx http host> <http port> <influx db>";
    std::cout << "dump_tick_files <mdrecorder files' dir> <product types> <product names> <begin date> <end date>, the last foure parameters are optional." << std::endl;
    std::cout << "generate_influx_msg <mdrecorder files> <source> <tick count in one influx msg>" << std::endl;   
    std::cout << "generate_influx_msg_m <files that contains full paths to multiple store tick files, one ticke file per line> <source config file> <tick count in one influx msg> <decode thread count, default 4> <write thread count default 4> last two are optional" << std::endl;   
    std::cout << "select_files_for_validation <products. one example is GOOG:F,GOOG:E. GOOG is the product name,F,E are the product type> <dir for md recorder files> <begin date> <end date>. the last two are optional." << std::endl;
}
}
int main(int argc, char * argv[])
{
 
try
{
    if (argc < 2) 
    {
        command_format(); 
        return 0;
    }
    if (!cxx_influx::Log::init()) return 0;

    if (strcmp(argv[1], "generate_influx_msg") == 0)
    {
        generate_influx_msg(argc, argv);       
        return 0;
    }
    if (strcmp(argv[1], "dump_tick_files") == 0)
    {
        dump_tick_files(argc, argv);
        return 0;
    }
    if (strcmp(argv[1], "generate_influx_msg_m") == 0)
    {
        generate_influx_msg_m(argc, argv);
        return 0;
    }
    if (strcmp(argv[1], "add_processed_files") == 0)
    {
        add_processed_files(argc, argv);
        return 0;
    }
    if (strcmp(argv[1], "select_files_for_validation") == 0)
    {
        select_files_for_validation(argc, argv);
        return 0;            
    }

    std::cout << "unknown command." << std::endl;
    return 0;
}
catch(Poco::Exception& e)
{
    std::cout << "poco error : " << e.displayText() << std::endl;
}
catch(std::exception& e)
{
    std::cout << "exception : " << e.what() << std::endl;
}


   BOOST_LOG_SEV(cxx_influx::Log::logger(), cxx_influx::logging::trivial::info) << "done";

}
