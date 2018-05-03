#include "CSV_To_Influx_Msg.h"
#include "Log.h"
#include "gperftools/profiler.h"
#include <mutex>
#include <lzma.h>
#include <boost/algorithm/string.hpp>



namespace 
{
enum class BookColumnIndex : uint8_t
{
    otype = 0,
    recv,
    exch,
    bid1,bid2,bid3,bid4,bid5,bidv1,bidv2,bidv3,bidv4,bidv5,nbid1,nbid2,nbid3,nbid4,nbid5,ask1,ask2,ask3,
    ask4,ask5,askv1,askv2,askv3,askv4,askv5,nask1,nask2,nask3,nask4,nask5,count

};
enum class TradeColumnIndex : uint8_t
{
    price = BookColumnIndex::bid1,
    qty,
    side
};
enum class ProductAttr : uint8_t //PROD.O.GOOG.SEP2018.3000.C.0
{
    live = 0,
    type,
    product,
    expiry,
    strike,
    call_put,
    version,
    count
};
enum class NewColumn : uint8_t
{
    product = BookColumnIndex::count,
    state,
    nicts,
    count
};
enum class OldColumn : uint8_t
{
    market = BookColumnIndex::count,
    type,prod,series,strike,call_put,version,count
};
std::array<std::string, static_cast<uint8_t>(BookColumnIndex::count)> BOOK_FIELD_ARRAY;
const std::string MEASUREMENT_BOOK("book");
const std::string MEASUREMENT_TRADE("trade");

const std::string TAG_PRODUCT("product");
const std::string TAG_EXPIRY("expiry");
const std::string TAG_TYPE("type");
const std::string TAG_CALLPUT("cp");
const std::string TAG_VERSION("version");
const std::string TAG_STRIKE("strike");
const std::string TAG_MARKET("market");
const std::string TAG_INDEX("index");
const std::string TAG_SIDE("side");
const std::string TAG_SOURCE("source");

const std::string OTYPE("otype");
const std::string TRADE_PRICE("price");
const std::string TRADE_QTY("volume");
const std::string OTYPE_QUOTE("Q");
const std::string OTYPE_TRADE_SUMMARY("S");
const std::string OTYPE_TRADE("T"); //T is used in mdrecorder files that were generated in 2016
const std::string NA("NA");
const std::string NETWORK_TS("nicts");
const std::string DESCRIPTION("desc");

enum Side { buy = 0, sell = 1 };



bool
init_decoder(lzma_stream *strm)
{
    // Initialize a .xz decoder. The decoder supports a memory usage limit
    // and a set of flags.
    //
    // The memory usage of the decompressor depends on the settings used
    // to compress a .xz file. It can vary from less than a megabyte to
    // a few gigabytes, but in practice (at least for now) it rarely
    // exceeds 65 MiB because that's how much memory is required to
    // decompress files created with "xz -9". Settings requiring more
    // memory take extra effort to use and don't (at least for now)
    // provide significantly better compression in most cases.
    //
    // Memory usage limit is useful if it is important that the
    // decompressor won't consume gigabytes of memory. The need
    // for limiting depends on the application. In this example,
    // no memory usage limiting is used. This is done by setting
    // the limit to UINT64_MAX.
    //
    // The .xz format allows concatenating compressed files as is:
    //
    //     echo foo | xz > foobar.xz
    //     echo bar | xz >> foobar.xz
    //
    // When decompressing normal standalone .xz files, LZMA_CONCATENATED
    // should always be used to support decompression of concatenated
    // .xz files. If LZMA_CONCATENATED isn't used, the decoder will stop
    // after the first .xz stream. This can be useful when .xz data has
    // been embedded inside another file format.
    //
    // Flags other than LZMA_CONCATENATED are supported too, and can
    // be combined with bitwise-or. See lzma/container.h
    // (src/liblzma/api/lzma/container.h in the source package or e.g.
    // /usr/include/lzma/container.h depending on the install prefix)
    // for details.
    lzma_ret ret = lzma_stream_decoder(
            strm, UINT64_MAX, LZMA_CONCATENATED);

    // Return successfully if the initialization went fine.
    if (ret == LZMA_OK)
        return true;

    // Something went wrong. The possible errors are documented in
    // lzma/container.h (src/liblzma/api/lzma/container.h in the source
    // package or e.g. /usr/include/lzma/container.h depending on the
    // install prefix).
    //
    // Note that LZMA_MEMLIMIT_ERROR is never possible here. If you
    // specify a very tiny limit, the error will be delayed until
    // the first headers have been parsed by a call to lzma_code().
    const char *msg;
    switch (ret) {
    case LZMA_MEM_ERROR:
        msg = "Memory allocation failed";
        break;

    case LZMA_OPTIONS_ERROR:
        msg = "Unsupported decompressor flags";
        break;

    default:
        // This is most likely LZMA_PROG_ERROR indicating a bug in
        // this program or in liblzma. It is inconvenient to have a
        // separate error message for errors that should be impossible
        // to occur, but knowing the error code is important for
        // debugging. That's why it is good to print the error code
        // at least when there is no good error message to show.
        msg = "Unknown error, possibly a bug";
        break;
    }

    fprintf(stderr, "Error initializing the decoder: %s (error code %u)\n",
            msg, ret);
    return false;
}


template<class Func>
static bool
decompress(lzma_stream *strm, const char *inname, FILE *infile, std::string& buf, Func func)
{
    // When LZMA_CONCATENATED flag was used when initializing the decoder,
    // we need to tell lzma_code() when there will be no more input.
    // This is done by setting action to LZMA_FINISH instead of LZMA_RUN
    // in the same way as it is done when encoding.
    //
    // When LZMA_CONCATENATED isn't used, there is no need to use
    // LZMA_FINISH to tell when all the input has been read, but it
    // is still OK to use it if you want. When LZMA_CONCATENATED isn't
    // used, the decoder will stop after the first .xz stream. In that
    // case some unused data may be left in strm->next_in.
    lzma_action action = LZMA_RUN;

    uint8_t inbuf[BUFSIZ];
    uint8_t outbuf[BUFSIZ];

    strm->next_in = NULL;
    strm->avail_in = 0;
    strm->next_out = outbuf;
    strm->avail_out = sizeof(outbuf);

    while (true) {
        if (strm->avail_in == 0 && !feof(infile)) {
            strm->next_in = inbuf;
            strm->avail_in = fread(inbuf, 1, sizeof(inbuf),
                    infile);

            if (ferror(infile)) {
                fprintf(stderr, "%s: Read error: %s\n",
                        inname, strerror(errno));
                return false;
            }

            // Once the end of the input file has been reached,
            // we need to tell lzma_code() that no more input
            // will be coming. As said before, this isn't required
            // if the LZMA_CONATENATED flag isn't used when
            // initializing the decoder.
            if (feof(infile))
                action = LZMA_FINISH;
        }

        lzma_ret ret = lzma_code(strm, action);

        if (strm->avail_out == 0 || ret == LZMA_STREAM_END) {
            size_t write_size = sizeof(outbuf) - strm->avail_out;
            buf.append(reinterpret_cast<char*>(outbuf), write_size);
            strm->next_out = outbuf;
            strm->avail_out = sizeof(outbuf);
            func(buf, ret == LZMA_STREAM_END);
        }

        if (ret != LZMA_OK) {
            // Once everything has been decoded successfully, the
            // return value of lzma_code() will be LZMA_STREAM_END.
            //
            // It is important to check for LZMA_STREAM_END. Do not
            // assume that getting ret != LZMA_OK would mean that
            // everything has gone well or that when you aren't
            // getting more output it must have successfully
            // decoded everything.
            if (ret == LZMA_STREAM_END)
                return true;

            // It's not LZMA_OK nor LZMA_STREAM_END,
            // so it must be an error code. See lzma/base.h
            // (src/liblzma/api/lzma/base.h in the source package
            // or e.g. /usr/include/lzma/base.h depending on the
            // install prefix) for the list and documentation of
            // possible values. Many values listen in lzma_ret
            // enumeration aren't possible in this example, but
            // can be made possible by enabling memory usage limit
            // or adding flags to the decoder initialization.
            const char *msg;
            switch (ret) {
            case LZMA_MEM_ERROR:
                msg = "Memory allocation failed";
                break;

            case LZMA_FORMAT_ERROR:
                // .xz magic bytes weren't found.
                msg = "The input is not in the .xz format";
                break;

            case LZMA_OPTIONS_ERROR:
                // For example, the headers specify a filter
                // that isn't supported by this liblzma
                // version (or it hasn't been enabled when
                // building liblzma, but no-one sane does
                // that unless building liblzma for an
                // embedded system). Upgrading to a newer
                // liblzma might help.
                //
                // Note that it is unlikely that the file has
                // accidentally became corrupt if you get this
                // error. The integrity of the .xz headers is
                // always verified with a CRC32, so
                // unintentionally corrupt files can be
                // distinguished from unsupported files.
                msg = "Unsupported compression options";
                break;

            case LZMA_DATA_ERROR:
                msg = "Compressed file is corrupt";
                break;

            case LZMA_BUF_ERROR:
                // Typically this error means that a valid
                // file has got truncated, but it might also
                // be a damaged part in the file that makes
                // the decoder think the file is truncated.
                // If you prefer, you can use the same error
                // message for this as for LZMA_DATA_ERROR.
                msg = "Compressed file is truncated or "
                        "otherwise corrupt";
                break;

            default:
                // This is most likely LZMA_PROG_ERROR.
                msg = "Unknown error, possibly a bug";
                break;
            }

            fprintf(stderr, "%s: Decoder error: "
                    "%s (error code %u)\n",
                    inname, msg, ret);
            return false;
        }
    }
}
//can't use boost::algorithm::split as ',' can be contained in product ID.
//also simpler than boost::split thus faster
template<char seperator>
size_t internal_split(std::vector<std::string>& cols_, const std::string& line)
{
    size_t index = 0;
    if (index >= cols_.size()) cols_.push_back(std::string());
    std::string* col = &(cols_[index]);
    bool in_quote = false;
    for (auto c : line)
    {
        switch(c)
        {
        case seperator:
            if (in_quote) col->push_back(c);
            else
            {
                ++index;
                if (index >= cols_.size()) cols_.push_back(std::string());
                col = &(cols_[index]);
            }
            break;
        case '"':
            in_quote = !in_quote;
            break;//remove "
        default:
            col->push_back(c);
        }
    }
    return index + 1;
}

//check if it's nessary to do trim first. faster than calling boost trim directly.
void trim(std::string& str)
{
    if (str.empty()) return;
    if (*str.begin() != ' ' & *str.rbegin() != ' ') return;//this is probably 99.999999..% of the case.
    boost::algorithm::trim(str);
}

}//end namespace
namespace cxx_influx
{
//thread_local String_Pool CSVToInfluxMsg::_pool;
//thread_local std::vector<std::string> CSVToInfluxMsg::_columns;
//thread_local std::vector<std::string> CSVToInfluxMsg::_product_attributes;
CSVToInfluxMsg::CSVToInfluxMsg(uint32_t batch_count_)
    : _batch_count(batch_count_)
{
    _columns.resize(static_cast<uint8_t>(OldColumn::count));
    _product_attributes.resize(static_cast<uint8_t>(ProductAttr::count));
    static bool init = false;
    if (!init)
    {
        static std::mutex m;
        std::lock_guard<std::mutex> guard(m);
        if (init) return;
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::exch] = "exch";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::bid1] = "bid1";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::bid2] = "bid2";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::bid3] = "bid3";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::bid4] = "bid4";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::bid5] = "bid5";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::bidv1] = "bidv1";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::bidv2] = "bidv2";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::bidv3] = "bidv3";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::bidv4] = "bidv4";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::bidv5] = "bidv5";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::nbid1] = "nbid1";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::nbid2] = "nbid2";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::nbid3] = "nbid3";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::nbid4] = "nbid4";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::nbid5] = "nbid5";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::ask1] = "ask1";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::ask2] = "ask2";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::ask3] = "ask3";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::ask4] = "ask4";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::ask5] = "ask5";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::askv1] = "askv1";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::askv2] = "askv2";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::askv3] = "askv3";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::askv4] = "askv4";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::askv5] = "askv5";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::nask1] = "nask1";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::nask2] = "nask2";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::nask3] = "nask3";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::nask4] = "nask4";
        BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::nask5] = "nask5";
        init = true;
    }
}

void CSVToInfluxMsg::unzip(const std::string& file_name_)
{
    lzma_stream strm = LZMA_STREAM_INIT;

    bool success = true;

    if (!init_decoder(&strm)) 
    {
       return;
    }
    FILE *infile = fopen(file_name_.data(), "rb");
    if (infile == NULL)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Error opening " << file_name_.data() << ". " << strerror(errno);
        return;
    }
    std::string buf;
    decompress(&strm, file_name_.data(), infile, buf, [this](std::string& str, bool end)
                                                      {this->convert_decoded_string(str, end);});
    fclose(infile);
    lzma_end(&strm);
}

void CSVToInfluxMsg::generate_points(const TickFile& file_, const Msg_Handler& handler_)
{
    if (file_._reactor_source.empty())
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "No source for file " << file_._file_path.native();
        return;            
    }
    _source = file_._reactor_source;
    //ProfilerStart("./profile");
    _file = &file_;
    _msg_handler = handler_;
    std::string file_path(file_._file_path.native());
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Opened file " << file_path << " to generate influx data."
                                << "source : " << _source << " batch count : " << _batch_count;
    if (boost::algorithm::ends_with(file_path, ".xz"))
    {
        unzip(file_path);
    }
    else
    {
        std::fstream file(file_path, std::ios::in);
        if (!file)
        {
            CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Failed to open file : " << file_path;
            return;
        }
        std::string line;
        while (std::getline(file, line))
        {
            convert_one_line(line);
        }
    }
    //ProfilerStop();
    if (_builder.msg_count() > 0)
    {
        process_msg(true);
    }
    if (!_pending_columns.empty())
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << _pending_columns.size() << " of lines have no time info in " << _file->_file_path.native()
                             << ". thus can't be imported into influx, worth doing a check. some old mdrecorder files have no time info.";
    }
    
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "generated " << _trade_cnt << " trades. " 
                              << _book_cnt << " quotes from " << file_path;

}

void CSVToInfluxMsg::convert_decoded_string(std::string& str, bool end)
{
    std::string line;
    size_t offset = 0;
    size_t pos = str.find('\n');
    while (pos != std::string::npos)
    {
        line.append(str.data() + offset, pos - offset);
        convert_one_line(line);
        line.clear();
        offset = pos + 1;
        if (offset >= str.size()) break;
        pos = str.find('\n', offset);
    }

    if (end)
    {
        if (offset < str.size())
        {
            line.append(str.data() + offset, str.size() - offset);
            convert_one_line(line);
        }                            
    }
    else str.erase(0, offset);
}
void CSVToInfluxMsg::process_msg(bool last_)
{
    str_ptr str = _pool.get_str_ptr();
    _builder.get_influx_msg(*str);
    _builder.clear();
    Influx_Msg msg{_file->_file_path.filename().string(), _file->_date, last_, str};
    //Influx_Msg msg{_file->_date, str};
    _msg_handler(msg);
}
void CSVToInfluxMsg::process_pending_lines(const std::string& time_, const std::string& description_)
{
    _description = description_;
    for (auto& cols : _pending_columns)
    {
        cols[static_cast<uint8_t>(BookColumnIndex::recv)] = time_;
        if (cols[static_cast<uint8_t>(BookColumnIndex::otype)] == OTYPE_QUOTE)
        {
            convert_quote(cols);
        }
        else
        {
            convert_trade(cols);
        }
    }
    _pending_columns.clear();
    _description.clear();
}
bool CSVToInfluxMsg::invalid_recv_time(const std::string& time_, const std::string& line_)
{
    //time must be in nanoseconds
    //time must start with 1. if it starts with 2. it's at least 2033 May 18 11:33:20.
    if (time_.size() != 19 || time_[0] != '1')
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "invalid time found, either too small or too large. " << time_;
        return true;
    }
    return false;
}
bool CSVToInfluxMsg::set_recv_time(std::vector<std::string>& cols_, const std::string& line_)
{
    if (cols_[static_cast<uint8_t>(BookColumnIndex::recv)] == NA || invalid_recv_time(cols_[static_cast<uint8_t>(BookColumnIndex::recv)], line_))
    {
        //too many lines like this. make it trade not warning.
        CUSTOM_LOG(Log::logger(), logging::trivial::trace) << "line with invalid recv time info. "
                      << line_;
        if (!_current_recv_time.empty())
        {
            cols_[static_cast<uint8_t>(BookColumnIndex::recv)] = _current_recv_time;
            _description = "nr_use_prev_time";//no recv time, use the last valid recv time 
        }
        else if (cols_[static_cast<uint8_t>(BookColumnIndex::exch)] != NA)
        {
            process_pending_lines(cols_[static_cast<uint8_t>(BookColumnIndex::exch)], "nr_use_next_exch_time");       
            _description = "nr_use_exch_time";
            cols_[static_cast<uint8_t>(BookColumnIndex::recv)] = cols_[static_cast<uint8_t>(BookColumnIndex::exch)];
        }
        else //no exch time, no recv time. wait till a valid time come up
        {
            if (_no_recvtime_log_count++ < 10)
            {
                CUSTOM_LOG(Log::logger(), logging::trivial::error) << "line with invalid recv and exch time info. no valid recv time found in previous too. log 10 lines most to avoid spamming the log "
                          << line_;
            }
            _pending_columns.push_back(cols_);
            return false;                        
        }            
    }
    else 
    {
        _current_recv_time = cols_[static_cast<uint8_t>(BookColumnIndex::recv)];
        process_pending_lines(_current_recv_time, "nr_use_next_recv_time");
    }
    return true;
}
void CSVToInfluxMsg::convert_one_line(const std::string& line_)
{
    CUSTOM_LOG(Log::logger(), logging::trivial::trace) << "line : " << line_;
    _description.clear();
    _product_index_key.clear();
    std::vector<std::string>& columns = _columns;;
    for (auto& str : columns) str.clear();
    _cols_cnt = internal_split<','>(columns, line_);
    for (auto& col : columns)
    {
        trim(col);
    }
    if (columns[(uint8_t)BookColumnIndex::otype] == OTYPE_QUOTE) // quote
    {
        if (!set_recv_time(columns, line_)) return;
        convert_quote(columns);
    }
    else if (columns[(uint8_t)BookColumnIndex::otype] == OTYPE_TRADE_SUMMARY || columns[(uint8_t)BookColumnIndex::otype] == OTYPE_TRADE) // trade summary
    {
        if (!set_recv_time(columns, line_)) return;
        //check  /mnt/tank/backups/london/quants/data/rawdata/20160506/TNG-HKFE-QTG-Shim-A50-F-JUN2016-20160506-093423.csv.xz search for 'T,'
        // there are only
        convert_trade(columns);
    }
    else if (columns[(uint8_t)BookColumnIndex::otype] == OTYPE)
    {
        //header
        if (_cols_cnt >= static_cast<size_t>(OldColumn::count))
        {
            if (line_.find("market,type,prod") == std::string::npos)
            {
                CUSTOM_LOG(Log::logger(), logging::trivial::fatal) << "Unknown csv format. it should be old format, but cannot find market,type,prod in.treats it as new format. " << line_;       
            }
            else _old_format = true;
        }
    }
    else
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "unknown otype " << columns[(uint8_t)BookColumnIndex::otype] << " in " << line_;
    }

    if (_builder.msg_count() >= _batch_count)
    {
        process_msg();
    }
}

namespace 
{
//decimal strikes in reator uses comma instead of dot
//one example is SIM.O.GOOG.SEP2018.109,45.C.0
void replace_comma_with_dot_in_strike(std::string& str_)
{
    for (auto& c : str_)
    {
        if (c == ',')
        {
            c = '.';
            return;
        }
    }
}
}
void CSVToInfluxMsg::add_tags_old(std::vector<std::string>& cols_)
{
    //add product tag first so resendfailedmsg can work for both qtg tick and reactor tick
    _builder.add_tag(TAG_PRODUCT, cols_[(uint8_t)OldColumn::prod]);
    _builder.add_tag(TAG_TYPE, cols_[(uint8_t)OldColumn::type]);
    if (cols_[(uint8_t)OldColumn::series] != NA) _builder.add_tag(TAG_EXPIRY, cols_[(uint8_t)OldColumn::series]);
    if (cols_[(uint8_t)OldColumn::strike] != NA) 
    {
        replace_comma_with_dot_in_strike(cols_[(uint8_t)OldColumn::strike]);
        _builder.add_tag(TAG_STRIKE, cols_[(uint8_t)OldColumn::strike]);    
    }
    if (cols_[(uint8_t)OldColumn::call_put] != NA) _builder.add_tag(TAG_CALLPUT, cols_[(uint8_t)OldColumn::call_put]);    
    if (cols_[static_cast<uint8_t>(OldColumn::version)] != NA) _builder.add_tag(TAG_VERSION, cols_[static_cast<uint8_t>(OldColumn::version)]);
}

uint32_t CSVToInfluxMsg::get_index(const std::vector<std::string>& cols_, Recv_Time_Index& time_index_)
{
    _product_index_key.append(cols_[static_cast<uint8_t>(BookColumnIndex::recv)]);
    uint32_t& index = time_index_[_product_index_key];
    index++;
    if (index > 1)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::trace) << "recv time " << cols_[(uint8_t)BookColumnIndex::recv]
             << " in " << _file->_file_path.native() << " appears " << index << " time(s) for the same product.";
    }
    return index;
}

void CSVToInfluxMsg::add_tags_new(std::vector<std::string>& cols_)
{
    std::vector<std::string>& product_attributes = _product_attributes;
    for (auto& str : product_attributes) str.clear();

    uint8_t product_index = static_cast<uint8_t>(NewColumn::product);
    if (_cols_cnt < static_cast<size_t>(BookColumnIndex::count))
    {
        std::ostringstream os;
        for (auto& str : cols_) os << str << ',';
        CUSTOM_LOG(Log::logger(), logging::trivial::warning) << "too few columns, some old mdrecorder files use fewer columns for trade report." << os.str();
        //check /mnt/tank/backups/london/quants/data/rawdata/20160506/TNG-HKFE-QTG-Shim-A50-F-JUN2016-20160506-093423.csv.xz search for 'T'
        //there are only 28 columns for trade and the last column is used to store "product id"
        product_index = 27;
    }
    size_t size = internal_split<'.'>(product_attributes, cols_[product_index]);
    //add product tag first so resendfailedmsg can work for both qtg tick and reactor tick
    if (size > (uint8_t)ProductAttr::product)
        _builder.add_tag(TAG_PRODUCT, product_attributes[(uint8_t)ProductAttr::product]);
    if (size > (uint8_t)ProductAttr::type)
        _builder.add_tag(TAG_TYPE, product_attributes[(uint8_t)ProductAttr::type]);
    if (size > (uint8_t)ProductAttr::expiry)
        _builder.add_tag(TAG_EXPIRY, product_attributes[(uint8_t)ProductAttr::expiry]);
    if (size > (uint8_t)ProductAttr::strike)
    {
        replace_comma_with_dot_in_strike(product_attributes[(uint8_t)ProductAttr::strike]); 
        _builder.add_tag(TAG_STRIKE, product_attributes[(uint8_t)ProductAttr::strike]);
        _product_index_key.append(product_attributes[(uint8_t)ProductAttr::strike]);
    }
    if (size > (uint8_t)ProductAttr::call_put)
    {
        _builder.add_tag(TAG_CALLPUT, product_attributes[(uint8_t)ProductAttr::call_put]);
        _product_index_key.append(product_attributes[(uint8_t)ProductAttr::call_put]);
    }
    if (size > static_cast<uint8_t>(ProductAttr::version))
    {
        _builder.add_tag(TAG_VERSION, product_attributes[static_cast<uint8_t>(ProductAttr::version)]);
        _product_index_key.append(product_attributes[static_cast<uint8_t>(ProductAttr::version)]);
    }
}
void CSVToInfluxMsg::add_common_tags(std::vector<std::string>& cols_, Recv_Time_Index& time_index_)
{
    if (_old_format)
    {
        add_tags_old(cols_);
    }
    else
    {
        add_tags_new(cols_);
    }
    _builder.add_tag(TAG_INDEX, get_index(cols_, time_index_));
    _builder.add_tag(TAG_SOURCE, _source);
}
void CSVToInfluxMsg::add_network_ts(const std::vector<std::string>& cols_)
{
    if (!_old_format)
    {
        if (cols_.size() >= static_cast<size_t>(NewColumn::count))
        {
            const std::string& str = cols_[static_cast<size_t>(NewColumn::nicts)];
            if (!str.empty() && str != "0" && str != NA)
            {
                _builder.add_int_field(NETWORK_TS, str);
            }
        }
    }
}
void CSVToInfluxMsg::add_int_field(const std::string& key_, const std::string& value_)
{
    if (value_.empty() | value_ == NA) return;
    _builder.add_int_field(key_, value_);
}

void CSVToInfluxMsg::add_float_field(const std::string& key_, const std::string& value_)
{
    if (value_.empty() | value_ == NA) return;
    _builder.add_float_field(key_, value_);
}


void CSVToInfluxMsg::add_field(const std::string& key_, const std::string& value_)
{
    if (value_.empty() | value_ == NA) return;
    _builder.add_field(key_, value_);
}
void CSVToInfluxMsg::convert_trade(std::vector<std::string>& cols_)
{
    _trade_cnt++;
    _builder.point_begin(MEASUREMENT_TRADE);
    add_common_tags(cols_, _trade_recv_time_index);
    //side is a key for trade.
    _builder.add_tag(TAG_SIDE, cols_[(uint8_t)TradeColumnIndex::side]);
    add_int_field(BOOK_FIELD_ARRAY[static_cast<uint8_t>(BookColumnIndex::exch)], cols_[static_cast<uint8_t>(BookColumnIndex::exch)]);
    add_network_ts(cols_); 
    add_float_field(TRADE_PRICE, cols_[(uint8_t)TradeColumnIndex::price]);    
    add_float_field(TRADE_QTY, cols_[(uint8_t)TradeColumnIndex::qty]); //use float for volume in case there are decimals in cash products' volume.
    add_field(OTYPE, cols_[static_cast<uint8_t>(BookColumnIndex::otype)]);   
    add_field(DESCRIPTION, _description);
    _builder.point_end(cols_[(uint8_t)BookColumnIndex::recv]);    
}

void CSVToInfluxMsg::convert_quote(std::vector<std::string>& cols_)
{
    _book_cnt++;
    _builder.point_begin(MEASUREMENT_BOOK);
    add_common_tags(cols_, _quote_recv_time_index);
    add_int_field(BOOK_FIELD_ARRAY[(uint8_t)BookColumnIndex::exch], cols_[(uint8_t)BookColumnIndex::exch]);
    add_network_ts(cols_); 
    for (size_t i = (uint8_t)BookColumnIndex::bid1; i < (uint8_t)BookColumnIndex::count; ++i)
    {
        if (cols_[i].empty() | cols_[i] == NA) continue;

        if ((i >= (uint8_t)BookColumnIndex::nbid1 && i <= (uint8_t)BookColumnIndex::nbid5) 
         || (i >= (uint8_t)BookColumnIndex::nask1 && i <= (uint8_t)BookColumnIndex::nask5))
        {
            _builder.add_int_field(BOOK_FIELD_ARRAY[i], cols_[i]);
        }
        else _builder.add_float_field(BOOK_FIELD_ARRAY[i], cols_[i]);//use float for volume in case there are decimals in cash products' volume.
    }              
    add_field(OTYPE, cols_[static_cast<uint8_t>(BookColumnIndex::otype)]);    
    add_field(DESCRIPTION, _description);
    _builder.point_end(cols_[(uint8_t)BookColumnIndex::recv]);
}

}
