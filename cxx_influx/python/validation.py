import lzma
import sys
import csv
import requests
import json
from enum import Enum
from datetime import datetime
class CommonColumn(Enum):
    otype,recv,exch,bid1,bid2,bid3,bid4,bid5,bidv1,bidv2,bidv3,bidv4,bidv5,nbid1,nbid2,nbid3,nbid4,nbid5,ask1,ask2,ask3,ask4\
    ,ask5,askv1,askv2,askv3,askv4,askv5,nask1,nask2,nask3,nask4,nask5,count=range(34)
class Trade_SummaryColumn(Enum):
    price,volume,side,count=range(CommonColumn.bid1.value, CommonColumn.bid1.value + 4)

class OldColumn(Enum):
    market,type,prod,series,strike,call_put,version,count=range(CommonColumn.count.value, CommonColumn.count.value + 8)
class NewColumn(Enum):
    product,state,nicts,count=range(CommonColumn.count.value, CommonColumn.count.value + 4)
class TradeColumn(Enum):
    time, cp, exch, expiry, index, nicts, otype, price, product, side, source, strike, type, volume=range(14)
class BookColumn(Enum):
    time,ask1,ask2,ask3,ask4,ask5,askv1,askv2,askv3,askv4,askv5,bid1,bid2,bid3,bid4,bid5,bidv1,bidv2,bidv3,bidv4,bidv5,cp,exch \
    ,expiry,index,version,nask1,nask2,nask3,nask4,nask5,nbid1,nbid2,nbid3,nbid4,nbid5,nicts,otype,product,source\
    ,desc,strike,type=range(43)
class RowData:
    def get_key(self):
        return self._cols[CommonColumn.recv.value]
    def __init__(self, cols_, desc_):
        self._cols = cols_
        self._desc = desc_

class FixedListForRowData(list):
    def __init__(self, max_len_):
        self._max_len = max_len_
        self._sorted = False
    def append(self, row_data_):
        if len(self) >= self._max_len:
            if not self._sorted:#sort only once.
                self._sorted = True
                sorted(self, key = RowData.get_key)
            if row_data_.get_key() >= self[-1].get_key():
                return #bigger than all elements, no need to insert
            list.append(self, row_data_)
            sorted(self, key = RowData.get_key)
            self[self._max_len - len(self):] = []
        else:
            list.append(self, row_data_)#it's quite slow to do sort here multiple times.


class ValidateInfluxData:
    NA = "NA"
    Trade = "T"
    Trade_Summary = "S"
    Depth = "Q"
    Trade_Measurement = "trade"
    Book_Measurement = "book"
    _http_host = ""
    _http_port = 0
    _influx_db = ""
    _max_book_number = 0
    _max_trade_number = 0
    def __init__(self, file_name_):
        self._file_name = file_name_
        if self._max_book_number > 0:
            self._book_columns = FixedListForRowData(self._max_book_number)
        else:
            self._book_columns = []
        if self._max_trade_number > 0:
            self._trade_columns = FixedListForRowData(self._max_trade_number)
        else:
            self._trade_columns = []
        self._old_format = False
        self._product_type = ''
        self._product_name = ''
        self._expiry = None
        self._index = 0
        self._pending_columns = []
        self._current_recv_time = ''
    def parse(self, file_name_):
        print(str(datetime.now()), "Start parsing mdrecorder file", self._file_name)
        f = lzma.open(file_name_)
        for line in f:
            self.split(line.decode('utf8'))
        self._trade_columns = sorted(self._trade_columns, key=RowData.get_key)
        self._book_columns = sorted(self._book_columns, key=RowData.get_key)
        if len(self._book_columns) == 0:
            print(str(datetime.now()), "No valid book found")
            return
        if self._old_format:
            self._product_type = self._book_columns[0]._cols[OldColumn.type.value]
            self._product_name = self._book_columns[0]._cols[OldColumn.prod.value]
            if self._book_columns[0]._cols[OldColumn.series.value] != ValidateInfluxData.NA:
                self._expiry = self._book_columns[0]._cols[OldColumn.series.value]
        else:
            product_attr = self._book_columns[0]._cols[self.get_product_index_in_new_format(self._book_columns[0]._cols)].split('.')
            self._product_type = product_attr[1] #PROD.O.GOOG.SEP2018.3000.C.0
            self._product_name = product_attr[2]
            if len(product_attr) > 3:
                self._expiry = product_attr[3]
        print(str(datetime.now()), "Finished parsing mdrecorder file", self._file_name)
    def run(self):
        self.parse(self._file_name)
        if len(self._trade_columns) > 0:
            trade_begin = self._trade_columns[0]._cols[CommonColumn.recv.value]
            trade_end = self._trade_columns[-1]._cols[CommonColumn.recv.value]
            self.compare_with_influx(trade_begin, trade_end, ValidateInfluxData.Trade_Measurement)
        if len(self._book_columns) > 0:
            book_begin = self._book_columns[0]._cols[CommonColumn.recv.value]
            book_end = self._book_columns[-1]._cols[CommonColumn.recv.value]
            self.compare_with_influx(book_begin, book_end, ValidateInfluxData.Book_Measurement)
    def get_product_index_in_new_format(self, cols_):
        if len(cols_) < CommonColumn.count.value:
            #check /mnt/tank/backups/london/quants/data/rawdata/20160506/TNG-HKFE-QTG-Shim-A50-F-JUN2016-20160506-093423.csv.xz
            #search for 'T'
            #there are only 28 columns for trade and the last column is used to store "product id"
            return 27
        else:
            return NewColumn.product.value
                
    def get_key(self, row_data_):
        return row_data_._cols[CommonColumn.recv.value]
    def process_pending_columns(self, time_, desc_):
        for cols in self._pending_columns:
            cols[CommonColumn.recv.value] = time_
            if cols[CommonColumn.otype.value] == ValidateInfluxData.Trade or cols[CommonColumn.otype.value] == ValidateInfluxData.Trade_Summary:
                self._trade_columns.append(RowData(cols, desc_))
            elif cols[CommonColumn.otype.value] == ValidateInfluxData.Depth:
                self._book_columns.append(RowData(cols, desc_))
        self._pending_columns = []
    def invalid_recv_time(self, time_):
        #time should be in nanoseconds. and if it starts with 2. it's at least in 2033.
        return len(time_) != 19 or time_[0] != '1';
    def split(self, line_):
        cols = list(csv.reader([line_]))[0]
        cols = [col.strip() for col in cols] #trim spaces
        if cols[CommonColumn.otype.value] == CommonColumn.otype.name:
            if len(cols) >= OldColumn.count.value:
                self._old_format = True
            return #header
        description = ''
        if cols[CommonColumn.recv.value] == ValidateInfluxData.NA or self.invalid_recv_time(cols[CommonColumn.recv.value]): 
            if self._current_recv_time != '':
                description = 'nr_use_prev_time'
                cols[CommonColumn.recv.value] = self._current_recv_time
            elif cols[CommonColumn.exch.value] != ValidateInfluxData.NA:
                self.process_pending_columns(cols[CommonColumn.exch.value], "nr_use_next_exch_time")
                description = 'nr_use_exch_time'
                cols[CommonColumn.recv.value] = cols[CommonColumn.exch.value]
            else:
                self._pending_columns.append(cols)
                return
        else:
            self.process_pending_columns(cols[CommonColumn.recv.value], 'nr_use_next_recv_time')
            self._current_recv_time = cols[CommonColumn.recv.value]
        if not self._old_format:
            cols[self.get_product_index_in_new_format(cols)] = cols[self.get_product_index_in_new_format(cols)].replace('"', '')#product id is double quoted in csv file

        if cols[CommonColumn.otype.value] == ValidateInfluxData.Trade or cols[CommonColumn.otype.value] == ValidateInfluxData.Trade_Summary:
            self._trade_columns.append(RowData(cols, description))
        elif cols[CommonColumn.otype.value] == ValidateInfluxData.Depth:
            self._book_columns.append(RowData(cols, description))
    def compare_with_influx(self, begin_, end_, measurement):
        statement = "select * from {} where type = '{}' and product = '{}' and time >= {} and time <= {}"  \
                    .format(measurement, self._product_type, self._product_name, begin_, end_)
        if self._expiry is not None:
            statement += " and expiry = '{}'".format(self._expiry)
#        if measurement == ValidateInfluxData.Book_Measurement:
#            statement += " limit 10"
        print(str(datetime.now()), "begin to query from influx db.", statement)
        r = requests.get("http://{}:{}/query?epoch=ns&db={}&pretty=true&q={}".format(self._http_host, self._http_port, self._influx_db, statement)) 
        if (r.status_code != 200):
            print(statement)
            print("Failed to query db", r.status_code, r.reason)
            quit()
        print(str(datetime.now()), "Finished querying influx db.")
        print(str(datetime.now()), "Begin to parse queried results into json")
        result = json.loads(r.text)
        print(str(datetime.now()), "Finished parsing queried results into json")
        try:
            influx_columns = result["results"][0]["series"][0]["columns"]
            #converts list to dict with key = column name, value = list index
            influx_columns = {k : v for v, k in enumerate(influx_columns)};
            influx_values = result["results"][0]["series"][0]["values"] 
        except KeyError as err:
            print("No data found in db. the data is probably not imported. {}".format(err))
            return
        #both book and trade measurements have time and index fields. so it's ok to use BookColumn here
        recv_index = influx_columns[BookColumn.time.name]
        tag_index = influx_columns[BookColumn.index.name]
        influx_values = sorted(influx_values, key = lambda value_ : (value_[recv_index], int(value_[tag_index])))
        if measurement == ValidateInfluxData.Book_Measurement:
            self.compare_books(influx_values, influx_columns)
        else:
           self.compare_trades(influx_values, influx_columns)
    def compare_description(self, desc_, values_in_influx_, influx_column_index_):
            desc_in_influx = None
            if BookColumn.desc.name in influx_column_index_:
                desc_in_influx = values_in_influx_[influx_column_index_[BookColumn.desc.name]]
            if desc_ == '':
                if desc_in_influx is not None:
                    print("Description should be empty. but it has value {} in influx. index {}".format(desc_in_influx, self._index))
                    quit()
            else:
                if desc_in_influx != desc_:
                    print("Description is {} in file {}. index : {}. but {} in influx."\
                         .format(desc_, self._file_name, self._index, desc_in_influx))
                    quit()
    def compare_books(self, book_in_influx_, influx_book_column_):
        print(str(datetime.now()), "Start comparing books in file with books in influx.")
        if self._max_book_number != 0:
            if len(self._book_columns) > len(book_in_influx_):
                 print("there are {} lines for quote in file {} but {} lines in influx db." \
                 .format(len(self._book_columns), self._file_name, len(book_in_influx_)))
        elif len(self._book_columns) != len(book_in_influx_):
            print("there are {} lines for quote in file {} but {} lines in influx db" \
                 .format(len(self._book_columns), self._file_name, len(book_in_influx_)))             
            quit()
        for index in range(len(self._book_columns)):
            self._index = index
            self.compare_book(self._book_columns[index]._cols, book_in_influx_[index], influx_book_column_)
            self.compare_description(self._book_columns[index]._desc, book_in_influx_[index], influx_book_column_)
            
        print(str(datetime.now()), "Finished comparing {} books in file with books in influx. no mismatch found.".format(len(self._book_columns)))
    #this funtion checks if a column exsits in influx first before doing comparison
    def compare_column_ck(self, value_in_file_, values_in_influx_, influx_column_index_, influx_column_name_, col_name_, check_na_):
        value_in_influx = None
        if influx_column_name_ in influx_column_index_:
            value_in_influx = values_in_influx_[influx_column_index_[influx_column_name_]]
        self.compare_one_column(value_in_file_, value_in_influx, col_name_, check_na_)
    #this function compares column value directly
    def compare_one_column(self, value_in_file_, value_in_influx_, col_name_, check_na_):
        exit = False
        if value_in_file_ != str(value_in_influx_):
            if check_na_:
                if value_in_file_ != ValidateInfluxData.NA or value_in_influx_ is not None:
                    exit = True
            else:
                exit = True
        if exit:
            print("value in file {} for column {} is {}, index : {}, in influx is {}."\
                 .format(self._file_name, col_name_, value_in_file_, self._index, value_in_influx_))
            quit()
    def compare_book(self, book_in_file_, book_in_influx_, influx_book_columns_):
        for c in CommonColumn:
            if c == CommonColumn.recv:
                continue
            if c == CommonColumn.count:
                break
            self.compare_column_ck(book_in_file_[c.value], book_in_influx_, influx_book_columns_, c.name, c.name, check_na_=True)
        self.compare_column_ck(book_in_file_[CommonColumn.recv.value], book_in_influx_, influx_book_columns_, BookColumn.time.name, CommonColumn.recv.name, False)
        if self._old_format:
            self.compare_old_product(book_in_file_, book_in_influx_, influx_book_columns_)
        else:
            self.compare_new_book(book_in_file_, book_in_influx_, influx_book_columns_)
    def compare_old_product(self, product_in_file_, product_in_influx_, product_keys_):
        self.compare_column_ck(product_in_file_[OldColumn.type.value], product_in_influx_, product_keys_, BookColumn.type.name, OldColumn.type.name, False)
        self.compare_column_ck(product_in_file_[OldColumn.prod.value], product_in_influx_, product_keys_, BookColumn.product.name, OldColumn.prod.name, False)
        self.compare_column_ck(product_in_file_[OldColumn.series.value], product_in_influx_, product_keys_, BookColumn.expiry.name, OldColumn.series.name, True)
        strike = product_in_influx_[product_keys_[BookColumn.strike.name]]
        if strike is not None:
            strike = strike.replace('.', ',')#comma is used for dot in csv
        self.compare_one_column(product_in_file_[OldColumn.strike.value], strike, OldColumn.strike.name, True)
        self.compare_column_ck(product_in_file_[OldColumn.call_put.value], product_in_influx_, product_keys_, BookColumn.cp.name, OldColumn.call_put.name, True)
        self.compare_column_ck(product_in_file_[OldColumn.version.value], product_in_influx_, product_keys_, BookColumn.version.name, OldColumn.version.name, True)

    def compare_new_book(self, book_in_file_, book_in_influx_, influx_book_columns_):
        product_id = 'PROD.{}.{}'.format(book_in_influx_[influx_book_columns_[BookColumn.type.name]], book_in_influx_[influx_book_columns_[BookColumn.product.name]])
        expiry = book_in_influx_[influx_book_columns_[BookColumn.expiry.name]]
        if expiry is not None:
            product_id += '.' + expiry
        strike = book_in_influx_[influx_book_columns_[BookColumn.strike.name]]
        if strike is not None:
            product_id += '.' + strike.replace('.', ',')#csv uses comma for dot.
        call_put = book_in_influx_[influx_book_columns_[BookColumn.cp.name]]
        if call_put is not None:
            product_id += '.' + call_put
        product_in_file = book_in_file_[self.get_product_index_in_new_format(book_in_file_)]
        version = None
        if BookColumn.version.name in influx_book_columns_:
            version = book_in_influx_[influx_book_columns_[BookColumn.version.name]]
        if version is not None:
            product_id += '.' + version
        if product_in_file != product_id:
            print("product in file {} is {}, index : {}, in influx is {}"\
                 .format(self._file_name, product_in_file, self._index, product_id))
            quit()
        nicts = '0'
        if len(book_in_file_) >= NewColumn.count.value:
            nicts = book_in_file_[NewColumn.nicts.value]#nicts could be 0 or NA in csv
        if nicts == '0':
            nicts = 'NA'
        self.compare_column_ck(nicts, book_in_influx_, influx_book_columns_, BookColumn.nicts.name, NewColumn.nicts.name, True)
    def compare_trades(self, trades_in_influx_, influx_trade_columns_):
        print(str(datetime.now()), "Start comparing trades in file with trades in influx.")
        if self._max_trade_number != 0:
            if len(self._trade_columns) > len(trades_in_influx_):
                 print("there are {} lines for treade in file {} but {} lines in influx db." \
                 .format(len(self._book_columns), self._file_name, len(book_in_influx_)))
        elif len(self._trade_columns) != len(trades_in_influx_):
            print("there are {} lines for trade in file {} but {} lines in influx db".format(len(self._trade_columns), self._file_name, len(trades_in_influx_)))
            quit()
        for index in range(len(self._trade_columns)):
            self._index = index
            self.compare_trade(self._trade_columns[index]._cols, trades_in_influx_[index], influx_trade_columns_)
            self.compare_description(self._trade_columns[index]._desc, trades_in_influx_[index], influx_trade_columns_)
        print(str(datetime.now()), "Finished comparing {} trades in file with trades in influx. no mismatch found.".format(len(self._trade_columns)))
        
    def compare_trade(self, trade_in_file_, trade_in_influx_, influx_trade_columns_):
        self.compare_column_ck(trade_in_file_[CommonColumn.otype.value], trade_in_influx_, influx_trade_columns_, TradeColumn.otype.name, CommonColumn.otype.name, True)
        self.compare_column_ck(trade_in_file_[CommonColumn.recv.value], trade_in_influx_, influx_trade_columns_, TradeColumn.time.name, CommonColumn.recv.name, False)
        self.compare_column_ck(trade_in_file_[CommonColumn.exch.value], trade_in_influx_, influx_trade_columns_, TradeColumn.exch.name, CommonColumn.exch.name, True)
        self.compare_column_ck(trade_in_file_[Trade_SummaryColumn.price.value], trade_in_influx_, influx_trade_columns_, TradeColumn.price.name, Trade_SummaryColumn.price.name, True)
        self.compare_column_ck(trade_in_file_[Trade_SummaryColumn.volume.value], trade_in_influx_, influx_trade_columns_, TradeColumn.volume.name, Trade_SummaryColumn.volume.name, True)
        self.compare_column_ck(trade_in_file_[Trade_SummaryColumn.side.value], trade_in_influx_, influx_trade_columns_, TradeColumn.side.name, Trade_SummaryColumn.side.name, True)
        if self._old_format:
            self.compare_old_product(trade_in_file_, trade_in_influx_, influx_trade_columns_)
        else:
            self.compare_new_book(trade_in_file_, trade_in_influx_, influx_trade_columns_)

if len(sys.argv) < 5:
    print("usage: python3 validation.py http_host http_port influx_db <files that contain paths to multiple mdrecorder_files, one line per file> <compare at most N of books > <compare at most N of trades>\n\
                  python3 validation.py http_host http_port influx_db -s mdrecorder_file <compare at most N of books > <compare at most N of trades>")
    quit()
ValidateInfluxData._http_host = sys.argv[1]
ValidateInfluxData._http_port = int(sys.argv[2])
ValidateInfluxData._influx_db = sys.argv[3]
arg4 = sys.argv[4]
book_count_index = 5
if arg4 == "-s":
    book_count_index += 1
if len(sys.argv) > book_count_index:
    ValidateInfluxData._max_book_number = int(sys.argv[book_count_index])
if len(sys.argv) > book_count_index + 1:
    ValidateInfluxData._max_trade_number = int(sys.argv[book_count_index + 1])
if arg4 == "-s":
    validate = ValidateInfluxData(sys.argv[5])
    validate.run()
    quit()

with open(arg4) as f:
    for line in f:
        validate = ValidateInfluxData(line.strip())
        validate.run()

