from enum import Enum

import pytz


class StrEnum(str, Enum):
    def __str__(self):
        return self._value_

    @classmethod
    def values(cls):
        return [v.value for _, v in cls.__members__.items()]


class BaseTable(object):
    @classmethod
    def name(cls):
        return cls.__name__


class EnrichedOHLCVN(BaseTable):

    class Fields(StrEnum):
        OPEN = 'open'
        CLOSE = 'close'

        HIGH = 'high'
        HASK = 'hask'
        HBID = 'hbid'

        LOW = 'low'
        LASK = 'lask'
        LBID = 'lbid'

        CASK1 = 'cask1'
        CASK2 = 'cask2'
        CASK3 = 'cask3'
        CASKV1 = 'caskv1'
        CASKV2 = 'caskv2'
        CASKV3 = 'caskv3'

        CBID1 = 'cbid1'
        CBID2 = 'cbid2'
        CBID3 = 'cbid3'
        CBIDV1 = 'cbidv1'
        CBIDV2 = 'cbidv2'
        CBIDV3 = 'cbidv3'

        TBUYC = 'tbuyc'
        TSELLC = 'tsellc'

        TBUYV = 'tbuyv'
        TSELLV = 'tsellv'

        TBUYVWAP = 'tbuyvwap'
        TSELLVWAP = 'tsellvwap'
        TWABMP = 'twabmp'

        NET_VOLUME = 'net_volume'
        VOLUME = 'volume'

        TIME = 'time'
        SOFTWARE_TIME = 'software_time'
        TRIGGER_TIME = 'trigger_time'
        EXCH_TIME = 'exch_time'

    class Tags(StrEnum):
        PRODUCT = 'product'
        TYPE = 'type'
        EXPIRY = 'expiry'

        CLOCK_TYPE = 'clock_type'
        WIDTH = 'width'
        OFFSET = 'offset'


class ContinuousContract(BaseTable):
    @classmethod
    def name(cls):
        return 'continuous_contract'

    class Fields(StrEnum):
        SHORT_CODE = 'short_code'
        TIME_ZONE = 'time_zone'

    class Tags(StrEnum):
        EXPIRY = 'expiry'
        PRODUCT = 'product'
        ROLL_STRATEGY = 'roll_strategy'
        TYPE = 'type'


class Basedb(object):
    HOSTNAME = None

    USERNAME = 'root'
    PASSWORD = 'root123'

    UNDEFINED = 999999999998


class Quantdb1(Basedb):
    HOSTNAME = 'lcldn-quantdb1'
    PORT = 8086

    class BarDatabase(object):
        DBNAME = 'bar'
        TABLES = {EnrichedOHLCVN.name(): EnrichedOHLCVN()}


class Quantsim1(Basedb):
    HOSTNAME = 'lcmint-quantsim1'
    PORT = 8086

    class BarDatabase(object):
        DBNAME = 'bar_data'
        TABLES = {EnrichedOHLCVN.name(): EnrichedOHLCVN(),
                  ContinuousContract.name(): ContinuousContract()}


class Lcmquantldn1(Basedb):
    HOSTNAME = 'lcmquantldn1'

    class FileConfig(BaseTable):
        BASEDIR = '/opt/data'

        SOURCE = 'source'
        TABLE = 'table'
        YEAR = 'year'
        DATA_FILE = 'data_file'

        TIMEZONE = pytz.UTC
        TIME_COL_IDX = 0
        SEPARATOR = ','
        FILE_STRUCTURE = []

        class SourceNames(StrEnum):
            cme_qtg = 'cme_qtg_bar',
            cme_reactor = 'cme_reactor_bar_edge_new',
            china_reactor = 'china_reactor_bar',
            eurex_reactor = 'eurex_reactor_bar',
            ose_reactor = 'ose_reactor_bar',
            asx_reactor = 'asx_reactor'

    class EnrichedOHLCVN(EnrichedOHLCVN, FileConfig):
        FILENAME_DATE_PATTERN = '[0-9]{8}'
        FILENAME_DATE_FORMAT = '%Y%m%d'

        def __init__(self):
            self.FILE_STRUCTURE = [
                self.SOURCE,
                self.Tags.TYPE,
                self.Tags.PRODUCT,
                self.Tags.EXPIRY,
                self.TABLE,
                self.Tags.CLOCK_TYPE,
                self.Tags.WIDTH,
                self.YEAR,
                self.DATA_FILE
            ]

    class ContinuousContract(ContinuousContract, FileConfig):
        def __init__(self):
            self.FILE_STRUCTURE = [
                self.SOURCE,
                self.Tags.TYPE,
                self.Tags.PRODUCT,
                self.DATA_FILE
            ]

            self.FILENAME_STRUCTURE = [self.Tags.PRODUCT]
            self.FILENAME_FORMAT = '{}-continuous_contract.csv.gz'

        class Fields(StrEnum):
            SHORT_CODE = 'short_code'
            TIME_ZONE = 'time_zone'
            EXPIRY = 'expiry'

        class Tags(StrEnum):
            PRODUCT = 'product'
            ROLL_STRATEGY = 'roll_strategy'
            TYPE = 'type'

    TABLES = {EnrichedOHLCVN.name(): EnrichedOHLCVN(),
              ContinuousContract.name(): ContinuousContract()}

