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


class EnrichedOHLCVN(object):
    @classmethod
    def name(cls):
        return 'EnrichedOHLCVN'

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


class ContinuousContract(object):
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
    DBNAME = 'bar'

    ENRICHEDOHLCVN = EnrichedOHLCVN.name()
    TABLES = {ENRICHEDOHLCVN: EnrichedOHLCVN()}


class Quantsim1(Basedb):
    HOSTNAME = 'lcmint-quantsim1'
    PORT = 8086
    DBNAME = 'bar_data'

    ENRICHEDOHLCVN = EnrichedOHLCVN.name()
    CONTINUOUS_CONTRACT = ContinuousContract.name()

    TABLES = {ENRICHEDOHLCVN: EnrichedOHLCVN(),
              CONTINUOUS_CONTRACT: ContinuousContract()}


class Lcmquantldn1(Basedb):
    HOSTNAME = 'lcmquantldn1'

    class FileConfig(BaseTable):
        BASEDIR = '/opt/data'

        SOURCE = 'source'
        GZIPS = 'gzips'
        REACTOR_GZIPS = 'reactor_gzips'

        TABLE = 'table'
        YEAR = 'year'
        DATA_FILE = 'data_file'

        TIMEZONE = pytz.UTC

        FILE_STRUCTURE = []

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
            self.FILENAME_FORMAT = '{}-' + self.name() + '.csv.gz'

    ENRICHEDOHLCVN = EnrichedOHLCVN.name()
    CONTINUOUS_CONTRACT = ContinuousContract.name()

    TABLES = {ENRICHEDOHLCVN: EnrichedOHLCVN(),
              CONTINUOUS_CONTRACT: ContinuousContract()}


dbbox_configs = {'quantdb1': Quantdb1,
                 'quantsim1': Quantsim1,
                 'lcmquantldn1': Lcmquantldn1}
