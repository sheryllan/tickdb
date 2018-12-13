from enum import Enum
import re
import datetime as dt
import pytz
from os.path import basename
from pandas import read_csv, to_datetime



class StrEnum(str, Enum):
    def __str__(self):
        return self._value_

    @classmethod
    def values(cls):
        return [v.value for _, v in cls.__members__.items()]


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
    UNDEFINED = 999999999998
    ENRICHEDOHLCVN = EnrichedOHLCVN.name()

    TABLE = 'table'
    TABLES = {ENRICHEDOHLCVN: EnrichedOHLCVN}


class Quantdb1(Basedb):
    DBNAME = 'bar'

    USERNAME = 'root'
    PASSWORD = 'root123'

    HOSTNAME = 'lcldn-quantdb1'
    PORT = 8086


class Quantsim1(Basedb):
    DBNAME = 'bar_data'

    USERNAME = 'root'
    PASSWORD = 'root123'

    HOSTNAME = 'lcmint-quantsim1'
    PORT = 8086

    ENRICHEDOHLCVN = EnrichedOHLCVN.name()
    CONTINUOUS_CONTRACT = ContinuousContract.name()

    TABLES = {ENRICHEDOHLCVN: EnrichedOHLCVN,
              CONTINUOUS_CONTRACT: ContinuousContract}


class Lcmquantldn1(Basedb):
    BASEDIR = '/opt/data'

    HOSTNAME = 'lcmquantldn1'

    class EnrichedOHLCVN(EnrichedOHLCVN):
        YEAR = 'year'

        FILE_STRUCTURE = [
            EnrichedOHLCVN.Tags.TYPE,
            EnrichedOHLCVN.Tags.PRODUCT,
            EnrichedOHLCVN.Tags.EXPIRY,
            Basedb.TABLE,
            EnrichedOHLCVN.Tags.CLOCK_TYPE,
            EnrichedOHLCVN.Tags.WIDTH,
            YEAR]

        DATE_FMT = '%Y%m%d'
        TIMEZONE = pytz.UTC

        @classmethod
        def date_from_filename(cls, fn):
            fn = basename(fn)
            return cls.TIMEZONE.localize(dt.datetime.strptime(re.search('[0-9]{8}', fn).group(), cls.DATE_FMT))

        @classmethod
        def read_func(cls):
            return lambda x: read_csv(x,
                                      parse_dates=[0],
                                      date_parser=lambda y: cls.TIMEZONE.localize(to_datetime(int(y))),
                                      index_col=0)


    ENRICHEDOHLCVN = EnrichedOHLCVN.name()
    CONTINUOUS_CONTRACT = ContinuousContract.name()

    TABLES = {ENRICHEDOHLCVN: EnrichedOHLCVN,
              CONTINUOUS_CONTRACT: ContinuousContract}

    # TABLE = 'table'
    # PRODUCT = 'product'
    # PTYPE = 'type'
    # EXPIRY = 'expiry'
    # CLOCK = 'clock'
    # WIDTH = 'width'
    # YEAR = 'year'
    #
    # FILE_STRUCTURE = [
    #     PTYPE,
    #     PRODUCT,
    #     EXPIRY,
    #     TABLE,
    #     CLOCK,
    #     WIDTH,
    #     YEAR]
    #
    # DATE_FMT = '%Y%m%d'
    #
    # @classmethod
    # def date_from_filename(cls, fn):
    #     fn = basename(fn)
    #     return dt.datetime.strptime(re.search('[0-9]{8}', fn).group(), cls.DATE_FMT)
    #
    #
    # @classmethod
    # def read_func(cls):
    #     return lambda x: read_csv(x, parse_dates=[0], date_parser=lambda x: to_datetime(int(x)), index_col=0)


dbbox_configs = {'quantdb1': Quantdb1,
                 'quantsim1': Quantsim1,
                 'lcmquantldn1': Lcmquantldn1}