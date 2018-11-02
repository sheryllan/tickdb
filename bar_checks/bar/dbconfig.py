from enum import Enum



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
    DBNAME = 'bar'

    UNDEFINED = 999999999998
    ENRICHEDOHLCVN = EnrichedOHLCVN


class Quantdb1(Basedb):
    USERNAME = 'root'
    PASSWORD = 'root123'

    HOSTNAME = 'lcldn-quantdb1'
    PORT = 8086


class Quantsim1(Basedb):
    USERNAME = 'root'
    PASSWORD = 'root123'

    HOSTNAME = 'lcmint-quantsim1'
    PORT = 8086

    CONTINUOUS_CONTRACT = ContinuousContract


