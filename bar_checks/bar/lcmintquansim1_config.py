from .datastore_config import StrEnum, BaseTable
import pytz

HOSTNAME = 'lcmint-quantsim1'
USERNAME = 'influx'
PASSWORD = 'influx123'


class EnrichedOHLCVN(BaseTable):
    class Fields(StrEnum):
        TIME = 'time'
        TRIGGER_TIME = 'trigger_time'
        EXCH_TIME = 'exch_time'
        SOFTWARE_TIME = 'software_time'

        OPEN = 'open'
        HIGH = 'high'
        LOW = 'low'
        CLOSE = 'close'

        VOLUME = 'volume'
        NET_VOLUME = 'net_volume'

        TBUYV = 'tbuyv'
        TBUYVWAP = 'tbuyvwap'
        TBUYC = 'tbuyc'

        TSELLV = 'tsellv'
        TSELLVWAP = 'tsellvwap'
        TSELLC = 'tsellc'

        HBID = 'hbid'
        LBID = 'lbid'
        HASK = 'hask'
        LASK = 'lask'

        CBID1 = 'cbid1'
        CASK1 = 'cask1'
        CBIDV1 = 'cbidv1'
        CASKV1 = 'caskv1'

        CBID2 = 'cbid2'
        CASK2 = 'cask2'
        CBIDV2 = 'cbidv2'
        CASKV2 = 'caskv2'

        CBID3 = 'cbid3'
        CASK3 = 'cask3'
        CBIDV3 = 'cbidv3'
        CASKV3 = 'caskv3'

        TWABMP = 'twabmp'

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

    # TODO double check the fields
    class Fields(StrEnum):
            SHORT_CODE = 'short_code'
            TIME_ZONE = 'time_zone'
            EXPIRY = 'expiry'

    class Tags(StrEnum):
        PRODUCT = 'product'
        ROLL_STRATEGY = 'roll_strategy'
        TYPE = 'type'


class FileConfig:
    BASEDIR = '/opt/data'

    SOURCE = 'source'
    TABLE = 'table'
    YEAR = 'year'
    DATA_FILE = 'data_file'

    TIMEZONE = pytz.UTC
    TIME_COL_IDX = 0
    DATETIME_COLS = [0]
    SEPARATOR = ','
    FILE_STRUCTURE = []
