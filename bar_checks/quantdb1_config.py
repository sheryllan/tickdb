
USERNAME = 'root'
PASSWORD = 'root123'

HOSTNAME = 'lcldn-quantdb1'
PORT = 8086

TIME_IDX = 'time'


class Bar(object):
    DBNAME = 'bar'

    MS_EOHLCVN = 'EnrichedOHLCVN'


class EnrichedOHLCVN(object):
    # fields

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

    # tags
    CLOCK_TYPE = 'clock_type'
    EXPIRY = 'expiry'
    OFFSET = 'offset'
    PRODUCT = 'product'
    TYPE = 'type'
    WIDTH = 'width'

    TAGS = [PRODUCT, TYPE, EXPIRY, CLOCK_TYPE, OFFSET, WIDTH]

