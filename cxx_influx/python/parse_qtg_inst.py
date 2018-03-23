
import sys
import time
import requests

Months = {'F' : 1, 'G' : 2, 'H': 3, 'J' : 4, 'K' : 5, 'M' : 6, 'N' : 7, 'Q' : 8, 'U': 9, 'V' : 10, 'X' : 11, 'Z' : 12}


Columns = {'qtg' : 1, 'trading' : 3, 'exchange' : 8, 'expirydate' : 9, 'typecode' : 16}


Special_Reactor_Name = {'OKS2201104C00262' : 'OKS2', 'GE' : 'ED'}

def get_qtg_short_name(qtg_name_):
    global Special_Reactor_Name
    if qtg_name_ in Special_Reactor_Name:
        return qtg_name_
    index = qtg_name_.find('_')
    if index != -1 and index <= 2: #name is like G_U8_xxx for ICE.
        return qtg_name_[:index]
    global Months
    index = 0
    for c in qtg_name_:
        if not c.isdigit():
            index = index + 1
            continue
        if index > 1 and qtg_name_[index - 1] in Months:
            return qtg_name_[:index - 1]
        index = index + 1
    return qtg_name_            

def get_eurex_name(trading_):
    index = 0
    for c in trading_:
        if c.isdigit():
            return trading_[:index]
        index = index + 1
    return trading_

def get_cme_sfe_name(trading_):
    return get_qtg_short_name(trading_)

def is_eurex(exch_):
    return exch_ == 'XEUR'

def is_cme_sfe(exch_):
    return exch_ == 'XCME' or exch_ == 'XCEC' or exch_ == 'XCBT' or exch_ == 'XNYM' or exch_ == 'XNAS' or exch_ == 'XSFE'

def get_reactor_name(qtg_short_name_, trading_, exch_):
    global Special_Reactor_Name
    if qtg_short_name_ in Special_Reactor_Name:
        return Special_Reactor_Name[qtg_short_name_]
    if is_eurex(exch_):
        if not trading_: #invalid trading for eurex
            return qtg_short_name_
        return get_eurex_name(trading_)
    if is_cme_sfe(exch_):
        if not trading_ or trading_.isdigit() or trading_.find(':') != -1:#invalid trading for cme
            return qtg_short_name_
        return get_cme_sfe_name(trading_)
    return qtg_short_name_

def get_reactor_name_for_equity_and_currency(qtg_name_):
    index = qtg_name_.find('_')
    if index == -1:
        return qtg_name_
    else:
        return qtg_name_[:index]

non_expiry_reactor_name = {}
def set_non_expire(cols_):
    exch = cols[Columns['exchange']].strip()
    qtg_name = cols[Columns['qtg']].strip()
    typecode = cols[Columns['typecode']].strip()
    global non_expire_dict
    if exch not in non_expire_dict:
        non_expire_dict[exch] = {}

    type = 'E'
    reactor_name = get_reactor_name_for_equity_and_currency(qtg_name)
    if typecode == '7': #currency
        type = 'C'            
    elif typecode == '6': #index
        type = 'I'
        reactor_name = qtg_name
    elif exch == 'XBGC': #bond market
        type = 'B'
        index = qtg_name.rfind('_')
        if index != -1:
            reactor_name = qtg_name[:index]
    if type not in non_expire_dict[exch]:
        non_expire_dict[exch][type] = {}
    if qtg_name in non_expire_dict[exch][type]:
        print "error, duplicated ", qtg_name
    global non_expiry_reactor_name
    if reactor_name + exch in non_expiry_reactor_name:
        print "error, duplicated reactor name ", reactor_name, qtg_name
    non_expiry_reactor_name[reactor_name + exch] = 1
    non_expire_dict[exch][type][qtg_name] = reactor_name
#print get_qtg_short_name(sys.argv[1])
#print get_eurex_name(sys.argv[1])
#exit(1)

if len(sys.argv) < 5:
    print "usage python ./parse_qtg_inst.py <inst file> <influx host> <influx port> <influx db>"
    exit(1)

file = open(sys.argv[1])

lines = file.readlines()

expire_dict = {}

non_expire_dict = {}

for line in lines:
    if line.strip()[0] == '#':
        continue
    cols = line.split(',')
    for col in cols:
        col.strip()

    expiry = cols[Columns['expirydate']].strip()

    if not expiry or expiry == '20991201':
        set_non_expire(cols)
        continue

    typecode = cols[Columns['typecode']].strip()
    if typecode == '9': #strategy
        continue
    exch = cols[Columns['exchange']].strip()

    qtg_name = cols[Columns['qtg']].strip()
    
    qtg_short_name = get_qtg_short_name(qtg_name)
    reactor_name = get_reactor_name(qtg_short_name, cols[Columns['trading']].strip(), exch)   
    if exch not in expire_dict:
        expire_dict[exch] = {}
    if qtg_short_name in expire_dict[exch]:
        if reactor_name and reactor_name != expire_dict[exch][qtg_short_name]:
            print 'error', exch, qtg_short_name, reactor_name, expire_dict[exch][qtg_short_name]
    if reactor_name:
        expire_dict[exch][qtg_short_name] = reactor_name
    else:
        print 'error no reactor name found for', exch, qtg_short_name, qtg_name
file.close() 

time = int(time.time() * 1000 * 1000 * 1000)
influx_sql = ''
for k, v in expire_dict.iteritems():
    for key, value in v.iteritems():
        if influx_sql:
            influx_sql = influx_sql + '\n';
        influx_sql = influx_sql + 'product_name_mapping,exch={},type={} qtg="{}",reactor="{}" {:d}'.format(k, 'F', key, value, time)
        influx_sql = influx_sql + '\n' + 'product_name_mapping,exch={},type={} qtg="{}",reactor="{}" {:d}'.format(k, 'O', key, value, time + 1)
        influx_sql = influx_sql + '\n' + 'product_name_mapping,exch={},type={} qtg="{}",reactor="{}" {:d}'.format(k, 'S', key, value, time + 2)
        time = time + 3

for k, v in non_expire_dict.iteritems():
    for k1, v1 in v.iteritems():
        for k2, v2 in v1.iteritems():
            if influx_sql:
                influx_sql = influx_sql + '\n';
            influx_sql = influx_sql + 'product_name_mapping,exch={},type={} qtg="{}",reactor="{}" {:d}'.format(k, k1, k2, v2, time)
            time = time + 1
print influx_sql
print "send above data to influx db."
r = requests.post("http://{}:{}/write?db={}".format(sys.argv[2], sys.argv[3], sys.argv[4]), data=influx_sql)

print(r.status_code, r.reason)
    
