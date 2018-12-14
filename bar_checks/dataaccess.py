from influxcommon import *
from influxdb import DataFrameClient
import paramiko, stat
import datetime as dt

from timeutils.commonfuncs import isin_closed, to_tz_datetime


class BaseAccesor(object):

    def get_data(self, **kwargs):
        raise NotImplementedError

    def get_continuous_contracts(self, **kwargs):
        raise NotImplementedError


class FileAccessor(BaseAccesor):
    TIME_IDX = 'time'

    # MAX_FILE_NUM = 10000

    def __init__(self, settings, username, password, sys_sep='/'):
        self.settings = settings
        self.host = settings.HOSTNAME
        self.basedir = settings.BASEDIR
        self.username = username
        self.password = password
        self.sys_sep = sys_sep

    def transport_session(self):
        transport = paramiko.Transport(self.host)
        transport.connect(username=self.username, password=self.password)
        return transport

    def join_paths(self, *args):
        return self.sys_sep + self.sys_sep.join((x.strip(self.sys_sep) for x in args))

    def rcsv_listdir(self, sftp: paramiko.SFTPClient, path, dirs):
        if stat.S_ISREG(sftp.stat('.').st_mode):
            yield self.join_paths(sftp.getcwd(), sftp.stat('.').filename)
            sftp.chdir('..')
            return
        sftp.chdir(path)

        if not dirs:
            yield from (self.join_paths(sftp.getcwd(), fn) for fn in sftp.listdir('.'))
            sftp.chdir('..')
            return
        subdirs = sftp.listdir('.') if dirs[0] is None \
            else set(sftp.listdir('.')).intersection(to_iter(dirs[0], ittype=iter))

        for subdir in subdirs:
            yield from self.rcsv_listdir(sftp, subdir, dirs[1:])
        sftp.chdir('..')

    def find_files(self, file_structure, sftp=None, close_conn=True, **kwargs):
        dirs = [kwargs.get(x, None) for x in file_structure]
        transport = None
        if sftp is None:
            transport = self.transport_session()
            sftp = paramiko.SFTPClient.from_transport(transport)

        yield from self.rcsv_listdir(sftp, self.basedir, dirs)

        if close_conn:
            sftp.close()
            if transport is not None:
                transport.close()

    def get_data(self, time_from: dt.datetime = None, time_to: dt.datetime = None, include_from=True, include_to=True,
                 read_func=None, empty=None, concat=True, **kwargs):
        table = self.settings.TABLES[kwargs[self.settings.TABLE]]
        tz, file_structure = table.TIMEZONE, table.FILE_STRUCTURE
        time_from = to_tz_datetime(pd.to_datetime(time_from), to_tz=tz)
        time_to = to_tz_datetime(pd.to_datetime(time_to), to_tz=tz)
        date_from = None if time_from is None else time_from.normalize()
        date_to = None if time_to is None else time_to.normalize()
        read_func = table.read_func() if read_func is None else read_func

        def from_files(files, client):
            yield pd.DataFrame()
            for fpath in files:
                with client.open(fpath) as fhandle:
                    yield read_func(fhandle)

        def bound_by_timeframe(df):
            for tag, tag_df in df.groupby(table.Tags.values()):
                i, j = bound_indices(tag_df.index,
                                     lambda x: isin_closed(x, time_from, time_to, (include_from, include_to)))
                yield tag, tag_df.iloc[i:j]

        with self.transport_session() as transport:
            with paramiko.SFTPClient.from_transport(transport) as sftp:
                files_found = self.find_files(file_structure, sftp, False, **kwargs)
                dated_fpaths = pd.DataFrame((table.date_from_filename(x), x) for x in files_found)
                dated_fpaths = dated_fpaths.set_index(0).sort_index().loc[date_from: date_to, 1]

                data_df = pd.concat(from_files(dated_fpaths, sftp))
                if data_df.empty:
                    return empty

                results = {k: v for k, v in bound_by_timeframe(data_df)}
                return pd.concat(results.values()) if concat else results

    def get_continuous_contracts(self, time_from=None, time_to=None, **kwargs):
        from bar.datastore_config import ContinuousContract
        import pytz
        fields, tags = ContinuousContract.Fields, ContinuousContract.Tags

        tz = pytz.timezone('America/Chicago')
        time_from = to_tz_datetime(time_from, to_tz=tz)
        time_to = to_tz_datetime(time_to, to_tz=tz)
        for product in to_iter(kwargs[tags.PRODUCT]):
            yield (time_from, time_to), pd.Series({fields.TIME_ZONE: 'America/Chicago',
                                                    tags.PRODUCT: product,
                                                    tags.TYPE: kwargs[tags.TYPE],
                                                    tags.EXPIRY: 'DEC2018'})


class InfluxdbAccessor(BaseAccesor):
    def __index__(self, host, port, database):
        self.client = DataFrameClient(host=host, port=port, database=database)

    def get_data(self, table, time_from=None, time_to=None, include_from=True, include_to=True,
                 others=None, empty=None, **kwargs):
        terms = [where_term(k, v) for k, v in kwargs.items()]
        terms = terms + time_terms(time_from, time_to, include_from, include_to)
        clauses = where_clause(terms) if others is None else [where_clause(terms)] + to_iter(others)
        qstring = select_query(table, clauses=clauses)
        return self.client.query(qstring).get(table, empty)

    def get_continuous_contracts(self, table, time_from=None, time_to=None, **kwargs):
        contracts = self.get_data(table, time_from, time_to, empty=pd.DataFrame(), **kwargs)

        if contracts.empty or contracts.index[0] > time_from:
            others = [order_by('DESC'), limit(1)]
            prev_contract = self.get_data(table, time_to=time_from, others=others, empty=pd.DataFrame(), **kwargs)
            prev_contract.index = pd.Index([time_from])
            contracts = prev_contract.append(contracts)

        start, end = 'start', 'end'
        contracts[start] = contracts.index
        sr_end = contracts[start][1:].append(pd.Series([time_to]), ignore_index=True)
        contracts[end] = sr_end.tolist()

        for i, row in contracts.iterrows():
            yield (row[start], row[end]), row



# from bar.datastore_config import Lcmquantldn1
#
# fa = FileAccessor(Lcmquantldn1, 'slan', 'slanpass')
# # r = fa.find_files(product='ES', type='F', expiry='SEP2018', table='EnrichedOHLCVN', clock='M')
# r = fa.get_data(dt.datetime(2018, 11, 1), product=['ES', 'ZN'], type='F', expiry='DEC2018', table='EnrichedOHLCVN',
#                 clock_type='M')
#
# print(r)
