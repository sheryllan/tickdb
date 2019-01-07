import datetime as dt
import re

import paramiko
import stat
import posixpath
from influxdb import DataFrameClient
from os.path import basename
from itertools import zip_longest

from influxcommon import *
from timeutils.commonfuncs import isin_closed, to_tz_datetime
from bar.datastore_config import *


class Accessor(object):
    def __init__(self, config):
        self.config = config

    @staticmethod
    def factory(type_name):
        if type_name == Lcmquantldn1.HOSTNAME:
            return Lcmquantldn1Accessor()

    def get_data(self, **kwargs):
        raise NotImplementedError

    def get_continuous_contracts(self, **kwargs):
        raise NotImplementedError


class InfluxdbAccessor(Accessor):
    def __init__(self, config, database):
        super().__init__(config)
        self.client = DataFrameClient(host=self.config.HOSTNAME, port=self.config.PORT, database=database)

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


class FileManager(object):
    def __init__(self, config):
        self.config = config

    @classmethod
    def rcsv_listdir(cls, filesys, path, dirs):
        if stat.S_ISREG(filesys.stat(path).st_mode):
            yield posixpath.join(filesys.getcwd(), path)
            return

        filesys.chdir(str(path))

        if not dirs:
            yield filesys.getcwd()
            filesys.chdir('..')
            return

        subdirs = filesys.listdir('.')
        if dirs[0] is not None:
            dir_name = dirs[0](filesys.getcwd()) if callable(dirs[0]) else dirs[0]
            subdirs = set(map(str, to_iter(dir_name, ittype=iter))).intersection(subdirs)

        for subdir in subdirs:
            yield from cls.rcsv_listdir(filesys, subdir, dirs[1:])
        filesys.chdir('..')

    def find_files(self, **kwargs):
        raise NotImplementedError


class Lcmquantldn1Accessor(Accessor):
    TIME_IDX = 'time'

    TIME_FROM = 'time_from'
    TIME_TO = 'time_to'
    INCLUDE_FROM = 'include_from'
    INCLUDE_TO = 'include_to'


    def __init__(self):
        super().__init__(Lcmquantldn1)

    def fmanager_factory(self, type_name):
        if type_name == Lcmquantldn1.ENRICHEDOHLCVN:
            return self.EnrichedManager()
        elif type_name == Lcmquantldn1.CONTINUOUS_CONTRACT:
            return self.ContinuousContractManager()

    class EnrichedManager(FileManager):
        def __init__(self):
            super().__init__(Lcmquantldn1.EnrichedOHLCVN)

        def directories(self, *args, **kwargs):
            dirs = {k: v for k, v in zip_longest(self.config.FILE_STRUCTURE, args)}
            kwargs[self.config.TABLE] = self.config.name()
            dirs.update({k: v for k, v in kwargs if k in dirs})
            return list(dirs.values())

        def date_from_path(self, path):
            match = re.search(self.config.FILENAME_DATE_PATTERN, basename(path))
            if match is None:
                return None
            return self.config.TIMEZONE.localize(dt.datetime.strptime(match.group(), self.config.FILENAME_DATE_FORMAT))

        def find_files(self, filesys, time_from: pd.Timestamp = None, time_to: pd.Timestamp = None, **kwargs):
            date_from = None if time_from is None else time_from.normalize()
            date_to = None if time_to is None else time_to.normalize()

            files = self.rcsv_listdir(filesys, self.config.BASEDIR, self.directories(**kwargs))
            fpaths = pd.DataFrame((self.date_from_path(x), x) for x in files)
            return pd.Series() if fpaths.empty else fpaths.set_index(0).sort_index().loc[date_from: date_to, 1]

    class ContinuousContractManager(FileManager):
        def __init__(self):
            super().__init__(Lcmquantldn1.ContinuousContract)

        def data_file(self, path):
            dirs = path[len(self.config.BASEDIR):].strip(posixpath.sep).split(posixpath.sep)
            dirs_dict = {k: v for k, v in zip_longest(self.config.FILE_STRUCTURE, dirs)}
            return self.config.FILENAME_FORMAT.format((dirs_dict.get(s, '') for s in self.config.FILENAME_STRUCTURE))

        def directories(self, *args, **kwargs):
            dirs = {k: v for k, v in zip_longest(self.config.FILE_STRUCTURE, args)}
            kwargs[self.config.TABLE] = self.config.name()
            kwargs[self.config.DATA_FILE] = self.data_file
            dirs.update({k: v for k, v in kwargs if k in dirs})
            return list(dirs.values())

        def find_files(self, filesys, **kwargs):
            return pd.Series(self.rcsv_listdir(filesys, self.config.BASEDIR, self.directories(**kwargs)))


    def transport_session(self):
        transport = paramiko.Transport(self.config.HOSTNAME)
        transport.connect(username=self.config.USERNAME, password=self.config.PASSWORD)
        return transport

    def read(self, filename, tbclass):
        return pd.read_csv(filename,
                           parse_dates=to_iter(tbclass.PARSE_DATES),
                           date_parser=lambda y: tbclass.TIMEZONE.localize(pd.to_datetime(int(y))),
                           index_col=tbclass.INDEX_COL)

    def get_data(self, table, empty=None, concat=True, **kwargs):

        file_mgr = self.fmanager_factory(table)
        time_from = to_tz_datetime(pd.to_datetime(kwargs.get(self.TIME_FROM, None)), to_tz=file_mgr.config.TIMEZONE)
        time_to = to_tz_datetime(pd.to_datetime(kwargs.get(self.TIME_TO, None)), to_tz=file_mgr.config.TIMEZONE)
        closed = kwargs.get(self.INCLUDE_FROM, None), kwargs.get(self.INCLUDE_TO, None)

        def from_files(files, client):
            for fpath in files:
                with client.open(fpath) as fhandle:
                    yield self.read(fhandle, file_mgr.config)

        def bound_by_time(df):
            for tag, tag_df in df.groupby(table.Tags.values()):
                i, j = bound_indices(tag_df.index,
                                     lambda x: isin_closed(x, time_from, time_to, closed))
                yield tag, tag_df.iloc[i:j]

        with self.transport_session() as transport:
            with paramiko.SFTPClient.from_transport(transport) as sftp:
                files_found = file_mgr.find_files(sftp, **kwargs)
                if files_found.empty:
                    return empty

                data_df = pd.concat(from_files(files_found, sftp))

                if data_df.empty:
                    return empty
                results = {k: v for k, v in bound_by_time(data_df)}
                return pd.concat(results.values()) if concat else results


    def get_continuous_contracts(self, time_from=None, time_to=None, **kwargs):
        # from bar.datastore_config import ContinuousContract
        # import pytz
        # fields, tags = ContinuousContract.Fields, ContinuousContract.Tags
        #
        # tz = pytz.timezone('America/Chicago')
        # time_from = to_tz_datetime(time_from, to_tz=tz)
        # time_to = to_tz_datetime(time_to, to_tz=tz)
        # for product in to_iter(kwargs[tags.PRODUCT]):
        #     yield (time_from, time_to), pd.Series({fields.TIME_ZONE: 'America/Chicago',
        #                                             tags.PRODUCT: product,
        #                                             tags.TYPE: kwargs[tags.TYPE],
        #                                             tags.EXPIRY: 'DEC2018'})

        contracts = self.get_data(self.config.CONTINUOUS_CONTRACT, concat=True, **kwargs)
        if contracts is not None:
            contracts = contracts.sort_index()
            filtered = contracts.loc[time_from: time_to]

            if not filtered.empty:
                start = filtered.index[0]
                if start > time_from:
                    yield (time_from, start), contracts[None: time_from].iloc[-1]

                end = filtered.index[1] if len(filtered) > 1 else time_to
                contract = filtered.iloc[0]

                for i, row in filtered.iloc[1:].iterrows():
                    yield (start, end), contract

                    start = end
                    end = i
                    contract = row


                yield (start, end), contract



# from bar.datastore_config import Lcmquantldn1
#
# fa = FileAccessor(Lcmquantldn1, 'slan', 'slanpass')
# # r = fa.find_files(product='ES', type='F', expiry='SEP2018', table='EnrichedOHLCVN', clock='M')
# r = fa.get_data(dt.datetime(2018, 11, 1), product=['ES', 'ZN'], type='F', expiry='DEC2018', table='EnrichedOHLCVN',
#                 clock_type='M')
#
# print(r)
