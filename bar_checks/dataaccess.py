import datetime as dt
import re

import paramiko
import stat
import posixpath
from influxdb import DataFrameClient
from os.path import basename

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

    def rcsv_listdir(self, filesys, path, dirs):
        if stat.S_ISREG(filesys.stat(path).st_mode):
            yield posixpath.join(filesys.getcwd(), path)
            return

        filesys.chdir(str(path))
        subdirs = filesys.listdir('.')

        if not dirs:
            yield from (posixpath.join(filesys.getcwd(), fn) for fn in subdirs)
            filesys.chdir('..')
            return

        if dirs[0] is not None:
            subdirs = set(map(str, to_iter(dirs[0], ittype=iter))).intersection(subdirs)

        for subdir in subdirs:
            yield from self.rcsv_listdir(filesys, subdir, dirs[1:])
        filesys.chdir('..')

    def find_files(self, **kwargs):
        raise NotImplementedError


class Lcmquantldn1Accessor(Accessor):
    TIME_IDX = 'time'

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

        def directories(self, **kwargs):
            kwargs[self.config.TABLE] = self.config.name()
            return [kwargs.get(x, None) for x in self.config.FILE_STRUCTURE]

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

        def find_files(self, filesys, **kwargs):
            # for name_part in self.config.FILENAME_STRUCTURE:
            #     for value in to_iter(kwargs.get(name_part, [])):

            products = to_iter(kwargs.get(self.config.Tags.PRODUCT, []))
            for p in products:
                kwargs[self.config.Tags.PRODUCT] = p
                dirs = [kwargs.get(x, None) for x in self.config.FILE_STRUCTURE]
                filename = p


    def transport_session(self):
        transport = paramiko.Transport(self.config.HOSTNAME)
        transport.connect(username=self.config.USERNAME, password=self.config.PASSWORD)
        return transport

    def get_data(self, table, time_from: dt.datetime = None, time_to: dt.datetime = None, include_from=True,
                 include_to=True, empty=None, **kwargs):
        # tbclass = self.settings.TABLES[table]
        # time_from = to_tz_datetime(pd.to_datetime(time_from), to_tz=tbclass.TIMEZONE)
        # time_to = to_tz_datetime(pd.to_datetime(time_to), to_tz=tbclass.TIMEZONE)
        #
        # dirs = tbclass.directories(**kwargs)
        #
        # def from_files(files, client):
        #     for fpath in files:
        #         with client.open(fpath) as fhandle:
        #             yield self.read(fhandle, tbclass)
        #
        # def bound_by_time(df):
        #     for tag, tag_df in df.groupby(table.Tags.values()):
        #         i, j = bound_indices(tag_df.index,
        #                              lambda x: isin_closed(x, time_from, time_to, (include_from, include_to)))
        #         yield tag, tag_df.iloc[i:j]
        #
        # with self.transport_session() as transport:
        #     with paramiko.SFTPClient.from_transport(transport) as sftp:
        #         files_found = self.find_files(dirs, sftp, False)
        #         fpaths = files_found
        #
        #         if hasattr(tbclass, 'date_from_filename'):
        #             fpaths = pd.DataFrame((tbclass.date_from_path(x), x) for x in files_found)
        #             if fpaths.empty:
        #                 return empty
        #             date_from = None if time_from is None else time_from.normalize()
        #             date_to = None if time_to is None else time_to.normalize()
        #             fpaths = fpaths.set_index(0).sort_index().loc[date_from: date_to, 1]
        #
        #             if fpaths.empty:
        #                 return empty
        #
        #         data_df = pd.concat(from_files(fpaths, sftp))
        #
        #         if data_df.empty:
        #             return empty
        #         results = {k: v for k, v in bound_by_time(data_df)}
        #         return pd.concat(results.values()) if concat else results


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

        contracts = self.get_data(self.settings.CONTINUOUS_CONTRACT, concat=True, **kwargs)
        if contracts is not None:
            contracts = contracts.sort_index()
            filtered = contracts.loc[time_from: time_to]
            if filtered.index[0] > time_from:
                yield (time_from, filtered.index[0]), contracts[None: time_from].iloc[-1]



    def read(self, filename, tbclass):
        return pd.read_csv(filename,
                           parse_dates=to_iter(tbclass.PARSE_DATES),
                           date_parser=lambda y: tbclass.TIMEZONE.localize(pd.to_datetime(int(y))),
                           index_col=tbclass.INDEX_COL)



# from bar.datastore_config import Lcmquantldn1
#
# fa = FileAccessor(Lcmquantldn1, 'slan', 'slanpass')
# # r = fa.find_files(product='ES', type='F', expiry='SEP2018', table='EnrichedOHLCVN', clock='M')
# r = fa.get_data(dt.datetime(2018, 11, 1), product=['ES', 'ZN'], type='F', expiry='DEC2018', table='EnrichedOHLCVN',
#                 clock_type='M')
#
# print(r)
