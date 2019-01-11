import datetime as dt
import re
import paramiko
from os.path import basename, splitext
from itertools import zip_longest

from dataaccess import *
from bar.datastore_config import *
from timeutils.commonfuncs import isin_closed, to_tz_datetime


def accessor_factory(type_name):
    if type_name == Lcmquantldn1.HOSTNAME:
        return Lcmquantldn1Accessor()
    if type_name == Quantdb1.HOSTNAME:
        return InfluxBarAccessor(Quantdb1, Quantdb1.BarDatabase.DBNAME)
    if type_name == Quantsim1.HOSTNAME:
        return InfluxBarAccessor(Quantsim1, Quantsim1.BarDatabase.DBNAME)


class InfluxBarAccessor(InfluxdbAccessor):

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


class Lcmquantldn1Accessor(Accessor):
    TIME_FROM = 'time_from'
    TIME_TO = 'time_to'
    INCLUDE_FROM = 'include_from'
    INCLUDE_TO = 'include_to'

    def __init__(self):
        super().__init__(Lcmquantldn1)

    def fmanager_factory(self, type_name):
        if type_name == Lcmquantldn1.EnrichedOHLCVN.name():
            return self.EnrichedManager()
        elif type_name == Lcmquantldn1.ContinuousContract.name():
            return self.ContinuousContractManager()

    class EnrichedManager(FileManager):
        def __init__(self):
            super().__init__(Lcmquantldn1.TABLES[Lcmquantldn1.EnrichedOHLCVN.name()])

        def directories(self, *args, **kwargs):
            kwargs[self.config.TABLE] = self.config.name()
            dirs = {k: kwargs.get(k, v) for k, v in zip_longest(self.config.FILE_STRUCTURE, args)}
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
            super().__init__(Lcmquantldn1.TABLES[Lcmquantldn1.ContinuousContract.name()])
            self.tz = self.config.TIMEZONE

        def data_file(self, path):
            dirs = path[len(self.config.BASEDIR):].strip(posixpath.sep).split(posixpath.sep)
            dirs_dict = {k: v for k, v in zip_longest(self.config.FILE_STRUCTURE, dirs)}
            return self.config.FILENAME_FORMAT.format(*(dirs_dict.get(s, '') for s in self.config.FILENAME_STRUCTURE))

        def directories(self, *args, **kwargs):
            kwargs[self.config.TABLE] = self.config.name()
            kwargs[self.config.DATA_FILE] = self.data_file
            dirs = {k: kwargs.get(k, v) for k, v in zip_longest(self.config.FILE_STRUCTURE, args)}
            return list(dirs.values())

        def find_files(self, filesys, **kwargs):
            return pd.Series(self.rcsv_listdir(filesys, self.config.BASEDIR, self.directories(**kwargs)))

    def transport_session(self):
        transport = paramiko.Transport(self.config.HOSTNAME)
        transport.connect(username=self.config.USERNAME, password=self.config.PASSWORD)
        return transport

    def read(self, filepath, filesys, tbclass):
        extension = splitext(filepath)[1]
        if extension == '.gz':
            compression = 'gzip'
        else:
            compression = 'infer'

        with filesys.open(filepath) as fhandle:
            return pd.read_csv(fhandle,
                               parse_dates=to_iter(tbclass.DATETIME_COLS),
                               date_parser=lambda x: tbclass.TIMEZONE.localize(pd.to_datetime(int(x))),
                               index_col=tbclass.TIME_IDX_COL,
                               compression=compression)

    def get_data(self, table, empty=None, concat=True, **kwargs):
        file_mgr = self.fmanager_factory(table)
        time_from = to_tz_datetime(pd.to_datetime(kwargs.get(self.TIME_FROM, None)), to_tz=file_mgr.config.TIMEZONE)
        time_to = to_tz_datetime(pd.to_datetime(kwargs.get(self.TIME_TO, None)), to_tz=file_mgr.config.TIMEZONE)
        kwargs.update({self.TIME_FROM: time_from, self.TIME_TO: time_to})
        closed = kwargs.get(self.INCLUDE_FROM, None), kwargs.get(self.INCLUDE_TO, None)

        def bound_by_time(df):
            tags = file_mgr.config.Tags.values()
            for tag_key, tag_df in df.groupby(file_mgr.config.Tags.values()):
                i, j = bound_indices(tag_df.index,
                                     lambda x: isin_closed(x, time_from, time_to, closed))
                yield dict(zip(tags, tag_key)), tag_df.iloc[i:j]

        with self.transport_session() as transport:
            with paramiko.SFTPClient.from_transport(transport) as sftp:
                files_found = file_mgr.find_files(sftp, **kwargs)
                if files_found.empty:
                    return empty

                data_df = pd.concat(self.read(f, sftp, file_mgr.config) for f in files_found)
                if data_df.empty:
                    return empty
                results = list(bound_by_time(data_df))
                return pd.concat(y for x, y in results) if concat else results

    def get_continuous_contracts(self, time_from=None, time_to=None, **kwargs):
        table_config = self.config.TABLES[self.config.ContinuousContract.name()]
        time_from = to_tz_datetime(pd.to_datetime(time_from), to_tz=table_config.TIMEZONE)
        time_to = to_tz_datetime(pd.to_datetime(time_to), to_tz=table_config.TIMEZONE)

        for _, contracts in self.get_data(table_config.name(), empty=[], concat=False, **kwargs):
            contracts = contracts.sort_index()
            filtered = contracts.loc[time_from: time_to]

            if not filtered.empty:
                start, end = filtered.index[0], None
                if time_from is not None and start > time_from:
                    yield (time_from, start), contracts.loc[:time_from].iloc[-1]

                contract = filtered.iloc[0]
                for i, row in filtered.iloc[1:].iterrows():
                    end = i
                    yield (start, end), contract
                    start = end
                    contract = row

                if end != time_to:
                    yield (start, time_to), contract


# fa = Lcmquantldn1Accessor()
# # r = fa.find_files(product='ES', type='F', expiry='SEP2018', table='EnrichedOHLCVN', clock='M')
# # r = fa.get_data('EnrichedOHLCVN',
# #                 time_from=dt.datetime(2018, 11, 1),
# #                 include_from=False,
# #                 product=['ES', 'ZN'],
# #                 type='F',
# #                 expiry='DEC2018',
# #                 clock_type='M')
# cc = fa.get_continuous_contracts(dt.datetime(2018, 1, 1), dt.datetime(2018, 12, 31),
#                                  include_from=False,
#                                  source='reactor_gzips',
#                                  product=['ES'],
#                                  type='F',
#                                  clock_type='M'
#                                  )
#
# for c in cc:
#     print(c[0])
#     print(c[1])
