import datetime as dt
import re
import paramiko
from os.path import basename, splitext
from itertools import zip_longest

from dataaccess import *
from bar.datastore_config import *
from timeutils.commonfuncs import isin_closed, to_tz_datetime


class BarAccessor(object):
    TIME_IDX = 'time'

    TIME_FROM = 'time_from'
    TIME_TO = 'time_to'
    INCLUDE_FROM = 'include_from'
    INCLUDE_TO = 'include_to'

    @classmethod
    def factory(cls, type_name):
        if type_name == Lcmquantldn1.HOSTNAME:
            return Lcmquantldn1Accessor()
        if type_name == Quantdb1.HOSTNAME:
            return InfluxBarAccessor(Quantdb1, Quantdb1.BarDatabase.DBNAME)
        if type_name == Quantsim1.HOSTNAME:
            return InfluxBarAccessor(Quantsim1, Quantsim1.BarDatabase.DBNAME)

    def set_arg_time_range(self, kwargs, tz=None):
        time_from = to_tz_datetime(pd.to_datetime(kwargs.get(self.TIME_FROM)), to_tz=tz)
        time_to = to_tz_datetime(pd.to_datetime(kwargs.get(self.TIME_TO)), to_tz=tz)
        kwargs.update({self.TIME_FROM: time_from, self.TIME_TO: time_to})
        return time_from, time_to

    def set_arg_includes(self, kwargs):
        include_from = kwargs.get(self.INCLUDE_FROM)
        include_to = kwargs.get(self.INCLUDE_TO)
        kwargs.update({self.INCLUDE_FROM: include_from, self.INCLUDE_TO: include_to})
        return include_from, include_to


class InfluxBarAccessor(InfluxdbAccessor, BarAccessor):
    def get_data(self, table, others=None, empty=None, **kwargs):
        time_from, time_to = self.set_arg_time_range(kwargs)
        include_from, include_to = self.set_arg_includes(kwargs)
        data = super().get_data(table, time_from, time_to, include_from, include_to, others, empty, **kwargs)
        return data if data == empty else data.rename_axis(self.TIME_IDX)

    def get_continuous_contracts(self, **kwargs):
        table = self.config.TABLES[self.config.ContinuousContract.name()]
        time_from, time_to = self.set_arg_time_range(kwargs)
        contracts = self.get_data(table, time_from, time_to, empty=pd.DataFrame(), **kwargs)
        time_from = contracts.index[0] if time_from is None else time_from

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


class Lcmquantldn1Accessor(Accessor, BarAccessor):

    def __init__(self):
        super().__init__(Lcmquantldn1)

    def fmanager_factory(self, type_name):
        if type_name == Lcmquantldn1.EnrichedOHLCVN.name():
            return self.EnrichedManager(self.TIME_FROM, self.TIME_TO)
        elif type_name == Lcmquantldn1.ContinuousContract.name():
            return self.ContinuousContractManager()

    class EnrichedManager(FileManager):
        def __init__(self, arg_time_from, arg_time_to):
            super().__init__(Lcmquantldn1.TABLES[Lcmquantldn1.EnrichedOHLCVN.name()])
            self.arg_time_from = arg_time_from
            self.arg_time_to = arg_time_to

        def directories(self, **kwargs):
            kwargs[self.config.TABLE] = self.config.name()
            return [kwargs.get(x) for x in self.config.FILE_STRUCTURE]

        def date_from_path(self, path):
            match = re.search(self.config.FILENAME_DATE_PATTERN, basename(path))
            if match is None:
                return None
            return self.config.TIMEZONE.localize(dt.datetime.strptime(match.group(), self.config.FILENAME_DATE_FORMAT))

        def find_files(self, filesys, **kwargs):
            time_from, time_to = kwargs.get(self.arg_time_from), kwargs.get(self.arg_time_to)
            date_from = None if time_from is None else time_from.normalize()
            date_to = None if time_to is None else time_to.normalize()

            years =  None if any(x is None for x in (date_from, date_to)) else (date_from.year, date_to.year)
            kwargs.update({self.config.YEAR: years})

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

        def directories(self, **kwargs):
            kwargs[self.config.TABLE] = self.config.name()
            kwargs[self.config.DATA_FILE] = self.data_file
            return [kwargs.get(x) for x in self.config.FILE_STRUCTURE]

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
                               compression=compression)\
                .rename_axis(self.TIME_IDX)

    def get_data(self, table, empty=None, concat=True, **kwargs):
        file_mgr = self.fmanager_factory(table)
        time_from, time_to = self.set_arg_time_range(kwargs, file_mgr.config.TIMEZONE)
        closed = self.set_arg_includes(kwargs)

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

    def get_continuous_contracts(self, **kwargs):
        table_config = self.config.TABLES[self.config.ContinuousContract.name()]
        time_from, time_to = self.set_arg_time_range(kwargs, table_config.TIMEZONE)
        new_kwargs = {k: v for k, v in kwargs.items() if k not in [self.TIME_FROM, self.TIME_TO]}
        for _, contracts in self.get_data(table_config.name(), empty=[], concat=False, **new_kwargs):
            contracts = contracts.sort_index()
            filtered = contracts.loc[time_from: time_to]

            if not filtered.empty:
                start, end = filtered.index[0], None
                time_from = filtered.index[0] if time_from is None else time_from
                if start > time_from:
                    yield (time_from, start), contracts.loc[:time_from].iloc[-1]

                contract = filtered.iloc[0]
                for i, row in filtered.iloc[1:].iterrows():
                    end = i
                    yield (start, end), contract
                    start = end
                    contract = row

                if end != time_to:
                    yield (start, time_to), contract

            elif not contracts.empty:
                yield (time_from, time_to), contracts.loc[:time_from].iloc[-1]


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
