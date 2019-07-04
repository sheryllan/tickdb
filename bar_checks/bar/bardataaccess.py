import re
import paramiko
from os.path import basename
from itertools import zip_longest
from collections import MutableMapping

from ..dataaccess import *
from .datastore_config import *
from ..timeutils.commonfuncs import isin_closed, to_tz_datetime, parse_datetime


class FixedKwargs(MutableMapping):
    def __init__(self, keys, default_val=None, **aliases):
        self.fixed_keys = frozenset(keys)
        self.default_val = default_val
        self._data_all = {k: self.default_val for k in self.fixed_keys}
        self.__data = {}
        self._aliases = aliases

    def add_alias(self, **aliases):
        self._aliases.update(aliases)

    def __len__(self):
        return len(self.__data)

    def __iter__(self):
        return iter(self.__data)

    def __setitem__(self, k, v):
        if k not in self.fixed_keys and k not in self._aliases:
            raise KeyError(k)

        key = self._aliases.get(k, k)
        self._data_all[key] = v
        if v != self.default_val:
            self.__data[key] = v
        else:
            self.__data.__delitem__(key)

    def __delitem__(self, k):
        raise NotImplementedError

    def __getitem__(self, k):
        return self._data_all[k]

    def __contains__(self, k):
        return k in self.fixed_keys


class ResultData(object):
    def __init__(self, df_or_groupby_obj, keys=(), **kwargs):
        self._dataframe = None
        self._groupby_obj = None
        self.keys = to_iter(keys, ittype=tuple)

        if isinstance(df_or_groupby_obj, pd.DataFrame):
            self._dataframe = df_or_groupby_obj
        elif not nontypes_iterable(df_or_groupby_obj):
            raise TypeError('Invalid type of input: df_or_groupby_obj must be of an Iterable')
        else:
            self._groupby_obj = df_or_groupby_obj

        if self._dataframe is None and self._groupby_obj is None:
            raise ValueError('Invalid initialization: the first argument must be set(not None)')

        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def dataframe(self):
        if self._dataframe is None:
            group_obj = self.__iter__()
            self._dataframe = pd.concat(v for _, v in group_obj) if self._groupby_obj else pd.DataFrame()
        return self._dataframe

    def to_key_tuple(self, keys):
        if isinstance(keys, dict):
            return tuple(keys.get(k, None) for k in self.keys)
        return keys if isinstance(keys, tuple) and len(self.keys) == len(keys) \
            else tuple(k for _, k in zip_longest(self.keys, to_iter(keys, ittype=iter)))

    def __iter__(self):
        if self._groupby_obj is None:
            self._groupby_obj = self.dataframe.groupby(self.keys) if self.keys else [None, self.dataframe]
        if not isinstance(self._groupby_obj, list):
            self._groupby_obj = list(self._groupby_obj)

        for i, (k, v) in enumerate(self._groupby_obj):
            ktuple = self.to_key_tuple(k)
            self._groupby_obj[i] = ktuple, v
            yield ktuple, v


class BarAccessor(object):
    TIME_IDX = 'time'

    TIME_FROM = 'time_from'
    TIME_TO = 'time_to'
    INCLUDE_FROM = 'include_from'
    INCLUDE_TO = 'include_to'

    @classmethod
    def factory(cls, type_name):
        if type_name == LcmintQuantsim1.HOSTNAME:
            return LcmintQuantsim1Accessor()
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


class LcmintQuantsim1Accessor(Accessor, BarAccessor):
    ServerConfig = LcmintQuantsim1()
    ServerConfig.EnrichedOHLCVN.Fields = \
        StrEnum('Fields', {**LcmintQuantsim1.EnrichedOHLCVN.Fields.__members__, 'IN_FILE': 'in_file'})

    def __init__(self):
        super().__init__(self.ServerConfig)

    def fmanager_factory(self, type_name):
        if type_name == LcmintQuantsim1.EnrichedOHLCVN.name():
            return self.EnrichedManager(self.TIME_FROM, self.TIME_TO)
        elif type_name == LcmintQuantsim1.ContinuousContract.name():
            return self.ContinuousContractManager()

    class CommonFileManager(FileManager):
        def __init__(self, config):
            self.config = config

        def directories(self, **kwargs):
            kwargs[self.config.TABLE] = self.config.name()
            return [kwargs.get(x) for x in self.config.FILE_STRUCTURE]

        def find_files(self, filesys, **kwargs):
            raise NotImplementedError

        def get_file_handle(self, filepath, filesys):
            open_func = self.any_compressed_open(filepath, mode='rb')
            return open_func(filesys.open(filepath, mode='rb'))

        def parse_datetime(self, x, **kwargs):
            return parse_datetime(x, self.config.TIMEZONE, **kwargs)

        def get_table(self, fhandle, **kwargs):
            tbclass = self.config
            default_kwargs = dict(
                sep=tbclass.SEPARATOR,
                comment='#',
                index_col=tbclass.TIME_COL_IDX,
                parse_dates=to_iter(tbclass.DATETIME_COLS),
                date_parser=self.parse_datetime,
                skipinitialspace=True,
                skip_blank_lines=True)
            default_kwargs.update(kwargs)
            df = pd.read_table(fhandle, **default_kwargs)
            return df

        def read(self, filepath, filesys, **kwargs):
            with self.get_file_handle(filepath, filesys) as fhandle:
                return self.get_table(fhandle, **kwargs)

    class EnrichedManager(CommonFileManager):
        def __init__(self, arg_time_from, arg_time_to):
            super().__init__(LcmintQuantsim1Accessor.ServerConfig.TABLES
                             [LcmintQuantsim1Accessor.ServerConfig.EnrichedOHLCVN.name()])
            self.arg_time_from = arg_time_from
            self.arg_time_to = arg_time_to
            self.in_file = self.config.Fields.IN_FILE

        def date_from_path(self, path):
            match = re.search(self.config.FILENAME_DATE_PATTERN, basename(path))
            if match is None:
                return None
            return self.parse_datetime(match.group(), format=self.config.FILENAME_DATE_FORMAT)

        def find_files(self, filesys, **kwargs):
            time_from, time_to = kwargs.get(self.arg_time_from), kwargs.get(self.arg_time_to)
            date_from = None if time_from is None else time_from.normalize()
            date_to = None if time_to is None else time_to.normalize()

            years = None if any(x is None for x in (date_from, date_to)) else (date_from.year, date_to.year)
            kwargs.update({self.config.YEAR: years})

            def parse_date(paths):
                for path in paths:
                    if not path.endswith('.csv.gz'):
                        continue
                    try:
                        date = self.date_from_path(path)
                        if date is not None:
                            yield True, date, path
                    except ValueError as e:
                        yield False, e, path

            dir_struct = self.directories(**kwargs)
            files = pd.DataFrame(parse_date(self.rcsv_listdir(filesys, self.config.BASEDIR, dir_struct)))
            if files.empty:
                files = pd.DataFrame(columns=[0, 1, 2])

            valid_files = files[files[0]].set_index(1).sort_index().loc[date_from: date_to, 2]
            invalid_files = files[~files[0]].set_index(1)[2]
            missing_paths = list(self.missing_paths(valid_files, dir_struct, self.config.BASEDIR))
            invalid_files.append(pd.Series(missing_paths, index=['missing directory'] * len(missing_paths)))
            return valid_files, None if invalid_files.empty else invalid_files

        def get_file_handle(self, filepath, filesys):
            fhandle = super().get_file_handle(filepath, filesys)
            line = fhandle.readline().decode()
            while self.config.SEPARATOR not in line:
                line = fhandle.readline().decode()
            match = re.search('(?<= )[^# ]', line)
            offset = 0 if match is None else match.start() - len(line)
            fhandle.seek(offset, 1)
            return fhandle

        def read(self, filepath, filesys, **kwargs):
            df = super().read(filepath, filesys, **kwargs)
            df[self.in_file] = filepath
            return df

    class ContinuousContractManager(CommonFileManager):
        def __init__(self):
            super().__init__(LcmintQuantsim1Accessor.ServerConfig.TABLES
                             [LcmintQuantsim1Accessor.ServerConfig.ContinuousContract.name()])
            self.tz = self.config.TIMEZONE

        def data_file(self, path, files=None):
            # the 'files' argument kept here just for consistency with the usage of callable dir in rcsv_listdir
            basedir, *dirs = path.rsplit(posixpath.sep, len(self.config.FILE_STRUCTURE[:-1]))
            dirs = {n: d for n, d in zip(self.config.FILE_STRUCTURE, dirs)}
            product = dirs[self.config.Tags.PRODUCT]
            return self.config.FILENAME_FORMAT.format(product)

        def find_files(self, filesys, **kwargs):
            kwargs[self.config.DATA_FILE] = self.data_file
            try:
                dir_struct = self.directories(**kwargs)
                valid_files = pd.Series(self.rcsv_listdir(filesys, self.config.BASEDIR, dir_struct))
                missing_paths = list(self.missing_paths(valid_files, dir_struct, self.config.BASEDIR))
                invalid_files = pd.Series(missing_paths, index=['missing directory'] * len(missing_paths))
                return pd.Series(self.rcsv_listdir(filesys, self.config.BASEDIR, self.directories(**kwargs))), \
                       (None if invalid_files.empty else invalid_files)

            except EnvironmentError as e:
                return pd.Series(), pd.Series([e.filename], e)

        def get_file_handle(self, filepath, filesys):
            fhandle = super().get_file_handle(filepath, filesys)

            line = fhandle.readline().decode()
            while self.config.SEPARATOR not in line:
                line = fhandle.readline().decode()
            match = re.search('[^#]', line)
            offset = 0 if match is None else match.start() - len(line)
            fhandle.seek(offset, 1)
            return fhandle

    def transport_session(self):
        transport = paramiko.Transport(self.config.HOSTNAME)
        transport.connect(username=self.config.USERNAME, password=self.config.PASSWORD)
        return transport

    def bound_by_time(self, df, keys, time_from=None, time_to=None, closed=None):
        for key_values, key_df in df.groupby(keys):
            i, j = bound_indices(key_df.index, lambda x: isin_closed(x, time_from, time_to, closed))
            yield key_values, key_df.iloc[i:j]

    def get_data(self, table, empty=None, **kwargs):
        file_mgr = self.fmanager_factory(table)
        time_from, time_to = self.set_arg_time_range(kwargs, file_mgr.config.TIMEZONE)
        closed = self.set_arg_includes(kwargs)

        with self.transport_session() as transport:
            with paramiko.SFTPClient.from_transport(transport) as sftp:
                valid_files, invalid_files = file_mgr.find_files(sftp, **kwargs)
                keys = file_mgr.config.Tags.values()
                if valid_files.empty:
                    return ResultData(empty, keys, invalid_files=invalid_files)

                data_df = pd.concat(file_mgr.read(f, sftp) for f in valid_files)
                if data_df.empty:
                    return ResultData(empty, keys, invalid_files=invalid_files)

                data_df.rename_axis(self.TIME_IDX, inplace=True)
                results = self.bound_by_time(data_df, keys, time_from, time_to, closed)
                return ResultData(results, keys, invalid_files=invalid_files)

    def get_continuous_contracts(self, **kwargs):
        table_config = self.config.TABLES[self.config.ContinuousContract.name()]
        time_from, time_to = self.set_arg_time_range(kwargs, table_config.TIMEZONE)
        new_kwargs = {k: v for k, v in kwargs.items() if k not in [self.TIME_FROM, self.TIME_TO]}
        results = self.get_data(table_config.name(), empty=[], **new_kwargs)

        def valid_contracts(data, tfrom, tto):
            for _, contracts in data:
                contracts = contracts.sort_index()
                filtered = contracts.loc[tfrom: tto]

                if not filtered.empty:
                    start, end = filtered.index[0], None
                    tfrom = filtered.index[0] if tfrom is None else tfrom
                    if start > tfrom:
                        yield (tfrom, start), contracts.loc[:tfrom].iloc[-1]

                    contract = filtered.iloc[0]
                    for i, row in filtered.iloc[1:].iterrows():
                        end = i
                        yield (start, end), contract
                        start = end
                        contract = row

                    if end != tto:
                        yield (start, tto), contract

                elif not contracts.empty:
                    yield (tfrom, tto), contracts.loc[:tfrom].iloc[-1]

        return ResultData(valid_contracts(results, time_from, time_to), invalid_files=results.invalid_files)
