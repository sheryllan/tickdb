import stat
import posixpath
from influxdb import DataFrameClient

import gzip
import bz2
import lzma
import zipfile

from influxcommon import *


class Accessor(object):
    def __init__(self, config):
        self.config = config

    def get_data(self, **kwargs):
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


class FileManager(object):
    def __init__(self, config):
        self.config = config

    @classmethod
    def any_compressed_open(cls, filepath):
        if filepath.endswith('gz'):
            return gzip.open
        elif filepath.endswith('bz2'):
            return bz2.open
        elif filepath.endswith('xz'):
            return lzma.open
        elif filepath.endswith('zip'):
            return zipfile.ZipFile
        else:
            return open

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


