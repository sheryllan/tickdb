import stat
import posixpath
from influxdb import DataFrameClient
# from collections import defaultdict

import gzip
import bz2
import lzma
import zipfile
import io
import os

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

    @classmethod
    def any_compressed_open(cls, filepath, **kwargs):
        if filepath.endswith('gz'):
            return lambda x: gzip.open(x, **kwargs)
        elif filepath.endswith('bz2'):
            return lambda x: bz2.open(x, **kwargs)
        elif filepath.endswith('xz'):
            return lambda x: lzma.open(x, **kwargs)
        elif filepath.endswith('zip'):
            return lambda x: zipfile.ZipFile(x, **kwargs)
        else:
            return lambda x: x if isinstance(x, io.IOBase) else open(x, **kwargs)

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

    @staticmethod
    def tree():
        return defaultdict(FileManager.tree)

    @classmethod
    def path_tree_from_lists(cls, dirs, root=None):
        root = cls.tree() if root is None else root
        if not dirs:
            return None

        for dir_name in to_iter(dirs[0], ittype=iter):
            root[dir_name] = cls.path_tree_from_lists(dirs[1:], root[dir_name])

        return root

    @classmethod
    def path_tree_from_str(cls, path, base_path='', root=None):
        base_len = len(base_path) if path.startswith(base_path) else 0
        dirs = list(filter(None, path[base_len:].split(posixpath.sep)))
        return cls.path_tree_from_lists(dirs, root)

    @classmethod
    def missing_paths(cls, listed_paths, dir_struct, base_path=''):
        dir_tree = cls.tree()
        for lp in listed_paths:
            if not lp.startswith(base_path):
                raise ValueError(f'Invalid listed path: {lp} must have base path of {base_path}')
            cls.path_tree_from_str(lp, base_path, dir_tree)

        def rcsv_check(subdir_struct, subdir_tree, path_stack=None):
            path_stack = [] if path_stack is None else path_stack
            if not subdir_struct:
                return
            if subdir_tree is None:
                raise ValueError('Incompatible input parameters: '
                                 'the depth of subdir_tree is shorter than the length of subdir_struct')
            subdirs = list(subdir_tree) if subdir_struct[0] is None else to_iter(subdir_struct[0], ittype=iter)
            for subdir in subdirs:
                path_stack.append(subdir)
                if subdir in subdir_tree:
                    yield from rcsv_check(subdir_struct[1:], subdir_tree[subdir], path_stack)
                else:
                    yield posixpath.join(base_path, *path_stack)
                path_stack.pop()

        yield from rcsv_check(to_iter(dir_struct), dir_tree)

    def find_files(self, **kwargs):
        raise NotImplementedError


# print(list(FileManager.missing_paths(
#     ['/home/user/foo/boo/temp/file.txt', '/home/user/foo/doo/beep.pdf', '/home/user/lll/hello.py'],
#     ['user', ['foo', 'lll', 'kkk'], None], '/home')))
