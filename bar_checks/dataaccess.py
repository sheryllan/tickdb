from influxcommon import *
from influxdb import DataFrameClient
import paramiko, stat
import datetime as dt


class BaseAccesor(object):

    def get_data(self, **kwargs):
        raise NotImplementedError



class FileAccessor(BaseAccesor):
    def __init__(self, settings, username, password):
        self.settings = settings
        self.host = settings.HOSTNAME
        self.basedir = settings.BASEDIR
        self.username = username
        self.password = password


    def transport_session(self):
        transport = paramiko.Transport(self.host)
        transport.connect(username=self.username, password=self.password)
        return transport

    def _rcsv_listdir(self, sftp, path, dirs):
        if stat.S_ISREG(sftp.stat('.').st_mode):
            yield sftp.stat('.').filename
            sftp.chdir('..')
            return
        sftp.chdir(path)

        if not dirs:
            yield from sftp.listdir('.')
            sftp.chdir('..')
            return
        subdirs = sftp.listdir('.') if dirs[0] is None \
            else set(sftp.listdir('.')).intersection(to_iter(dirs[0], ittype=iter))

        for subdir in subdirs:
            yield from self._rcsv_listdir(sftp, subdir, dirs[1:])
        sftp.chdir('..')

    def find_files(self, **kwargs):
        dirs = [kwargs.get(x, None) for x in self.settings.FILE_STRUCTURE]

        with self.transport_session() as transport:
            with paramiko.SFTPClient.from_transport(transport) as sftp:
                yield from self._rcsv_listdir(sftp, self.basedir, dirs)


    def get_data(self, tbname, time_from: dt.datetime=None, time_to: dt.datetime=None, closed=None, empty=None, **kwargs):

        time_from = None if time_from is None else time_from.
        file_path = self.find_file(**kwargs)



class InfluxdbAccessor(BaseAccesor):
    def __index__(self, host, port, database):
        self.client = DataFrameClient(host=host, port=port, database=database)


    def get_data(self, tbname, time_from=None, time_to=None, include_from=True, include_to=True,
                 others=None, empty=None, **kwargs):
        terms = [where_term(k, v) for k, v in kwargs.items()]
        terms = terms + time_terms(time_from, time_to, include_from, include_to)
        clauses = where_clause(terms) if others is None else [where_clause(terms)] + to_iter(others)
        qstring = select_query(tbname, clauses=clauses)
        return self.client.query(qstring).get(tbname, empty)


from bar.datastore_config import Lcmquantldn1
fa = FileAccessor(Lcmquantldn1, 'slan', 'slanpass')
r = list(fa.find_files(product='ES', type='F', expiry='SEP2018', table='EnrichedOHLCVN', clock='M'))
for f in r:
    print(f)
