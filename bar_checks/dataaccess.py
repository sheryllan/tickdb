from influxcommon import *
from influxdb import DataFrameClient
import paramiko


class BaseAccesor(object):

    def get_data(self, **kwargs):
        raise NotImplementedError



class FileAccessor(BaseAccesor):
    def __init__(self, host, basedir, username, password):
        self.host = host
        self.basedir = basedir
        self.username = username
        self.password = password


    def find_file(self, product, ptype, tbname, expiry, clocktype, ):
        with paramiko.SSHClient() as ssh_client:
            ssh_client.connect(hostname=self.host, username=self.username, password=self.password)
            file_path = os.path.join(self.basedir, product)
            sftp_client = ssh_client.open_sftp()
            with sftp_client.open(file_path) as remote_file:
                
                return remote_file.read()




    def get_data(self, tbname, time_from=None, time_to=None, include_from=True, include_to=True,
                 others=None, empty=None, **kwargs):
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
