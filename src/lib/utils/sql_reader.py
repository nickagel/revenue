
from os import getcwd
from os.path import join


class SQLReader:
    def __init__(self):
        self._root_path = join('/src', 'lib', 'sql')
        self._ext = '.sql'

    def _path(self, query_name):
        sql_file = query_name + self._ext
        return join(self._root_path, sql_file)

    def read_sql(self, query_name):
        path = self._path(query_name=query_name)
        return open(path, "r").read()