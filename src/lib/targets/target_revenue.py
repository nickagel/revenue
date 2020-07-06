from os.path import join

class TargetRevenue:
    def __init__(self, target):
        self._target = target

    def _path(self, table_name):
        return join(self._target, table_name)

    def write_csv(self, data_frame, table):
        path = self._path(table.name)
        data_frame.coalesce(1).write.csv(
            path=path, 
            header=table.header, 
            mode='overwrite', 
            sep=table.delimiter, 
            compression= None
        ) 
