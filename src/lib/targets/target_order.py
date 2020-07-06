from os.path import join

class TargetOrderLog:
    def __init__(self, target):
        self._target = target

    def _path(self, table_name):
        return join(self._target, table_name)

    def read_csv(self, spark, table):
        path = self._path(table.name)
        data_frame = spark.read.csv(
            path=path, 
            sep=table.delimiter, 
            header=table.header, 
            schema=table.schema, 
            timestampFormat="yyyy-MM-dd HH:mm:ss"
        )
        data_frame.createTempView(name=table.name)