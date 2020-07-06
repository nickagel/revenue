from lib.targets.target_order import TargetOrderLog
from lib.targets.target_revenue import TargetRevenue
from lib.utils.sql_reader import SQLReader
from lib.tables.table_order_log import OrderLogTable 
from lib.tables.table_revenue import RevenueTable
from pyspark.sql import SparkSession

class Revenue(object):

    spark = SparkSession.builder.getOrCreate()
    targetOrderLog = TargetOrderLog(target='/root/data/prod')
    targetOrderLog.read_csv(spark, OrderLogTable())

    queries = [
        ('new_customer', True),
        ('revenue', False)
    ]
    data_frames=[]
    for query_name, tempView in queries:
        query = SQLReader().read_sql(query_name=query_name)
        data_frame = spark.sql(query)
        if tempView:
            data_frame.createTempView(query_name)
        data_frames.append(data_frame)
    results = data_frames[-1]


    output_table = RevenueTable()
    targetRevenue = TargetRevenue(target='/root/data/results')
    targetRevenue.write_csv(data_frame=results, table=output_table)

if __name__ == '__main__':
    Revenue()