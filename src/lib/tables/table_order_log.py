from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructType,
    StructField,
    TimestampType
)

class OrderLogTable:
    @property
    def name(self):
        return 'order_log'

    @property
    def delimiter(self):
        return ','

    @property
    def header(self):
        return True

    @property
    def schema(self):
        return StructType(fields=[
            StructField(name='index',       dataType=IntegerType(),   nullable=True),
            StructField(name='order_id',    dataType=LongType(),      nullable=True),
            StructField(name='customer_id', dataType=StringType(),    nullable=True),
            StructField(name='amount',      dataType=DoubleType(),    nullable=True),
            StructField(name='created_at',  dataType=TimestampType(),    nullable=True)
        ])