from pyspark.sql.types import DecimalType, StringType, StructType, StructField
class RevenueTable:
    @property
    def name(self):
        return 'revenue'

    @property
    def delimiter(self):
        return ','

    @property
    def header(self):
        return True

    @property
    def schema(self):
        return StructType(fields=[
            StructField(name='date', dataType=StringType(), nullable=False),
            StructField(name='revenue', dataType=DecimalType(17,2), nullable=False),
            StructField(name='new_revenue_percentage', dataType=DecimalType(5,2), nullable=False),
            StructField(name='revenue_from_new_customers', dataType=DecimalType(17,2), nullable=False)
        ])