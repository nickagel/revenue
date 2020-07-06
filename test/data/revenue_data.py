from src.lib.tables.order_log import OrderLogTable
from collections import namedtuple
from datetime import datetime
from decimal import Decimal
from pyspark.sql.types import DoubleType, DecimalType, StringType, StructType, StructField

class Data:
    @staticmethod
    def order_log(spark_session):
        OL = namedtuple('CO',  ['index', 'order_id', 'customer_id', 'amount', 'created_at'])
        return spark_session.createDataFrame(data=[
            OL(index=0, order_id=2147328163975, customer_id='cus_00001', amount=10.2, created_at=datetime(2020, 4, 13, 22, 0, 41)), 
            OL(index=1, order_id=2147322200199, customer_id='cus_00002', amount=10.2, created_at=datetime(2020, 4, 13, 21, 57, 28)), 
            OL(index=2, order_id=2147301163143, customer_id='cus_00003', amount=10.2, created_at=datetime(2020, 4, 13, 21, 47, 14)), 
            OL(index=3, order_id=2147301163143, customer_id='cus_00003', amount=10.2, created_at=datetime(2020, 4, 23, 21, 47, 14)), 
            OL(index=4, order_id=2147300081799, customer_id='cus_00001', amount=20.0, created_at=datetime(2020, 5, 13, 21, 46, 40)), 
            OL(index=5, order_id=2147299360903, customer_id='cus_00002', amount=20.5, created_at=datetime(2020, 5, 13, 21, 46, 13)), 
            OL(index=6, order_id=2147297230983, customer_id='cus_00003', amount=20.67, created_at=datetime(2020, 5, 13, 21, 45, 14)), 
            OL(index=7, order_id=2147297230983, customer_id='cus_00003', amount=20.67, created_at=datetime(2020, 5, 23, 21, 45, 14)), 
            OL(index=8, order_id=2147296444551, customer_id='cus_00004', amount=20.91, created_at=datetime(2020, 5, 13, 21, 44, 49)), 
            OL(index=9, order_id=2147296444551, customer_id='cus_00004', amount=20.91, created_at=datetime(2020, 5, 23, 21, 44, 49)), 
            OL(index=10, order_id=2147262333063, customer_id='cus_00001', amount=30.1, created_at=datetime(2020, 6, 13, 21, 26, 38)), 
            OL(index=11, order_id=2147262234759, customer_id='cus_00002', amount=30.2, created_at=datetime(2020, 6, 13, 21, 26, 41)), 
            OL(index=12, order_id=2147262136455, customer_id='cus_00003', amount=30.3, created_at=datetime(2020, 6, 13, 21, 26, 39)), 
            OL(index=13, order_id=2147252535431, customer_id='cus_00004', amount=30.4, created_at=datetime(2020, 6, 13, 21, 21, 40))
        ], schema=OrderLogTable().schema)

    @staticmethod
    def new_customer(spark_session):
        schema = StructType(fields=[
            StructField(name='customer_id', dataType=StringType(), nullable=False),
            StructField(name='year_month', dataType=StringType(), nullable=False)
        ])
        NC = namedtuple('NC', [ 'customer_id', 'year_month' ])
        return spark_session.createDataFrame(data=[
            NC(customer_id='cus_00004', year_month='2020-05'), 
            NC(customer_id='cus_00002', year_month='2020-04'), 
            NC(customer_id='cus_00003', year_month='2020-04'), 
            NC(customer_id='cus_00001', year_month='2020-04')
        ], schema=schema)

    @staticmethod
    def revenue(spark_session):
        schema = StructType(fields=[
            StructField(name='date', dataType=StringType(), nullable=False),
            StructField(name='revenue', dataType=DecimalType(17,2), nullable=False),
            StructField(name='new_revenue_percentage', dataType=DecimalType(5,2), nullable=False),
            StructField(name='revenue_from_new_customers', dataType=DecimalType(17,2), nullable=False)
        ])
        R = namedtuple('R', ['date', 'revenue', 'new_revenue_percentage', 'revenue_from_new_customers'])
        return spark_session.createDataFrame(data=[
            R(date='2020-06', revenue=Decimal('121.00'), new_revenue_percentage=Decimal('0.00'), revenue_from_new_customers=Decimal('0.00')), 
            R(date='2020-05', revenue=Decimal('123.66'), new_revenue_percentage=Decimal('33.82'), revenue_from_new_customers=Decimal('41.82')), 
            R(date='2020-04', revenue=Decimal('40.80'), new_revenue_percentage=Decimal('100.00'), revenue_from_new_customers=Decimal('40.80'))
        ], schema=schema)