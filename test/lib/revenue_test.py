from src.lib.utils.sql_reader import SQLReader
from test.data.revenue_data import Data

def test_new_customer(spark_session):
    # arrange
    order_log = Data.order_log(spark_session=spark_session)
    order_log.createTempView(name='order_log')
    new_customer = Data.new_customer(spark_session=spark_session)
    query = SQLReader().read_sql(query_name='new_customer')

    # act
    actual_data_frame = spark_session.sql(sqlQuery=query)
    actual_count = new_customer.intersect(actual_data_frame).count()

    # assert
    assert 4 == actual_count


def test_revenue(spark_session):
    # arrange
    order_log = Data.order_log(spark_session=spark_session)
    order_log.createTempView(name='order_log')
    new_customer = Data.new_customer(spark_session=spark_session)
    new_customer.createTempView(name='new_customer')
    revenue = Data.revenue(spark_session=spark_session)
    query = SQLReader().read_sql(query_name='revenue')

    # act
    actual_data_frame = spark_session.sql(sqlQuery=query)
    actual_count = revenue.intersect(actual_data_frame).count()

    # assert 
    assert 3 == actual_count
