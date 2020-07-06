# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pytest import fixture
import logging


@fixture(scope='session')
def spark_context(request):
    spark_conf = (SparkConf().setMaster('local[2]').setAppName('local-test'))
    spark_context = SparkContext(conf=spark_conf)
    request.addfinalizer(lambda: spark_context.stop())
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)
    return spark_context

@fixture(scope='function')
def spark_session(spark_context):
    return SparkSession(sparkContext=spark_context).newSession()