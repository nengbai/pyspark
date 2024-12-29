import os
import logging
import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark import SparkConf
from random import random
from demo import load_config, create_spark_session, estimate_pi, read_data_from_postgres, main

class TestPySparkApp(unittest.TestCase):

    @patch.dict(os.environ, {"POSTGRES_USER": "test_user", "POSTGRES_PASSWORD": "test_password", "TASK_SCALE": "100000", "JDBC_PATH": "/test/jdbc/path"})
    def test_load_config(self):
        config = load_config()
        self.assertEqual(config['POSTGRES_USER'], 'test_user')
        self.assertEqual(config['POSTGRES_PASSWORD'], 'test_password')
        self.assertEqual(config['TASK_SCALE'], 100000)
        self.assertEqual(config['JDBC_PATH'], '/test/jdbc/path')

    def test_create_spark_session(self):
        spark = create_spark_session("/test/jdbc/path")
        self.assertIsInstance(spark, SparkSession)
        self.assertEqual(spark.conf.get("spark.driver.extraClassPath"), "/test/jdbc/path")
        spark.stop()

    @patch('random.random', side_effect=[0.5, 0.5, 0.3, 0.4, 0.6, 0.7, 0.8, 0.9, 0.1, 0.2])
    def test_estimate_pi(self, mock_random):
        spark = SparkSession.builder.appName("test").getOrCreate()
        estimate_pi(10, spark)
        spark.stop()

    @patch('pyspark.sql.DataFrameReader.load')
    @patch('pyspark.sql.DataFrame.show')
    @patch('pyspark.sql.DataFrame.createOrReplaceTempView')
    @patch('pyspark.sql.SparkSession.sql')
    def test_read_data_from_postgres(self, mock_sql, mock_createOrReplaceTempView, mock_show, mock_load):
        spark = SparkSession.builder.appName("test").getOrCreate()
        mock_df = MagicMock()
        mock_load.return_value = mock_df
        mock_sql.return_value = mock_df
        read_data_from_postgres(spark, "jdbc:postgresql://127.0.0.1:5432/demo", {"user": "test_user", "password": "test_password", "driver": "org.postgresql.Driver"})
        spark.stop()

    @patch('pyspark.sql.SparkSession.stop')
    @patch('pyspark.sql.SparkSession.builder')
    @patch('random.random')
    @patch('pyspark.sql.DataFrameReader.load')
    @patch('pyspark.sql.DataFrame.show')
    @patch('pyspark.sql.DataFrame.createOrReplaceTempView')
    @patch('pyspark.sql.SparkSession.sql')
    def test_main(self, mock_sql, mock_createOrReplaceTempView, mock_show, mock_load, mock_random, mock_builder, mock_stop):
        mock_spark = MagicMock()
        mock_builder.appName.return_value = mock_spark
        mock_spark.config.return_value = mock_spark
        mock_spark.getOrCreate.return_value = mock_spark
        mock_df = MagicMock()
        mock_load.return_value = mock_df
        mock_sql.return_value = mock_df
        main()
        mock_spark.stop.assert_called_once()

if __name__ == '__main__':
    unittest.main()