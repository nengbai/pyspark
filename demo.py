import os
import logging
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf

# 配置日志记录
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 从配置文件中获取敏感信息
def load_config():
    config = {
        'POSTGRES_USER': os.getenv('POSTGRES_USER', 'postgres'),
        'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD', 'casa1234'),
        'TASK_SCALE': int(os.getenv('TASK_SCALE', 1000000)),
        'JDBC_PATH': os.getenv('JDBC_PATH', "/Users/baineng/Documents/devops/spark/apps/jars/postgresql-42.7.4.jar")
    }
    return config

config = load_config()

# 创建SparkSession对象，并设置相关参数
def create_spark_session(jdbc_path):
    conf = SparkConf()
    conf.set("spark.driver.extraClassPath", jdbc_path)
    return SparkSession.builder \
        .appName("pyspark_postgresql") \
        .config(conf=conf) \
        .getOrCreate()

# 并行计算 Pi
def estimate_pi(n, spark):
    def inside(p):
        from random import random
        x, y = random(), random()
        return x*x + y*y <= 1

    count = spark.sparkContext.parallelize(range(0, n), 4).filter(inside).count()
    pi = 4 * count / n
    logger.info(f"Estimated value of Pi: {pi}")

# 读取Postgres SQL数据库中的数据
def read_data_from_postgres(spark, url, properties):
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", "test") \
            .option("user", properties["user"]) \
            .option("password", properties["password"]) \
            .option("driver", properties["driver"]) \
            .load()
        
        df.show()
        df.createOrReplaceTempView("tempTable")
        query_result = spark.sql("SELECT * FROM tempTable order by id")
        query_result.show()
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)

# 主函数
def main():
    spark = create_spark_session(config['JDBC_PATH'])
    try:
        estimate_pi(config['TASK_SCALE'], spark)
        url = "jdbc:postgresql://127.0.0.1:5432/demo"
        properties = {
            "user": config['POSTGRES_USER'],
            "password": config['POSTGRES_PASSWORD'],
            "driver": "org.postgresql.Driver"
        }
        read_data_from_postgres(spark, url, properties)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()