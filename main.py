from pyspark.sql import SparkSession
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import json
import yaml
from datetime import datetime,timedelta, timezone
import argparse

# 自定义 JsonFormatter 类
class JsonFormatter(logging.Formatter):
    def format(self, record):
        """
        格式化日志记录为 JSON 格式
        :param record: 日志记录
        :return: JSON 格式的日志记录字符串
        """
        east_8 = timezone(timedelta(hours=8))
        log_record = {
            "time": datetime.fromtimestamp(record.created,tz=east_8).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "level": record.levelname,
            "message": record.getMessage(),
            "name": record.name,
            "module": record.module,
            "filename": record.filename,
            "lineno": record.lineno,
            "funcName": record.funcName
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)

# 配置日志
def configure_logging(log_level=logging.INFO):
    """
    配置日志记录
    :param log_level: 日志级别，默认为 INFO
    """
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file = os.path.join(log_dir, 'app.log')
    
    handler = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=7)
    handler.suffix = "%Y-%m-%d"
    formatter = JsonFormatter()
    handler.setFormatter(formatter)
    
    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.addHandler(handler)

def create_spark_session(app_name="ExampleApp", master="local[*]"):
    """
    创建 SparkSession
    :param app_name: 应用名称
    :param master: Spark master URL
    :return: SparkSession 实例
    """
    return SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .getOrCreate()

def read_csv_file(spark, file_path):
    """
    读取 CSV 文件
    :param spark: SparkSession 实例
    :param file_path: 文件路径
    :return: DataFrame
    """
    try:
        return spark.read.csv(file_path, header=True, inferSchema=True)
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        spark.stop()
        raise  # 重新抛出异常以便上层处理

def process_data(spark, df,output_file):
    """
    处理数据
    :param spark: SparkSession 实例
    :param df: DataFrame
    :param output_file: 输出文件路径
    """
    required_columns = ["age1", "age2"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logging.error(f"Missing required columns: {missing_columns}")
        spark.stop()
        raise ValueError(f"Missing required columns: {missing_columns}")
    logging.info("Processing data using Spark SQL: create a temporary view 'temp_view' for SQL queries.")
    df.createOrReplaceTempView("temp_view")  # create a temporary view for SQL queries.
    logging.info("Processing data using Spark SQL: select sum(age1+age2) as new_age, count(*) as count from temp_view where age2 >= 20.")
    new_df = spark.sql("""
        SELECT 
            sum(coalesce(age1, 0) + coalesce(age2, 0)) as new_age, 
            count(*) as count 
        FROM temp_view 
        WHERE age2 >= 20
        """)
    logging.info(f"Processing data using Spark SQL: write the result to {output_file}.")
    new_df.write.csv(output_file, header=True, mode="overwrite")
    logging.info("Finished processing data. ")

def filter_data(spark, df, column_name, threshold):
    """
    过滤数据
    :param spark: SparkSession 实例
    :param df: DataFrame
    :param column_name: 列名
    :param threshold: 阈值
    :return: 过滤后的 DataFrame
    """
    logging.info(f"Filtering data by {column_name} with threshold {threshold}")
    if column_name not in df.columns:
        logging.error(f"Column {column_name} does not exist in DataFrame")
        spark.stop()
        raise ValueError(f"Column {column_name} does not exist in DataFrame")
    
    df.withColumn("age2", 100 - df[column_name]).filter(df["age2"] >= threshold).show(10)
    logging.info("Finished filtering data: filtered data with age2 >= 20")
    return df.filter(df[column_name] >= threshold)

def main(config):
    """
    主函数
    :param config: 配置字典
    """
    # 配置日志级别
    configure_logging(config.get('log_level', logging.INFO))

    # 创建 SparkSession
    logging.info("Starting Spark and creating SparkSession...")
    spark = create_spark_session(app_name=config['app']['app_name'], master=config['app']['master'])
    logging.info("SparkSession created successfully")

    try:
        # 读取 CSV 文件
        file_path = config['path']['input_dir'] +'/'+ config['file']['input_file']
        print(f"file_path: {file_path}")
        logging.info(f"Reading CSV file {file_path}...")
        df = read_csv_file(spark, file_path)

        # 显示前几行数据
        logging.info("Displaying first 5 rows of data...")
        df.show(5)

        # 执行一些基本操作
        logging.info("Performing some basic operations...")
        df_filtered = filter_data(spark, df, "age2", 20)
        df_filtered.show(10)

        # Spark SQL 处理数据
        logging.info("Processing data using Spark SQL...")
        output_file = config['path']['output_dir'] +'/'+ config['file']['output_file']
        print(f"file_path: {output_file}")
        process_data(spark, df,output_file)

    except Exception as e:
        logging.error(f"Error creating SparkSession: {e}")
    finally:
        # 停止 SparkSession
        logging.info("Stopping SparkSession...")
        spark.stop()
        logging.info("Finished processing data")
    
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument('--config', type=str, default='config/config.yaml', help='Path to the configuration file')
    args = parser.parse_args()
    
    try:
        with open(args.config, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
        print(f"config: {config}")
        main(config)
    except FileNotFoundError:
        logging.error(f"Configuration file {args.config} not found.")
    except yaml.YAMLError as exc:
        logging.error(f"Error in configuration file: {exc}")