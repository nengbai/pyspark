import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(app_name="ExampleApp", master="local[*]"):
    """创建 SparkSession"""
    logger.info(f"Creating SparkSession with app_name={app_name} and master={master}")
    return SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .getOrCreate()

def read_csv_file(spark, file_path):
    """读取 CSV 文件"""
    try:
        logger.info(f"Reading CSV file from {file_path}")
        return spark.read.csv(file_path, header=True, inferSchema=True)
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        return None


def filter_data(df, column_name, threshold):
    """过滤数据"""
    logger.info(f"Filtering data by {column_name} with threshold {threshold}")
    return df.filter(df[column_name] >= threshold)

def user_defined_function(x,y):
    """自定义函数"""
    logger.info(f"User defined function called with {x,y}")
    if isinstance(x, str) and x.isdigit() and isinstance(y, str) and y.isdigit():
        return int(x) * int(y)
    elif isinstance(x, int) and isinstance(y, int):
        return x * y
    else:
        return None 
    
def spark_udf(spark, df):
    """Spark UDF"""
    logger.info(f"Registering user defined function and applying it to DataFrame.")
    df.createOrReplaceTempView("my_table")
    spark.udf.register("user_defined_function", user_defined_function,IntegerType())
    df_udf = spark.sql("SELECT no, user_defined_function(age1,age2) as age FROM my_table")
    return df_udf
def main():
    # 创建 SparkSession
    spark = create_spark_session()
  

    # 读取 CSV 文件
    file_path = "data.csv"
    df = read_csv_file(spark, file_path)
    if df is not None:
        # 处理 df1
        df1 = df.select("no", "age1", "age2").withColumnRenamed("age1", "new_age").withColumnRenamed("age2", "new_age1")

        # 处理 df3
        df3 = df.select("no", "age1", "age2").withColumnRenamed("age1", "new_age2").withColumnRenamed("age2", "new_age3")

        df_join = df1.join(df3, "no", "inner")
        df_join.summary()
        # 显示 df3 的前 10 行
        df_join.show(10)
       

        # 调用UDF函数，生成新的dataform,并返回前10行数据
        df_udf = spark_udf(spark, df)
        logger.info("Result of UDF application:")
        df_udf.show(20)
        

        # 执行一些基本操作
        df_filtered = filter_data(df, "age2", 20)
        logger.info("Filtered DataFrame by age2 with threshold 20:")
        df_filtered.show(10)
    else:
        logger.error("Error reading CSV file")
         # 停止 SparkSession
        spark.stop()
        return
    # 停止 SparkSession
    spark.stop()
   

if __name__ == "__main__":
    main()