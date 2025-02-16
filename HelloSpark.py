import os
import sys
from pyspark.sql import *
from lib.logger import log4J
from lib.utils import get_spark_app_config, load_survey_df
from pyspark import SparkConf


log_dir = "logs"
print("HADOOP_HOME:", os.environ.get("HADOOP_HOME"))
if not os.path.exists(log_dir):
    os.makedirs(log_dir) #log 폴더 없으면 생성
if __name__ == "__main__":
    conf = get_spark_app_config()
    # create pyspark session
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)
    logger.info("Starting HelloSpark")
    survey_df = load_survey_df(spark, sys.argv[1])
    survey_df.show()
    logger.info("Finished HelloSpark")
    # spark.stop()

    # create a new Class for handling log4J and expose simple and easy to use methods to create a log entry
