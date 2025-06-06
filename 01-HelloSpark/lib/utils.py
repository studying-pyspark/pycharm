import configparser
from pyspark import SparkConf

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for(key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
        return spark_conf

def load_survey_df(spark,data_file): # spark_session, file_location
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(data_file)
    # logger.info("Finished HelloSpark")
    # spark.stop()

def count_by_country(survey_df):
    return survey_df \
    .where("Age < 40") \
    .select("Age", "Gender", "Country", "state") \
    .groupBy("Country") \
    .count()
