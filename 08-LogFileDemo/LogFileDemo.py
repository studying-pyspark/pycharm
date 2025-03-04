from pyspark.sql import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("LogFileDemo") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.4.4") \
        .getOrCreate()

    file_df = spark.read.text("data/apache_logs.txt")
    file_df.printSchema()
    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]"*)'

    # create a new dataframe with 4 fields
    logs_df = file_df.select(regexp_extract('value', log_reg, 1).alias("ip"),
                             regexp_extract('value', log_reg, 4).alias("date"),
                             regexp_extract('value', log_reg, 6).alias("request"),
                             regexp_extract('value', log_reg, 10).alias("referrer"))

    logs_df.printSchema()

    logs_df \
        .withColumn("referrer", substring_index("referrer", "/", 3)) \
        .groupBy("referrer") \
        .count() \
        .show(100, truncate=False)