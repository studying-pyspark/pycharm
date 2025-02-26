from pyspark.sql import *
# Create a managed table  and save DataFame to the Spark Table

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSQLTableDemo") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.4.4") \
        .config("spark.jars.packages", "org.apache.spark:spark-hive_2.12:3.4.4") \
        .enableHiveSupport() \
        .getOrCreate()

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    # bucketBy: allows user to restrict the number of partitions
    flightTimeParquetDF.write \
        .format("csv") \
        .mode("overwrite") \
        .bucketBy(5, "OP_CARRIER", "ORIGIN") \
        .sortBy("OP_CARRIER", "ORIGIN") \
        .saveAsTable("flight_data_tbl")

    print(spark.catalog.listTables("AIRLINE_DB"))


