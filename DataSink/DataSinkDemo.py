from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.types import *


if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.4.4") \
        .getOrCreate()

    # Read the data sources
    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/flight*.parquet")

    # Write the DataFrames as an Avro output.
    flightTimeParquetDF.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "avro/") \
        .save()

    print("Num Partitions before: " + str(flightTimeParquetDF.rdd.getNumPartitions()))
    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()

    # Make 5 partitions
    partitionedDF = flightTimeParquetDF.repartition(5)
    print("Num Partitions after: " + str(partitionedDF.rdd.getNumPartitions()))
    partitionedDF.groupBy((spark_partition_id())).count().show()


    partitionedDF.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "json/") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordsPerFile", 10000) \
        .save()