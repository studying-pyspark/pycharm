# 1. How to use DataFrame for CSV, JSON, and Parquet
# 1. Spark Data Types
# 2. How to explicitly define a schema for your data

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    # column anme and data type separated by a comma.
    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING,
    ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT,
    WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT
    """


    # 현재 스키마 정의 안함. inferSchema도 정의 안함.(모두 sting으로 인식됨.)
    flightTimeCsvDF = spark.read \
        .format("csv") \
        .option('header', "true") \
        .schema(flightSchemaStruct) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/y") \
        .load("data/flight*.csv")

    flightTimeCsvDF.show(5)
    print("CSV Schema: "+ flightTimeCsvDF.schema.simpleString())

    # json & parquet 파일을 읽어올때는 항상 schema를 자동적으로 추론하기 때문에, inferSchema 옵션을 사용할 필요가 X.
    flightTimeJsonDF = spark.read \
        .format("json") \
        .schema(flightSchemaDDL)\
        .option("dateFormat", "M/d/y") \
        .load("data/flight*.json")

    flightTimeJsonDF.show(5)
    print("JSON Schema: " + flightTimeJsonDF.schema.simpleString())

    flightTimeParquetDF = spark.read \
            .format("parquet") \
            .load("data/flight*.parquet")

    flightTimeParquetDF.show(5)
    print("PARQUET Schema: " + flightTimeParquetDF.schema.simpleString())



