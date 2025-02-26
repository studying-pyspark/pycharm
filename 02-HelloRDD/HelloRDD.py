# create a SparkConf object
# create a Spark Session using SparkConf
# use the spark session to read the data file
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
# from lib.logger import Log4j
import sys
from collections import namedtuple

SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])
if __name__ == "__main__":
    conf = SparkConf() \
        .setMaster("local[3]") \
        .setAppName("HelloRDD")
    # sc = SparkContext(conf=conf)

    spark = SparkSession \
        .builder \
        .config(conf = conf) \
        .getOrCreate()

    # sc = spark.sparkContext(conf=conf)
    #logger = Log4j(spark)
    sc = spark.sparkContext

    if len(sys.argv) != 2:
        #logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    linesRDD = sc.textFile(sys.argv[1])

    # Transformation & Action
    # RDD havs no schema or a row/column structure
    # just  lines of file
    partitionedRDD = linesRDD.repartition(2)

    # double quotes 제거 + comma 기준으로 word split
    colsRDD = partitionedRDD.map(lambda line: line.replace('"', '').split(","))
    # namedtuple 생성하여 스키마와 데이터 타입 부여
    selectRDD = colsRDD.map(lambda cols: SurveyRecord(int(cols[1]), cols[2], cols[3], cols[4]))
    filteredRDD = selectRDD.filter(lambda r: r.Age < 40)
    kvRDD = filteredRDD.map(lambda r: (r.Country, 1)) # key: Country, value: 1
    # reduceByKey를 사용해 같은 국가의 값을 합산.
    # 국가별로 1씩 맵핑해서 같은 국가의 값을 합산 -> 국가별 인원수 합산
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1+v2)

    colsList = countRDD.collect()

    for x in colsList:
        print(x)
