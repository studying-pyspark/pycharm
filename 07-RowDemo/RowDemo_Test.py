from datetime import date
from gc import collect
from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import StringType, StructType, StructField, Row

class RowDemoTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("RowDemoTest") \
            .getOrCreate()

        my_schema = StructType([
            StructField("ID", StringType()),
            StructField("EventDate", StringType())
        ])

        my_rows = [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]
        my_rdd = cls.spark.sparkContext.parallelize(my_rows, 2)
        cls.my_df = cls.spark.createDataFrame(my_rdd, my_schema)

    # String을 날짜 형식으로 transformation
    def to_date_df(self, df, fmt, fld):
        return df.withColumn(fld, to_date(col(fld), fmt))

    # 올바른 DateType으로 변환 되었는지 검증
    def test_data_type(self):
        rows = self.to_date_df(self.my_df, "M/d/y", "EventDate").collect()
        for row in rows:
            self.assertIsInstance(row["EventDate"], date)
    # 값이 정확하게 변환되었는지를 검증
    def test_date_value(self):
        rows = self.to_date_df(self.my_df, "M/d/y", "EventDate").collect()
        for row in rows:
            self.assertEqual(row["EventDate"], date(2020,4,5))
