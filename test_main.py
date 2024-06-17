from datetime import date, datetime
from unittest import TestCase

from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual
from main import transform_get_all_data_year_2022


class Test(TestCase):

    def test_transform_get_all_data_year_2022(self):
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame([
            (2, 3.0, 'asus', date(2022, 2, 1), datetime(2022, 1, 2, 12, 0)),
            (2, 3.0, 'predator', date(2022, 2, 1), datetime(2022, 1, 2, 12, 0)),
            (2, 3.0, 'legion', date(2022, 2, 1), datetime(2022, 1, 2, 12, 0))
        ], schema='id long, precision double, model string, d date, e timestamp')
        assertDataFrameEqual(transform_get_all_data_year_2022(), df)
