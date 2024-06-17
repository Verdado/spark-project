from datetime import date, datetime
from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.getOrCreate()


def create_dataframe():
    df = spark.createDataFrame([
        (1, 2.0, 'acer', date(2021, 1, 1), datetime(2021, 1, 1, 12, 0)),
        (2, 3.0, 'asus', date(2022, 2, 1), datetime(2022, 1, 2, 12, 0)),
        (2, 3.0, 'predator', date(2022, 2, 1), datetime(2022, 1, 2, 12, 0)),
        (2, 3.0, 'legion', date(2022, 2, 1), datetime(2022, 1, 2, 12, 0)),
        (3, 4.0, 'hp', date(2023, 3, 1), datetime(2023, 1, 3, 12, 0)),
        (3, 4.0, 'ibm', date(2024, 3, 1), datetime(2024, 1, 3, 12, 0))
    ], schema='id long, precision double, model string, d date, e timestamp')
    return df


def transform_get_all_data_year_2022() -> DataFrame:
    df = create_dataframe()
    """
    Write your transformation logic here using df
    """
    return df
