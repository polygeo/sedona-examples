from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from sedona.spark import *

config = SedonaContext.builder(). \
    config('spark.jars.packages',
           'org.apache.sedona:sedona-spark-3.5_2.12:1.6.1,'
           'org.datasyslab:geotools-wrapper:1.7.0-28.5'). \
    config('spark.jars.repositories', 'https://artifacts.unidata.ucar.edu/repository/unidata-all'). \
    getOrCreate()

sedona = SedonaContext.create(config)


def test_run_sedona():
    sql = """
    SELECT ST_DistanceSphere(
        ST_Transform(ST_Point(-74, 40.8), 'EPSG:3857', 'EPSG:3857'),
        ST_Transform(ST_Point(-46.6, -23.5), 'EPSG:3857', 'EPSG:3857')) as result
    """
    sedona.sql(sql).show()
    