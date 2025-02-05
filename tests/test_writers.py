from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from sedona.spark import *
from .sedona import sedona
from pyspark.sql.functions import col, lit
import chispa
import pytest


def test_geometrytype():
    df = sedona.createDataFrame([
        ("a", 'LINESTRING(2.0 5.0,6.0 1.0)'),
        ("b", 'LINESTRING(7.0 4.0,9.0 2.0)'),
        ("c", 'LINESTRING(1.0 3.0,3.0 1.0)'),
    ], ["id", "geom"])
    actual = df.withColumn("geom", ST_GeomFromText(col("geom")))
    print("***")
    actual.printSchema()
    actual.show()
    actual.write.format("geoparquet").mode("overwrite").save("/tmp/a_thing")