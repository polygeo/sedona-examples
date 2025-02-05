from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from sedona.spark import *
from .sedona import sedona
from pyspark.sql.functions import col, lit
import chispa
import pytest


def test_write_geojson():
    df = sedona.createDataFrame([
        ("a", 'LINESTRING(2.0 5.0,6.0 1.0)'),
        ("b", 'LINESTRING(7.0 4.0,9.0 2.0)'),
        ("c", 'LINESTRING(1.0 3.0,3.0 1.0)'),
    ], ["id", "geometry"])
    actual = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    print("***")
    actual.printSchema()
    actual.show()
    actual.write.format("geojson").mode("overwrite").save("/tmp/a_thing")