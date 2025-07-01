from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from sedona.spark import *
from .sedona import sedona
import chispa
import pytest
from pyspark.sql.functions import col


def test_centroids():
    df = sedona.createDataFrame([
        ('a', 'POLYGON((1.0 1.0,1.0 3.0,2.0 3.0,2.0 1.0,1.0 1.0))'),
        ('b', 'LINESTRING(4.0 1.0,4.0 2.0,6.0 4.0)'),
        ('c', 'POINT(9.0 2.0)'),
    ], ["id", "geometry"])
    df = df.withColumn("geometry", ST_GeomFromText(col("geometry")))

    print("***")

    df.withColumn(
        "centroid", 
        ST_Centroid(col("geometry"))
    ).show(truncate=False)

    print("***")

    df.withColumn(
        "centroid", 
        ST_Centroid(col("geometry"))
    ).select("id", "centroid").show(truncate=False)


def test_st_labelpoint():
    df = sedona.createDataFrame([
        ('a', 'POLYGON((1.0 1.0,1.0 3.0,2.0 3.0,2.0 1.0,1.0 1.0))'),
        ('b', 'POLYGON((4.0 1.0,4.0 2.0,6.0 4.0,6.0 1.0,4.0 1.0))'),
    ], ["id", "geometry"])
    df = df.withColumn("geometry", ST_GeomFromText(col("geometry")))

    print("***")

    df.withColumn(
        "labelpoint", 
        ST_LabelPoint(col("geometry"))
    ).select("id", "labelpoint").show(truncate=False)


def test_st_labelpoint_vs_centroid():
    df = sedona.createDataFrame([
        ('a', 'POLYGON((1.0 1.0,1.0 4.0,9.0 4.0,9.0 1.0,8.0 1.0, 8.0 3.9, 2.0 3.9, 2.0 1.0, 1.0 1.0))'),
    ], ["id", "geometry"])
    df = df.withColumn("geometry", ST_GeomFromText(col("geometry")))

    print("***")

    df.withColumn(
        "centroid", 
        ST_Centroid(col("geometry"))
    ).withColumn(
        "labelpoint", 
        ST_LabelPoint(col("geometry"))
    ).select("id", "centroid", "labelpoint").show(truncate=False)
