from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from sedona.spark import *
from .sedona import sedona
from pyspark.sql.functions import col, lit, expr
import chispa
import pytest


def test_csv_wkt():
    df = sedona.createDataFrame(
        [
            ("a", "LINESTRING(2.0 5.0,6.0 1.0)"),
            ("b", "POINT(1.0 2.0)"),
            ("c", "POLYGON((7.0 1.0,7.0 3.0,9.0 3.0,7.0 1.0))"),
        ],
        ["id", "geometry"],
    )
    df = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # print("***")
    # df.show(truncate=False)

    # create CSV with WKT
    df = df.withColumn("geom_wkt", ST_AsText(col("geometry"))).drop("geometry")
    df.repartition(1).write.option("header", True).format("csv").mode("overwrite").save(
        "/tmp/my_csvs"
    )

    # read CSV with WKT
    df = (
        sedona.read.option("header", True)
        .format("csv")
        .load("/tmp/my_csvs")
        .withColumn("geometry", ST_GeomFromText(col("geom_wkt")))
        .drop("geom_wkt")
    )
    # print("***")
    # df.printSchema()
    # df.show(truncate=False)


def test_csv_ewkt():
    df = sedona.createDataFrame(
        [
            ("a", "LINESTRING(2.0 5.0,6.0 1.0)"),
            ("b", "POINT(1.0 2.0)"),
            ("c", "POLYGON((7.0 1.0,7.0 3.0,9.0 3.0,7.0 1.0))"),
        ],
        ["id", "geometry"],
    )

    df = df.withColumn("geometry", ST_GeomFromWKT(col("geometry")))
    df = df.withColumn("geometry", ST_SetSRID(col("geometry"), 4326))

    # df = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # df = df.withColumn("geometry", ST_SetSRID(col("geometry"), 4326))
    # print("***")
    # df.show(truncate=False)

    # create CSV with WKT
    df = df.withColumn("geom_ewkt", ST_AsEWKT(col("geometry"))).drop("geometry")
    df.repartition(1).write.option("header", True).format("csv").mode("overwrite").save(
        "/tmp/my_ewkt_csvs"
    )

    # read CSV with WKT
    df = (
        sedona.read.option("header", True)
        .format("csv")
        .load("/tmp/my_ewkt_csvs")
        .withColumn("geometry", ST_GeomFromEWKT(col("geom_ewkt")))
        .drop("geom_ewkt")
    )
    # print("***EWKT example***")
    # df.printSchema()
    # df.show(truncate=False)
