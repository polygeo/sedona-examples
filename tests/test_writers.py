from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from sedona.spark import *
from .sedona import sedona
from pyspark.sql.functions import col, lit, expr
import chispa
import pytest


def test_write_geojson():
    df = sedona.createDataFrame([
        ("a", 'LINESTRING(2.0 5.0,6.0 1.0)'),
        ("b", 'LINESTRING(7.0 4.0,9.0 2.0)'),
        ("c", 'LINESTRING(1.0 3.0,3.0 1.0)'),
    ], ["id", "geometry"])
    actual = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # print("***")
    # actual.printSchema()
    # actual.show()
    actual.write.format("geojson").mode("overwrite").save("/tmp/a_thing")


def test_read_geojson():
    df = sedona.createDataFrame([
        ("a", 'LINESTRING(2.0 5.0,6.0 1.0)'),
        ("b", 'LINESTRING(7.0 4.0,9.0 2.0)'),
        ("c", 'LINESTRING(1.0 3.0,3.0 1.0)'),
    ], ["id", "geometry"])
    actual = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    actual.write.format("geojson").mode("overwrite").save("/tmp/another_thing")
    df = sedona.read.format("geojson").load("/tmp/another_thing")
    # df.show(truncate=False)


def test_read_multiline_geojson():
    df = (
        sedona.read.format("geojson").option("multiLine", "true").load("data/multiline_geojson.json")
        .selectExpr("explode(features) as features")
        .select("features.*")
        .withColumn("prop0", expr("properties['prop0']")).drop("properties").drop("type")
    )
    # df.show(truncate=False)


def test_read_singleline_geojson():
    df = (
        sedona.read.format("geojson")
        .load("data/singleline_geojson.json")
        .withColumn("prop0", expr("properties['prop0']"))
        .drop("properties")
        .drop("type")    
    )
    # df.show(truncate=False)


def test_write_geoparquet():
    df = sedona.createDataFrame([
        ("a", 'LINESTRING(2.0 5.0,6.0 1.0)'),
        ("b", 'LINESTRING(7.0 4.0,9.0 2.0)'),
        ("c", 'LINESTRING(1.0 3.0,3.0 1.0)'),
    ], ["id", "geometry"])
    df = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # print("***")
    # actual.printSchema()
    # actual.show()
    df.write.format("geoparquet").mode("overwrite").save("/tmp/somewhere")

    ## read geoparquet
    df = sedona.read.format("geoparquet").load("/tmp/somewhere")
    # df.show(truncate=False)

    ## read geoparquet metadata
    df = sedona.read.format("geoparquet.metadata").load("/tmp/somewhere")
    # df.show(truncate=False)
    # df.printSchema()


def test_write_geoparquet_bbox():
    df = sedona.createDataFrame([
        ("a", 'POINT(2.0 1.0)'),
        ("b", 'POINT(2.0 3.0)'),
        ("c", 'POINT(2.5 2.0)'),
        ("d", 'POINT(3.0 1.0)'),
    ], ["id", "geometry"])
    df = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    df.repartition(1).write.format("geoparquet").mode("overwrite").save("/tmp/some_ex")

    df = sedona.createDataFrame([
        ("e", 'POINT(5.0 4.0)'),
        ("f", 'POINT(6.0 4.0)'),
        ("g", 'POINT(6.0 5.0)'),
    ], ["id", "geometry"])
    df = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    df.repartition(1).write.format("geoparquet").mode("append").save("/tmp/some_ex")

    df = sedona.createDataFrame([
        ("h", 'POINT(7.0 1.0)'),
        ("i", 'POINT(7.0 2.0)'),
        ("j", 'POINT(8.0 1.0)'),
    ], ["id", "geometry"])
    df = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    df.repartition(1).write.format("geoparquet").mode("append").save("/tmp/some_ex")

    df = sedona.read.format("geoparquet").load("/tmp/some_ex")
    df.show(truncate=False)
    df.createOrReplaceTempView("points")

    my_shape = 'POLYGON((4.0 3.5, 4.0 6.0, 8.0 6.0, 8.0 4.5, 4.0 3.5))'

    res = sedona.sql(f'''
    select *
    from points
    where st_intersects(geometry, ST_GeomFromWKT('{my_shape}'))
    ''')
    res.show(truncate=False)

    df = sedona.read.format("geoparquet.metadata").load("/tmp/somewhere")
    df.select("columns").show(truncate=False)
