from pyspark.sql import SparkSession
from sedona.spark import *
from .sedona import sedona
from sedona.sql.types import GeometryType
from sedona.sql.st_predicates import ST_DWithin
from sedona.sql.st_constructors import ST_Point, ST_GeomFromText
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
)
from pathlib import Path
from sedona.stats.clustering.dbscan import dbscan
import chispa


def test_join_touches():
    df = sedona.createDataFrame([
        ("a", 'LINESTRING(2.0 4.0,4.0 0.0)'),
        ("b", 'LINESTRING(6.0 0.0,10.0 4.0)'),
    ], ["id", "geometry"])
    lines = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # lines.show(truncate=False)
    lines.createOrReplaceTempView("lines")

    df = sedona.createDataFrame([
        ("x", 'POLYGON((6.0 2.0,6.0 4.0, 8.0 4.0, 8.0 2.0, 6.0 2.0))'),
    ], ["id", "geometry"])
    polygons = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # polygons.show(truncate=False)
    polygons.createOrReplaceTempView("polygons")

    res = sedona.sql("""
    SELECT
        lines.id as line_id,
        polygons.id as polygon_id
    FROM lines
    LEFT OUTER JOIN polygons ON ST_Touches(lines.geometry, polygons.geometry);        
    """)
    # res.show()

    schema = StructType([
        StructField("line_id", StringType(), True),
        StructField("polygon_id", StringType(), True)
    ])
    expected = sedona.createDataFrame([
        ("a", None),
        ("b", "x")
    ], schema)
    # expected.show()

    chispa.assert_df_equality(res, expected)


def test_st_within():
    df = sedona.createDataFrame([
        ("a", 'POINT(1.0 1.0)'),
        ("b", 'POINT(2.0 4.0)'),
        ("c", 'POINT(8.0 2.0)'),
    ], ["id", "geometry"])
    points = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # lines.show(truncate=False)
    points.createOrReplaceTempView("points")

    df = sedona.createDataFrame([
        ("x", 'POLYGON((7.0 1.0,7.0 3.0, 9.0 3.0, 9.0 1.0, 7.0 1.0))'),
        ("y", 'POLYGON((1.0 3.0,1.0 5.0, 3.0 5.0, 3.0 3.0, 1.0 3.0))'),
    ], ["id", "geometry"])
    polygons = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # polygons.show(truncate=False)
    polygons.createOrReplaceTempView("polygons")

    res = sedona.sql("""
    SELECT
        points.id as point_id,
        polygons.id as polygon_id
    FROM points
    LEFT OUTER JOIN polygons ON ST_Within(points.geometry, polygons.geometry);        
    """)
    # res.show()

    schema = StructType([
        StructField("point_id", StringType(), True),
        StructField("polygon_id", StringType(), True)
    ])
    expected = sedona.createDataFrame([
        ("a", None),
        ("b", "y"),
        ("c", "x")
    ], schema)
    # expected.show()

    chispa.assert_df_equality(res, expected)


def test_st_crosses():
    df = sedona.createDataFrame([
        ("a", 'LINESTRING(2.0 5.0,6.0 1.0)'),
        ("b", 'LINESTRING(7.0 4.0,9.0 2.0)'),
        ("c", 'LINESTRING(1.0 3.0,3.0 1.0)'),
    ], ["id", "geometry"])
    lines = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # lines.show(truncate=False)
    lines.createOrReplaceTempView("lines")

    df = sedona.createDataFrame([
        ("x", 'POLYGON((3.0 2.0,3.0 5.0, 8.0 5.0, 8.0 2.0, 3.0 2.0))'),
    ], ["id", "geometry"])
    polygons = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # polygons.show(truncate=False)
    polygons.createOrReplaceTempView("polygons")

    res = sedona.sql("""
    SELECT
        lines.id as line_id,
        polygons.id as polygon_id
    FROM lines
    LEFT OUTER JOIN polygons ON ST_Crosses(lines.geometry, polygons.geometry);        
    """)
    # res.show()

    schema = StructType([
        StructField("line_id", StringType(), True),
        StructField("polygon_id", StringType(), True)
    ])
    expected = sedona.createDataFrame([
        ("a", "x"),
        ("b", "x"),
        ("c", None)
    ], schema)
    # expected.show()

    chispa.assert_df_equality(res, expected)


def test_st_overlaps():
    df = sedona.createDataFrame([
        ("a", 'POLYGON((2.0 1.0,2.0 4.0, 4.0 4.0, 4.0 1.0, 2.0 1.0))'),
        ("b", 'LINESTRING(7.0 2.0,9.0 2.0)'),
        ("c", 'LINESTRING(6.0 1.0,9.0 1.0)'),
        ("d", 'POINT(8.5 2.5)'),
    ], ["id", "geometry"])
    shapes = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # lines.show(truncate=False)
    shapes.createOrReplaceTempView("shapes")

    df = sedona.createDataFrame([
        ("x", 'POLYGON((1.0 2.0,1.0 5.0, 5.0 5.0, 5.0 2.0, 1.0 2.0))'),
        ("y", 'POLYGON((8.0 1.5,8.0 3.0, 9.0 3.0, 9.0 1.5, 8.0 1.5))'),
    ], ["id", "geometry"])
    polygons = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # polygons.show(truncate=False)
    polygons.createOrReplaceTempView("polygons")

    res = sedona.sql("""
    SELECT
        shapes.id as shape_id,
        polygons.id as polygon_id
    FROM shapes
    LEFT OUTER JOIN polygons ON ST_Overlaps(shapes.geometry, polygons.geometry);        
    """)
    # res.show()

    schema = StructType([
        StructField("shape_id", StringType(), True),
        StructField("polygon_id", StringType(), True)
    ])
    expected = sedona.createDataFrame([
        ("a", "x"),
        ("b", None),
        ("c", None),
        ("d", None),
    ], schema)
    # expected.show()

    chispa.assert_df_equality(res, expected)
