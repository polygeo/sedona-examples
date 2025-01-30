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
