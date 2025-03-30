from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql import SparkSession
from sedona.spark import *
from .sedona import sedona
from pyspark.sql.functions import col, lit, expr, concat_ws, concat
import chispa
import pytest
import geopandas as gpd
from shapely.geometry import Point, Polygon
import os


def test_create_linestrings():
    struct_schema = StructType([
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True)
    ])

    data = [
        (1, [{"x": 1.0, "y": 2.0}, {"x": 3.0, "y": 4.0}]),
        (2, [{"x": 5.0, "y": 6.0}, {"x": 3.0, "y": 4.0}]),
        (3, [{"x": 7.0, "y": 8.0}, {"x": 9.0, "y": 10.0}, {"x": 11.0, "y": 12.0}])
    ]

    df = sedona.createDataFrame(data, ["id", "points"])

    df = df.withColumn(
        "wkt",
        concat(
            lit("LINESTRING("),
            concat_ws(
                ",",
                expr("transform(points, point -> concat(point.x, ' ', point.y))")
            ),
            lit(")")
        )
    ).withColumn(
        "geom",
        ST_GeomFromWKT(col("wkt"))
    )

    # print("***")
    # df.show(truncate=False)
    # df.printSchema()
