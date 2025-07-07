from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from sedona.spark import *
from .sedona import sedona
import chispa
import pytest
from pyspark.sql.functions import col

from shapely import Point, Polygon, LineString


def test_H3CellDistance():


    empire_state_building = Point(-73.985428, 40.748817)
    freedom_tower = Point(-74.013379, 40.712743)

    df = sedona.createDataFrame([
        (empire_state_building, freedom_tower),
        (Point(2, 3), Point(6, 4)),
        (Point(6, 2), Point(9, 2)),
    ], ["start", "end"])

    res = df.withColumn(
        "st_distance", 
        ST_DistanceSphere(col("start"), col("end"))
    ).withColumn(
        "start_cellid", 
        ST_H3CellIDs(col("start"), 6, True)
    ).withColumn(
        "end_cellid", 
        ST_H3CellIDs(col("end"), 6, True)
    ).withColumn(
        "h3_distance", 
        ST_H3CellDistance(col("start_cellid")[0], col("end_cellid")[0])
    )

    print("***")
    res.select("st_distance", "h3_distance").show()