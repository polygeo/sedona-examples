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
    willis_tower = Point(-87.635918, 41.878876)

    df = sedona.createDataFrame([
        (empire_state_building, freedom_tower),
        (empire_state_building, willis_tower),
    ], ["start", "end"])

    res = df.withColumn(
        "st_distance_sphere", 
        ST_DistanceSphere(col("start"), col("end"))
    ).withColumn(
        "start_cellid", 
        ST_H3CellIDs(col("start"), 6, True)
    ).withColumn(
        "end_cellid", 
        ST_H3CellIDs(col("end"), 6, True)
    ).withColumn(
        "h3_cell_distance", 
        ST_H3CellDistance(col("start_cellid")[0], col("end_cellid")[0])
    )

    print("***")
    res.select("st_distance_sphere", "h3_cell_distance").show()