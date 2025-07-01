from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from sedona.spark import *
from .sedona import sedona
import chispa
import pytest
from pyspark.sql.functions import col

from shapely import Point, Polygon, LineString


def test_distance():

    df = sedona.createDataFrame([
        (Point(2, 3), Point(6, 4)),
        (Point(6, 2), Point(9, 2)),
    ], ["start", "end"])

    # print("***")

    res = df.withColumn(
        "distance", 
        ST_Distance(col("start"), col("end"))
    )
    # res.show()


def test_st_distance_functions():
    seattle = Point(-122.335167, 47.608013)
    new_york = Point(-73.935242, 40.730610)
    sydney = Point(151.2, -33.9)
    df = sedona.createDataFrame(
        [
            (seattle, "seattle", new_york, "new_york"),
            (seattle, "seattle", sydney, "sydney"),
        ],
        ["place1", "place1_name", "place2", "place2_name"],
    )
    # print("***ST_DistanceSphere***")
    # df.show(truncate=False)
    res = df.withColumn(
        "st_distance_sphere", 
        ST_DistanceSphere(col("place1"), col("place2"))
    )
    # res.show()
    print("***ST_DistanceSpheroid***")
    res = df.withColumn(
        "st_distance_spheroid", 
        ST_DistanceSpheroid(col("place1"), col("place2"))
    )
    # res.select("place1_name", "place2_name", "st_distance_spheroid").show()


def test_more_functions():
    wkt_1 = "POLYGON ((-88.110352 24.006326, -77.080078 24.006326, -77.080078 31.503629, -88.110352 31.503629, -88.110352 24.006326))"
    wkt_2 = "POLYGON ((-89.516602 25.204941, -78.266602 25.204941, -78.266602 32.398516, -89.516602 32.398516, -89.516602 25.204941))" 
    wkt_3 = "POLYGON ((-98.920898 35.442771, -92.922363 35.442771, -92.922363 38.427774, -98.920898 38.427774, -98.920898 35.442771))"


    res = sedona.sql(f"""
    SELECT
    ST_FrechetDistance(
                ST_GEOMFROMWKT('{wkt_1}'),
                ST_GEOMFROMWKT('{wkt_1}')) as same_geom_frechet,
    ST_FrechetDistance(
                ST_GEOMFROMWKT('{wkt_1}'),
                ST_GEOMFROMWKT('{wkt_2}')) as similar_geom_frechet,
    ST_FrechetDistance(
                ST_GEOMFROMWKT('{wkt_1}'),
                ST_GEOMFROMWKT('{wkt_3}')) as vvdifferent_geom_frechet,
    ST_HausdorffDistance(
                ST_GEOMFROMWKT('{wkt_1}'),
                ST_GEOMFROMWKT('{wkt_1}')) as same_geom_hausdorff,
    ST_HausdorffDistance(
                ST_GEOMFROMWKT('{wkt_1}'),
                ST_GEOMFROMWKT('{wkt_2}')) as similar_geom_hausdorff,
    ST_HausdorffDistance(
                ST_GEOMFROMWKT('{wkt_1}'),
                ST_GEOMFROMWKT('{wkt_3}')) as vvdifferent_geom_hausdorff

    """)
    # res.show()


def test_frechet_distance_functions():
    a = LineString([(1, 1), (1, 3), (2, 4)])
    b = LineString([(1.1, 1), (1.1, 3), (3, 4)])
    c = LineString([(7, 1), (7, 3), (6, 4)])
    df = sedona.createDataFrame([
        (a, "a", b, "b"),
        (a, "a", c, "c"),
    ], ["geometry1", "geometry1_id", "geometry2", "geometry2_id"])
    # print("***")

    res = df.withColumn(
        "frechet_distance", 
        ST_FrechetDistance(col("geometry1"), col("geometry2"))
    )
    
    # res.select("geometry1_id", "geometry2_id", "frechet_distance").show()


def test_max_distance():
    a = LineString([(1, 1), (1, 3), (2, 4)])
    b = LineString([(4, 2), (6, 2), (9, 1)])
    c = Point(8, 4)
    df = sedona.createDataFrame([
        ("a", a, "b", b),
        ("a", a, "c", c),
    ], ["id1", "geom1", "id2", "geom2"])

    res = df.withColumn(
        "max_distance",
        ST_MaxDistance(col("geom1"), col("geom2"))
    )

    # print("*** max_distance ***")
    # res.select("id1", "id2", "max_distance").show(truncate=False)


def test_distance_between_polygons():
    a = Polygon([(1, 1), (1, 3), (2, 3), (2, 1), (1, 1)])
    b = Polygon([(4, 1), (4, 2), (6, 4), (6, 1), (4, 1)])
    df = sedona.createDataFrame([
        ("a", a, "b", b),   
    ], ["id1", "geom1", "id2", "geom2"])    

    res = df.withColumn(
        "distance",
        ST_Distance(col("geom1"), col("geom2"))
    )

    print("*** distance ***")
    res.select("id1", "id2", "distance").show(truncate=False)


def test_3d_distance():
    empire_state_ground = Point(-73.9857, 40.7484, 0)
    empire_state_top = Point(-73.9857, 40.7484, 380)
    df = sedona.createDataFrame([
        (empire_state_ground, empire_state_top),
    ], ["point_a", "point_b"])
    res = df.withColumn(
        "distance",
        ST_Distance(col("point_a"), col("point_b"))
    ).withColumn(
        "3d_distance",
        ST_3DDistance(col("point_a"), col("point_b"))
    )
    print("*** 3d distance ***")
    res.show()