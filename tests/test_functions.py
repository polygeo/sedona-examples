from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from sedona.spark import *
from .sedona import sedona
import chispa
import pytest
from shapely import Polygon, LineString
from pyspark.sql.functions import col


def test_geometrytype():
    sql = """
    SELECT GeometryType(
        ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)')
    ) as result;
    """
    actual = sedona.sql(sql)
    expected = sedona.createDataFrame([("LINESTRING",)], ["result"])
    chispa.assert_df_equality(actual, expected)


# def test_st_3ddistance():
#     sql = """
#     SELECT ST_3DDistance(
#         ST_GeomFromText("POINT Z(0 0 -5)"),
#         ST_GeomFromText("POINT Z(1 1 -6")
#     ) as result
#     """
#     actual = sedona.sql(sql)
#     expected = sedona.createDataFrame([(1.7,)], ["result"])
#     chispa.assert_approx_df_equality(actual, expected, 0.1)


@pytest.mark.skip(reason="todo")
def test_st_addmeasure():
    2 + 2


@pytest.mark.skip(reason="todo")
def test_st_addpoint():
    2 + 2


@pytest.mark.skip(reason="todo")
def test_st_affine():
    2 + 2


def test_st_angle():
    sql = """
    SELECT ST_Angle(
        ST_GeomFromWKT('POINT(0 0)'),
        ST_GeomFromWKT('POINT (1 1)'),
        ST_GeomFromWKT('POINT(1 0)'),
        ST_GeomFromWKT('POINT(6 2)')
    ) as result
    """
    actual = sedona.sql(sql)
    expected = sedona.createDataFrame([(0.405,)], ["result"])
    chispa.assert_approx_df_equality(actual, expected, 0.001)


def test_st_areaspheroid():
    sql = """
    SELECT ST_AreaSpheroid(ST_GeomFromWKT('Polygon ((34 35, 28 30, 25 34, 34 35))')) as result
    """
    actual = sedona.sql(sql)
    expected = sedona.createDataFrame([(201824850811.76245,)], ["result"])
    chispa.assert_approx_df_equality(actual, expected, 0.1)


def test_st_distance():
    some_trips = sedona.createDataFrame(
        [
            ("New York", -74.0, 40.8, "Sao Paulo", -46.6, -23.5),
            ("Sao Paulo", -46.6, -23.5, "Rio", -43.2, -22.9),
        ],
        ["start_city", "start_lon", "start_lat", "end_city", "end_lon", "end_lat"],
    )
    some_trips.createOrReplaceTempView("some_trips")
    sql = """
    SELECT 
        start_city,
        end_city,
        ST_DistanceSphere(
            ST_Point(start_lon, start_lat),
            ST_Point(end_lon, end_lat)) AS distance
    FROM some_trips
    """
    actual = sedona.sql(sql)
    expected = sedona.createDataFrame(
        [("New York", "Sao Paulo", 7690115.1), ("Sao Paulo", "Rio", 353827.8)],
        ["start_city", "end_city", "distance"],
    )
    chispa.assert_approx_df_equality(actual, expected, 0.1)


def test_st_distance_sphere():
    sql = """
    SELECT ST_DistanceSphere(
        ST_Transform(ST_Point(-74, 40.8), 'EPSG:3857', 'EPSG:3857'),
        ST_Transform(ST_Point(-46.6, -23.5), 'EPSG:3857', 'EPSG:3857')) as result
    """
    actual = sedona.sql(sql)
    # actual.show()
    expected = sedona.createDataFrame([(7690115.1,)], ["result"])
    chispa.assert_approx_df_equality(actual, expected, 0.1)


def test_st_transform():
    sql = """
    SELECT ST_AsText(
        ST_Transform(
            ST_GeomFromText('POLYGON((170 50,170 72,-130 72,-130 50,170 50))'),
            'EPSG:4326',
            'EPSG:32649'
        )
    ) as result
    """
    # sedona.sql(sql).show()
    # TODO: figure out assertion


def test_linesegments():
    df = sedona.createDataFrame([
        ("a", LineString([(1, 1), (1, 3), (2, 4)])),
        ("b", LineString([(4, 2), (6, 2), (9, 1)])),
    ], ["id", "geometry"])

    print("***")

    df.selectExpr(
        "id",
        "ST_LineSegments(geometry)"
    ).show(truncate=False)

    # df.withColumn(
    #     "distance", 
    #     ST_LineSegements(col("geometry"))
    # ).show()


def test_uuid_type():
    df = sedona.createDataFrame([
        ("a", 1),
        ("b", 2),
    ], ["letter", "number"])
    res = df.selectExpr("letter", "uuid()")
    res.show(truncate=False)
    res.printSchema()
