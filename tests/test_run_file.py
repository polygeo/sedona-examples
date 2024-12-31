from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from sedona.spark import *
from .sedona import sedona
import chispa


def test_st_distance_sphere():
    sql = """
    SELECT ST_DistanceSphere(
        ST_Transform(ST_Point(-74, 40.8), 'EPSG:3857', 'EPSG:3857'),
        ST_Transform(ST_Point(-46.6, -23.5), 'EPSG:3857', 'EPSG:3857')) as result
    """
    actual = sedona.sql(sql)
    actual.show()
    expected = sedona.createDataFrame([(7690115.1, )], ["result"])
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
    sedona.sql(sql).show()
    # TODO: figure out assertion


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
    expected = sedona.createDataFrame([
        ("New York", "Sao Paulo", 7690115.1),
        ("Sao Paulo", "Rio", 353827.8)
    ], ["start_city", "end_city", "distance"])
    chispa.assert_approx_df_equality(actual, expected, 0.1)


def test_read_geojson():
    df = (
        sedona.read.format("geojson")
        .option("multiLine", "true")
        .load("tests/data/some_geojson.json")
    )
    parsedDf = (
        df.selectExpr("explode(features) as features")
        .select("features.*")
        .withColumn("prop0", expr("properties['prop0']"))
        .drop("properties")
        .drop("type")
    )
    parsedDf.show()


def test_read_csv():
    df = (
        sedona.read.format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load("tests/data/county_small.tsv")
    )
    df.createOrReplaceTempView("some_counties")

    sedona.sql(
        "SELECT ST_GeomFromWKT(_c0) AS countyshape, _c1, _c2 from some_counties"
    ).show()


def test_st_areaspheroid():
    df = (
        sedona.read.format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load("tests/data/county_small.tsv")
    )
    df.createOrReplaceTempView("some_counties")

    sql = """
    SELECT
        _c5 as county_name,
        ST_GeomFromWKT(_c0) as county_shape,
        ST_AreaSpheroid(county_shape) as county_area
    FROM some_counties
    """
    sedona.sql(sql).show()


def test_st_centroid():
    df = (
        sedona.read.format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load("tests/data/county_small.tsv")
    )
    df.createOrReplaceTempView("some_counties")

    sql = """
    SELECT
        _c5 as county_name,
        ST_GeomFromWKT(_c0) as county_shape,
        ST_Centroid(county_shape) as county_centriod 
    from some_counties
    """
    sedona.sql(sql).show()


def test_st_contains():
    sql = """
    SELECT ST_Contains(
        ST_GeomFromWKT('POLYGON((175 150,20 40,50 60,125 100,175 150))'),
        ST_GeomFromWKT('POINT(174 149)')
    ) as result
    """
    actual = sedona.sql(sql)
    expected = sedona.createDataFrame([(False, )], ["result"])
    chispa.assert_df_equality(actual, expected)
