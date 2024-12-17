from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from sedona.spark import *

config = SedonaContext.builder(). \
    config('spark.jars.packages',
           'org.apache.sedona:sedona-spark-3.5_2.12:1.6.1,'
           'org.datasyslab:geotools-wrapper:1.7.0-28.5'). \
    config('spark.jars.repositories', 'https://artifacts.unidata.ucar.edu/repository/unidata-all'). \
    getOrCreate()

sedona = SedonaContext.create(config)


def test_run_distance_sphere():
    sql = """
    SELECT ST_DistanceSphere(
        ST_Transform(ST_Point(-74, 40.8), 'EPSG:3857', 'EPSG:3857'),
        ST_Transform(ST_Point(-46.6, -23.5), 'EPSG:3857', 'EPSG:3857')) as result
    """
    sedona.sql(sql).show()


def test_as_text():
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


def test_st_distance():
    some_trips = sedona.createDataFrame([
        ("New York", -74.0, 40.8, "Sao Paulo", -46.6, -23.5),
        ("Sao Paulo", -46.6, -23.5, "Rio", -43.2, -22.9)
    ], ["start_city", "start_lon", "start_lat", "end_city", "end_lon", "end_lat"])
    print("***")
    print(some_trips.show())


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
    print("***")
    sedona.sql(sql).show()


def test_geojson():
    df = sedona.read.format("geojson").option("multiLine", "true").load("tests/data/some_geojson.json")
    parsedDf = df.selectExpr("explode(features) as features").select("features.*").withColumn("prop0", expr("properties['prop0']")).drop("properties").drop("type")
    parsedDf.show()


def test_read_csv():
    df = sedona.read.format("csv").option("delimiter", "\t").option("header", "false").load("tests/data/county_small.tsv")
    df.createOrReplaceTempView("some_counties")

    sedona.sql("SELECT ST_GeomFromWKT(_c0) AS countyshape, _c1, _c2 from some_counties").show()


def test_area_spheroid():
    df = sedona.read.format("csv").option("delimiter", "\t").option("header", "false").load("tests/data/county_small.tsv")
    df.createOrReplaceTempView("some_counties")

    sql = "SELECT ST_GeomFromWKT(_c0) AS county_shape, _c5, ST_AreaSpheroid(county_shape) as county_area from some_counties"
    sedona.sql(sql).show()


def test_centroid():
    df = sedona.read.format("csv").option("delimiter", "\t").option("header", "false").load("tests/data/county_small.tsv")
    df.createOrReplaceTempView("some_counties")

    sql = "SELECT ST_GeomFromWKT(_c0) AS county_shape, _c5, ST_Centroid(county_shape) as county_centriod from some_counties"
    sedona.sql(sql).show()
