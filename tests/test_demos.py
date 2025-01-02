from pyspark.sql import SparkSession
from sedona.spark import *
from .sedona import sedona


def test_st_areaspheroid():
    df = (
        sedona.read.format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load("data/county_small.tsv")
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
        .load("data/county_small.tsv")
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
