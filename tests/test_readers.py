from sedona.spark import *
from .sedona import sedona
from pyspark.sql.functions import expr
import chispa


def test_read_geojson():
    df = (
        sedona.read.format("geojson")
        .option("multiLine", "true")
        .load("data/some_geojson.json")
    )
    parsedDf = (
        df.selectExpr("explode(features) as features")
        .select("features.*")
        .withColumn("prop0", expr("properties['prop0']"))
        .drop("properties")
        .drop("type")
    )
    # parsedDf.show()


def test_read_csv():
    df = (
        sedona.read.format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load("data/county_small.tsv")
    )
    df.createOrReplaceTempView("some_counties")

    # sedona.sql(
    #     "SELECT ST_GeomFromWKT(_c0) AS countyshape, _c1, _c2 from some_counties"
    # ).show()
