from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from sedona.spark import *
from .sedona import sedona
import chispa
import pytest
from pyspark.sql.functions import col

from shapely import Point, Polygon, LineString


def test_raster_quickstart():
    print("*** One raster file ***")
    rawDf = sedona.read.format("binaryFile").load("data/raster/test1.tiff")
    rawDf.createOrReplaceTempView("rawdf")
    rawDf.show()

    print("*** Many raster files ***")
    rawDf = (
        sedona.read.format("binaryFile")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.tif*")
        .load("data/raster")
    )
    rawDf.createOrReplaceTempView("rawdf")
    rawDf.show()

    print("*** Add raster column ***")
    sql = "SELECT RS_FromGeoTiff(content) AS rast, modificationTime, length, path FROM rawdf"
    rasterDf = sedona.sql(sql)
    rasterDf.createOrReplaceTempView("rasterDf")
    rasterDf.show()
    rasterDf.printSchema()

    print("*** Get raster metadata ***")
    sql = "SELECT RS_MetaData(rast) FROM rasterDf"
    res = sedona.sql(sql)
    res.show()
    res.printSchema()
