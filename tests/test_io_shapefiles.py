from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from sedona.spark import *
from .sedona import sedona
from pyspark.sql.functions import col, lit, expr
import chispa
import pytest
import geopandas as gpd
from shapely.geometry import Point
import os


def test_io_shapefile():
    # Create sample geometries
    point1 = Point(0, 0)
    point2 = Point(1, 1)

    # Create a GeoDataFrame
    data = {
        "name": ["Point A", "Point B"],
        "value": [10, 20],
        "geometry": [point1, point2],
    }

    gdf = gpd.GeoDataFrame(data, geometry="geometry")

    # Write to a shapefile
    gdf.to_file("/tmp/my_geodata.shp")

    df = sedona.read.format("shapefile").load("/tmp/my_geodata.shp")
    # print("***")
    # df.show()

    df = (
        sedona.read.format("shapefile")
        .option("key.name", "FID")
        .load("/tmp/my_geodata.shp")
    )
    print("***")
    df.show()


def test_io_shapefiles():
    os.makedirs("/tmp/shapefiles")
    # write one DataFrame
    point1 = Point(0, 0)
    point2 = Point(1, 1)
    data = {
        "name": ["Point A", "Point B"],
        "value": [10, 20],
        "geometry": [point1, point2],
    }
    gdf = gpd.GeoDataFrame(data, geometry="geometry")
    gdf.to_file("/tmp/shapefiles/file1.shp")

    # write another DataFrame
    point3 = Point(2, 2)
    point4 = Point(3, 3)
    data = {
        "name": ["Point C", "Point D"],
        "value": [10, 20],
        "geometry": [point3, point4],
    }
    gdf = gpd.GeoDataFrame(data, geometry="geometry")
    gdf.to_file("/tmp/shapefiles/file2.shp")    

    df = sedona.read.format("shapefile").load("/tmp/shapefiles")
    # print("***")
    # df.show()
