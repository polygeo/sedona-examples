from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from sedona.spark import *
from .sedona import sedona
from pyspark.sql.functions import col, lit, expr
import chispa
import pytest
import geopandas as gpd
from shapely.geometry import Point, Polygon
import os


def test_io_geopackage():
    # Create sample geometries
    point1 = Point(0, 0)
    point2 = Point(1, 1)
    polygon1 = Polygon([(5, 5), (6, 6), (7, 5), (6, 4)])

    data = {
        "name": ["Point A", "Point B", "Polygon A"],
        "value": [10, 20, 30],
        "geometry": [point1, point2, polygon1],
    }
    gdf = gpd.GeoDataFrame(data, geometry="geometry")

    # Write to a GeoPackage
    gdf.to_file("/tmp/my_file.gpkg", layer="my_layer", driver="GPKG")

    # Read GeoPackage
    df = (
        sedona.read.format("geopackage")
        .option("tableName", "my_layer")
        .load("/tmp/my_file.gpkg")
    )
    # print("***")
    # print("***")
    # df.show()

    # See metadata of GeoPackage
    df = (
        sedona.read.format("geopackage")
        .option("showMetadata", "true")
        .load("/tmp/my_file.gpkg")
    )
    # print("***")
    # print("***")
    # df.show()


def test_io_geopackages():
    os.makedirs("/tmp/gpkgs", exist_ok=True)
    # Create one file
    point1 = Point(0, 0)
    point2 = Point(1, 1)
    data = {
        "name": ["Point A", "Point B"],
        "value": [10, 20],
        "geometry": [point1, point2],
    }
    gdf = gpd.GeoDataFrame(data, geometry="geometry")
    gdf.to_file("/tmp/gpkgs/my_file1.gpkg", layer="my_layer", driver="GPKG")

    # Create another file
    point3 = Point(5, 5)
    polygon1 = Polygon([(5, 5), (6, 6), (7, 5), (6, 4)])
    data = {
        "name": ["Point C", "Polygon A"],
        "value": [30, 40],
        "geometry": [point3, polygon1],
    }
    gdf = gpd.GeoDataFrame(data, geometry="geometry")
    gdf.to_file("/tmp/gpkgs/my_file2.gpkg", layer="my_layer", driver="GPKG")

    # Read GeoPackages
    df = (
        sedona.read.format("geopackage")
        .option("tableName", "my_layer")
        .load("/tmp/gpkgs")
    )
    # print("***")
    # print("***")
    # df.show()
