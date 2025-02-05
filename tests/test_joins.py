from pyspark.sql import SparkSession
from sedona.spark import *
from .sedona import sedona
from sedona.sql.types import GeometryType
from sedona.sql.st_predicates import ST_DWithin
from sedona.sql.st_constructors import ST_Point, ST_GeomFromText
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
)
from pathlib import Path
from sedona.stats.clustering.dbscan import dbscan
import chispa


def test_join_touches():
    df = sedona.createDataFrame([
        ("a", 'LINESTRING(2.0 4.0,4.0 0.0)'),
        ("b", 'LINESTRING(6.0 0.0,10.0 4.0)'),
    ], ["id", "geometry"])
    lines = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # lines.show(truncate=False)
    lines.createOrReplaceTempView("lines")

    df = sedona.createDataFrame([
        ("x", 'POLYGON((6.0 2.0,6.0 4.0, 8.0 4.0, 8.0 2.0, 6.0 2.0))'),
    ], ["id", "geometry"])
    polygons = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # polygons.show(truncate=False)
    polygons.createOrReplaceTempView("polygons")

    res = sedona.sql("""
    SELECT
        lines.id as line_id,
        polygons.id as polygon_id
    FROM lines
    LEFT OUTER JOIN polygons ON ST_Touches(lines.geometry, polygons.geometry);        
    """)
    # res.show()

    schema = StructType([
        StructField("line_id", StringType(), True),
        StructField("polygon_id", StringType(), True)
    ])
    expected = sedona.createDataFrame([
        ("a", None),
        ("b", "x")
    ], schema)
    # expected.show()

    chispa.assert_df_equality(res, expected)


def test_st_within():
    df = sedona.createDataFrame([
        ("a", 'POINT(1.0 1.0)'),
        ("b", 'POINT(2.0 4.0)'),
        ("c", 'POINT(8.0 2.0)'),
    ], ["id", "geometry"])
    points = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # lines.show(truncate=False)
    points.createOrReplaceTempView("points")

    df = sedona.createDataFrame([
        ("x", 'POLYGON((7.0 1.0,7.0 3.0, 9.0 3.0, 9.0 1.0, 7.0 1.0))'),
        ("y", 'POLYGON((1.0 3.0,1.0 5.0, 3.0 5.0, 3.0 3.0, 1.0 3.0))'),
    ], ["id", "geometry"])
    polygons = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # polygons.show(truncate=False)
    polygons.createOrReplaceTempView("polygons")

    res = sedona.sql("""
    SELECT
        points.id as point_id,
        polygons.id as polygon_id
    FROM points
    LEFT OUTER JOIN polygons ON ST_Within(points.geometry, polygons.geometry);        
    """)
    # res.show()

    schema = StructType([
        StructField("point_id", StringType(), True),
        StructField("polygon_id", StringType(), True)
    ])
    expected = sedona.createDataFrame([
        ("a", None),
        ("b", "y"),
        ("c", "x")
    ], schema)
    # expected.show()

    chispa.assert_df_equality(res, expected)


def test_st_crosses():
    df = sedona.createDataFrame([
        ("a", 'LINESTRING(2.0 5.0,6.0 1.0)'),
        ("b", 'LINESTRING(7.0 4.0,9.0 2.0)'),
        ("c", 'LINESTRING(1.0 3.0,3.0 1.0)'),
    ], ["id", "geometry"])
    lines = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # lines.show(truncate=False)
    lines.createOrReplaceTempView("lines")

    df = sedona.createDataFrame([
        ("x", 'POLYGON((3.0 2.0,3.0 5.0, 8.0 5.0, 8.0 2.0, 3.0 2.0))'),
    ], ["id", "geometry"])
    polygons = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # polygons.show(truncate=False)
    polygons.createOrReplaceTempView("polygons")

    res = sedona.sql("""
    SELECT
        lines.id as line_id,
        polygons.id as polygon_id
    FROM lines
    LEFT OUTER JOIN polygons ON ST_Crosses(lines.geometry, polygons.geometry);        
    """)
    # res.show()

    schema = StructType([
        StructField("line_id", StringType(), True),
        StructField("polygon_id", StringType(), True)
    ])
    expected = sedona.createDataFrame([
        ("a", "x"),
        ("b", "x"),
        ("c", None)
    ], schema)
    # expected.show()

    chispa.assert_df_equality(res, expected)


def test_st_overlaps():
    df = sedona.createDataFrame([
        ("a", 'POLYGON((2.0 1.0,2.0 4.0, 4.0 4.0, 4.0 1.0, 2.0 1.0))'),
        ("b", 'LINESTRING(7.0 2.0,9.0 2.0)'),
        ("c", 'LINESTRING(6.0 1.0,9.0 1.0)'),
        ("d", 'POINT(8.5 2.5)'),
    ], ["id", "geometry"])
    shapes = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # lines.show(truncate=False)
    shapes.createOrReplaceTempView("shapes")

    df = sedona.createDataFrame([
        ("x", 'POLYGON((1.0 2.0,1.0 5.0, 5.0 5.0, 5.0 2.0, 1.0 2.0))'),
        ("y", 'POLYGON((8.0 1.5,8.0 3.0, 9.0 3.0, 9.0 1.5, 8.0 1.5))'),
    ], ["id", "geometry"])
    polygons = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    # polygons.show(truncate=False)
    polygons.createOrReplaceTempView("polygons")

    res = sedona.sql("""
    SELECT
        shapes.id as shape_id,
        polygons.id as polygon_id
    FROM shapes
    LEFT OUTER JOIN polygons ON ST_Overlaps(shapes.geometry, polygons.geometry);        
    """)
    # res.show()

    schema = StructType([
        StructField("shape_id", StringType(), True),
        StructField("polygon_id", StringType(), True)
    ])
    expected = sedona.createDataFrame([
        ("a", "x"),
        ("b", None),
        ("c", None),
        ("d", None),
    ], schema)
    # expected.show()

    chispa.assert_df_equality(res, expected)


def test_knn_join():
    # Create Addresses DataFrame
    addresses = sedona.createDataFrame([
        ("a1", 2.0, 3.0),
        ("a2", 5.0, 5.0),
        ("a3", 7.0, 2.0),
    ], ["id", "longitude", "latitude"])

    addresses = addresses.withColumn("geometry", ST_Point(col("longitude"), col("latitude")))
    addresses.createOrReplaceTempView("addresses")

    # Create Coffee Shops DataFrame
    coffee_shops = sedona.createDataFrame([
        ("c1", 1.0, 4.0),
        ("c2", 3.0, 5.0),
        ("c3", 5.0, 1.0),
        ("c4", 8.0, 4.0),
    ], ["id", "longitude", "latitude"])

    coffee_shops = coffee_shops.withColumn("geometry", ST_Point(col("longitude"), col("latitude")))
    coffee_shops.createOrReplaceTempView("coffee_shops")

    # Perform KNN Join to find 2 nearest coffee shops to each address
    res = sedona.sql("""
    SELECT 
        addresses.id AS address_id, 
        coffee_shops.id AS coffee_shop_id
    FROM addresses
    JOIN coffee_shops 
    ON ST_KNN(addresses.geometry, coffee_shops.geometry, 2)
    """)

    schema = StructType([
        StructField("address_id", StringType(), True),
        StructField("coffee_shop_id", StringType(), True)
    ])

    expected = sedona.createDataFrame([
        ("a1", "c1"),
        ("a1", "c2"),
        ("a2", "c2"),
        ("a2", "c4"),
        ("a3", "c3"),
        ("a3", "c4"),
    ], schema)

    # res.show()

    chispa.assert_df_equality(res, expected)


def test_distance_join():
    # Create Points DataFrame
    df = sedona.createDataFrame([
        ("p1", "POINT (4.5 3)"),
    ], ["id", "geometry"])

    points = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
    points.createOrReplaceTempView("points")

    # Create Transit Stations DataFrame
    df = sedona.createDataFrame([
        ("t1", 1.0, 4.0),
        ("t2", 3.0, 4.0),
        ("t3", 5.0, 2.0),
        ("t4", 8.0, 4.0),
    ], ["id", "longitude", "latitude"])

    transit = df.withColumn("geometry", ST_Point(col("longitude"), col("latitude")))
    transit.createOrReplaceTempView("transit")

    # Perform Distance Join (within a distance of 2.5 units)
    res = sedona.sql("""
    SELECT 
        points.id AS point_id, 
        transit.id AS transit_id
    FROM points
    JOIN transit 
    ON ST_DWithin(points.geometry, transit.geometry, 2.5)
    """)

    schema = StructType([
        StructField("point_id", StringType(), True),
        StructField("transit_id", StringType(), True)
    ])

    expected = sedona.createDataFrame([
        ("p1", "t2"),
        ("p1", "t3"),
    ], schema)

    # res.show()

    chispa.assert_df_equality(res, expected)


def test_range_join():
    # Create City Polygon DataFrame
    cities = sedona.createDataFrame([
        ("city1", "POLYGON((1.0 1.0, 1.0 5.0, 5.0 5.0, 5.0 1.0, 1.0 1.0))"),
    ], ["id", "geometry"])

    cities = cities.withColumn("geometry", ST_GeomFromText(col("geometry")))
    cities.createOrReplaceTempView("cities")

    # Create Restaurants DataFrame
    restaurants = sedona.createDataFrame([
        ("r1", 2.0, 2.0),
        ("r2", 3.0, 3.0),
        ("r3", 4.0, 4.0),
        ("r4", 6.0, 6.0),
    ], ["id", "longitude", "latitude"])

    restaurants = restaurants.withColumn("geometry", ST_Point(col("longitude"), col("latitude")))
    restaurants.createOrReplaceTempView("restaurants")

    # Perform Range Join using ST_Intersects
    res = sedona.sql("""
    SELECT 
        cities.id AS city_id, 
        restaurants.id AS restaurant_id
    FROM cities
    JOIN restaurants 
    ON ST_Intersects(restaurants.geometry, cities.geometry)
    """)

    schema = StructType([
        StructField("city_id", StringType(), True),
        StructField("restaurant_id", StringType(), True)
    ])

    expected = sedona.createDataFrame([
        ("city1", "r1"),
        ("city1", "r2"),
        ("city1", "r3"),
    ], schema)

    res.show()

    chispa.assert_df_equality(res, expected)