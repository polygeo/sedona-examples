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
from sedona.spark import GridType
from sedona.utils.structured_adapter import StructuredAdapter
from sedona.sql.st_functions import ST_GeoHash
import glob


def test_geoparquet_optimizations():
    df = sedona.createDataFrame([
        ("a", 'POINT(2.0 1.0)'),
        ("b", 'POINT(2.0 3.0)'),
        ("c", 'POINT(2.5 2.0)'),
        ("d", 'POINT(3.0 1.0)'),
        ("e", 'POINT(5.0 4.0)'),
        ("f", 'POINT(6.0 4.0)'),
        ("g", 'POINT(6.0 5.0)'),
        ("h", 'POINT(7.0 1.0)'),
        ("i", 'POINT(7.0 2.0)'),
        ("j", 'POINT(8.0 1.0)'),
    ], ["id", "geometry"])
    df = df.withColumn("geometry", ST_GeomFromText(col("geometry")))

    # Create the partitioning. KDBTREE provides a nice balance providing
    # tight (but well-separated) partitions with approximately equal numbers of
    # features in each file. Note that num_partitions is only a suggestion
    # (actual value may differ)
    ###
    rdd = StructuredAdapter.toSpatialRdd(df, "geometry")
    print("rdd analyze")
    print(rdd.analyze())

    # We call the WithoutDuplicates() variant to ensure that we don't introduce
    # duplicate features (i.e., each feature is assigned a single partition instead of
    # each feature being assigned to every partition it intersects). For points the
    # behaviour of spatialPartitioning() and spatialPartitioningWithoutDuplicates()
    # is identical.
    ###
    rdd.spatialPartitioningWithoutDuplicates(GridType.KDBTREE, num_partitions=2)

    # Get the grids for this partitioning (you can reuse this partitioning
    # by passing it to some other spatialPartitioningWithoutDuplicates() to
    # ensure a different write has identical partition extents)
    ###
    rdd.getPartitioner().getGrids()
    df_partitioned = StructuredAdapter.toSpatialPartitionedDf(rdd, sedona)

    # Let's see the geohash column
    df_partitioned.withColumn("geohash", ST_GeoHash(df_partitioned.geometry, 12)).show()

    # Optional: sort within partitions for tighter rowgroup bounding boxes within files
    ###
    df_partitioned = (
        df_partitioned.withColumn("geohash", ST_GeoHash(df_partitioned.geometry, 12))
        .sortWithinPartitions("geohash")
        .drop("geohash")
    )

    print("***")
    print("df_partitioned")
    df_partitioned.show()

    df_partitioned.write.format("geoparquet").mode("overwrite").save(
        "/tmp/points_partitioned"
    )

    files = glob.glob("/tmp/points_partitioned/*.parquet")
    print(files)
    print(len(files))
    for f in files:
        print(f)
        sedona.read.format("geoparquet").load(f).show()

    print("***")
    print("check metadata")
    df = sedona.read.format("geoparquet.metadata").load("/tmp/points_partitioned")
    df.select("columns").show(truncate=False)
