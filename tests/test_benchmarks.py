from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from sedona.spark import *
from .sedona import sedona
from pyspark.sql.functions import col, lit, expr
import chispa
import pytest
import time
import pandas as pd


def benchmark(f, dfs, benchmarks, name, **kwargs):
    """Benchmark the given function against the given DataFrame.

    Parameters
    ----------
    f: function to benchmark
    dfs: DataFrames used in query
    benchmarks: container for benchmark results
    name: task name

    Returns
    -------
    Duration (in seconds) of the given operation
    """
    start_time = time.time()
    ret = f(dfs, **kwargs)
    benchmarks["duration"].append(time.time() - start_time)
    benchmarks["task"].append(name)
    print(f"{name} took: {benchmarks['duration'][-1]} seconds")
    return benchmarks["duration"][-1]


def get_results(benchmarks):
    """Return a pandas DataFrame containing benchmark results."""
    return pd.DataFrame.from_dict(benchmarks)


def q1(df):
    my_shape = 'POLYGON((4.0 3.5, 4.0 6.0, 8.0 6.0, 8.0 4.5, 4.0 3.5))'
    return sedona.sql(f'''
    select *
    from a_table
    where st_intersects(geometry, ST_GeomFromWKT('{my_shape}'))
    ''').show()


def test_run_benchmarks():
    df = sedona.createDataFrame([
        ("a", 'LINESTRING(2.0 5.0,6.0 1.0)'),
        ("b", 'LINESTRING(7.0 4.0,9.0 2.0)'),
        ("c", 'LINESTRING(1.0 3.0,3.0 1.0)'),
    ], ["id", "geometry"])
    actual = df.withColumn("geometry", ST_GeomFromText(col("geometry")))

    benchmarks = {
        "duration": [],
        "task": [],
    }

    # GeoJSON
    actual.write.format("geojson").mode("overwrite").save("/tmp/a_geojson")
    df = sedona.read.format("geojson").load("/tmp/a_geojson")
    df.createOrReplaceTempView("a_table")
    benchmark(q1, df, benchmarks=benchmarks, name="q1")

    # GeoParquet
    actual.write.format("geoparquet").mode("overwrite").save("/tmp/a_geoparquet")
    df = sedona.read.format("geoparquet").load("/tmp/a_geoparquet")
    df.createOrReplaceTempView("a_table")
    benchmark(q1, df, benchmarks=benchmarks, name="q2")

    res = get_results(benchmarks).set_index("task")
    print(res)
