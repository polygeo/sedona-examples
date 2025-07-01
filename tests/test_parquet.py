from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from sedona.spark import *
from .sedona import sedona
import chispa
import pytest
from pyspark.sql.functions import col

from shapely import Point, Polygon, LineString
import pyarrow.parquet as pq
import glob
import os


def test_create_parquet():
    df = sedona.createDataFrame([
        ("a", 1),
        ("b", 2),
    ], ["letter", "number"])
    res = df.selectExpr("letter", "uuid()")
    res.show(truncate=False)
    res.printSchema()
    directory = "tmp/parquet_uuid"
    res.repartition(1).write.format("parquet").mode("overwrite").save(directory)
    files = sorted(glob.glob(os.path.join(directory, '*.parquet')))
    print(files)
    # path = glob.glob("tmp/parquet_uuid")[0]
    path = files[0]
    print(path)
    parquet_file = pq.ParquetFile(path)
    print("*** Parquet File info ***")
    print("metadata")
    print(parquet_file.metadata)
    print("schema")
    print(parquet_file.schema)
