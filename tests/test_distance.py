from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from sedona.spark import *
from .sedona import sedona
import chispa
import pytest
from pyspark.sql.functions import col


def test_distance():
    df = sedona.createDataFrame([
        ('POINT(2.0 3.0)', 'POINT(6.0 4.0)'),
        ('POINT(6.0 2.0)', 'POINT(9.0 2.0)'),
    ], ["start", "end"])
    df = (df
      .withColumn("start", ST_GeomFromText(col("start")))
      .withColumn("end", ST_GeomFromText(col("end")))
    )

    print("***")

    df.withColumn(
        "distance", 
        ST_Distance(col("start"), col("end"))
    ).show()

    