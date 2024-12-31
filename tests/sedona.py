from sedona.spark import *


config = (
    SedonaContext.builder()
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-spark-3.5_2.12:1.6.1,"
        "org.datasyslab:geotools-wrapper:1.7.0-28.5",
    )
    .config(
        "spark.jars.repositories",
        "https://artifacts.unidata.ucar.edu/repository/unidata-all",
    )
    .getOrCreate()
)

sedona = SedonaContext.create(config)
