from sedona.spark import *


config = (
    SedonaContext.builder()
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-spark-3.5_2.12:1.7.0,"
        "org.datasyslab:geotools-wrapper:1.7.0-28.5",
    )
    .config(
        "spark.jars.repositories",
        "https://artifacts.unidata.ucar.edu/repository/unidata-all",
    )
    .config("spark.executor.memory", "12G")
    .config("spark.driver.memory", "12G")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

sedona = SedonaContext.create(config)
sedona.sparkContext.setCheckpointDir("/tmp/my_checkpoint_dir")