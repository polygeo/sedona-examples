# blog post: https://dewey.dunnington.ca/post/2024/wrangling-and-joining-130m-points-with-duckdb--the-open-source-spatial-stack/

duckdb.sql("INSTALL spatial")
duckdb.sql("LOAD spatial")

duckdb.sql(
    """
    COPY (
        SELECT geom,
        FROM 'microsoft-buildings-point.fgb'
    ) TO 'microsoft-buildings-point.parquet'
    """
)


import duckdb
con = duckdb.connect(config={"allow_unsigned_extensions": True})
# grab extension here: https://github.com/paleolimbot/duckdb-geography/actions/runs/12451008452#artifacts
con.sql("INSTALL 'geography.duckdb_extension'")
con.load_extension("geography")
con.sql("LOAD geography")

duckdb.sql(
    """
    SELECT
        geom.st_aswkb().s2_cellfromwkb().s2_cell_parent(4).s2_cell_token() as partition_cell,
        geom
    FROM 'microsoft-buildings-point.parquet'
    """
).to_parquet("buildings", partition_by="partition_cell", overwrite=True)
