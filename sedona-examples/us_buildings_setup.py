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

con.sql(
    """
    SELECT
        geom.st_aswkb().s2_cellfromwkb().s2_cell_parent(4).s2_cell_token() as partition_cell,
        geom
    FROM 'microsoft-buildings-point.parquet'
    """
).to_parquet("buildings", partition_by="partition_cell", overwrite=True)


SELECT function_name, parameters, description, tags 
FROM duckdb_functions() 
WHERE tags['ext'] = ['geography']


con.sql(
    """
    SELECT function_name, parameters, description, tags 
    FROM duckdb_functions() 
    WHERE tags['ext'] = ['geography']
    """
)

con.sql("FORCE INSTALL 'geography.duckdb_extension'")
con.sql("LOAD geography")





duckdb.sql(
    """
    SELECT function_name, parameters, description, tags 
    FROM duckdb_functions() 
    WHERE tags['ext'] = ['spatial'];
    """
)


con.sql("LOAD spatial;")


con.sql(
    """
    SELECT function_name, parameters, description, tags 
    FROM duckdb_functions() 
    WHERE tags['ext'] = ['spatial'];
    """
)


con.raw_sql(
    """
    COPY (
        SELECT ZCTA5CE20 as zipcode, geom
        FROM 'us-zip-codes/cb_2020_us_zcta520_500k.shp'
    ) TO 'us-zip-codes.fgb' WITH (FORMAT GDAL, DRIVER FlatGeoBuf)
    """
)

con.raw_sql(
    """
    COPY (
        SELECT ZCTA5CE20 as zipcode, geom
        FROM 'us-zip-codes/cb_2020_us_zcta520_500k.shp'
    ) TO 'us-zip-codes.parquet'
    """
)



