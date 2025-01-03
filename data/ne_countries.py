import geopandas

# https://www.naturalearthdata.com/
countries_pd = geopandas.read_file(
    "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"
)

(
    countries_pd[["ADMIN", "CONTINENT", "geometry"]]
    .rename(columns={"ADMIN": "name", "CONTINENT": "continent"})
    .to_parquet("data/ne_countries.parquet")
)
