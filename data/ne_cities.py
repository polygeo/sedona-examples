import geopandas

# https://www.naturalearthdata.com/
cities_pd = geopandas.read_file(
    "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_populated_places_simple.zip"
)

(
    cities_pd[["nameascii", "geometry"]]
    .rename(columns={"nameascii": "name"})
    .to_parquet("data/ne_cities.parquet")
)
