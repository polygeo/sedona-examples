# Sedona Examples

This repo shows how to run PySpark and Apache Sedona on your local machine.

Make sure to install [uv](https://docs.astral.sh/uv/) on your machine to run the examples.

You can run the test suite as follows: `uv run pytest tests`.

Here's how to run the test suite with the printed output displayed: `uv run pytest tests -s`.

## Simple example

Suppose you have the following table with the longitude/latitude for the starting and ending locations of trips:

```
+----------+---------+---------+---------+-------+-------+
|start_city|start_lon|start_lat| end_city|end_lon|end_lat|
+----------+---------+---------+---------+-------+-------+
|  New York|    -74.0|     40.8|Sao Paulo|  -46.6|  -23.5|
| Sao Paulo|    -46.6|    -23.5|      Rio|  -43.2|  -22.9|
+----------+---------+---------+---------+-------+-------+
```

Use Sedona to compute the distance in meters for each trip:

```python
sql = """
SELECT 
    start_city,
    end_city,
    ST_DistanceSphere(
        ST_Point(start_lon, start_lat),
        ST_Point(end_lon, end_lat)) AS distance
FROM some_trips
"""
sedona.sql(sql).show()
```

Here's the result:

```

+----------+---------+-----------------+
|start_city| end_city|         distance|
+----------+---------+-----------------+
|  New York|Sao Paulo|7690115.108592422|
| Sao Paulo|      Rio|353827.8316554592|
+----------+---------+-----------------+
```
