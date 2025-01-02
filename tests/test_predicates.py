import pytest
from sedona.spark import *
from .sedona import sedona
from pyspark.sql.functions import expr
import chispa


def test_st_contains():
    sql = """
    SELECT ST_Contains(
        ST_GeomFromWKT('POLYGON((175 150,20 40,50 60,125 100,175 150))'),
        ST_GeomFromWKT('POINT(174 149)')
    ) as result
    """
    actual = sedona.sql(sql)
    expected = sedona.createDataFrame([(False,)], ["result"])
    chispa.assert_df_equality(actual, expected)


@pytest.mark.skip(reason="todo")
def test_st_crosses():
    2 + 2


@pytest.mark.skip(reason="todo")
def test_st_disjoint():
    2 + 2


@pytest.mark.skip(reason="todo")
def test_st_dwithin():
    2 + 2


@pytest.mark.skip(reason="todo")
def test_st_equals():
    2 + 2


@pytest.mark.skip(reason="todo")
def test_st_intersects():
    2 + 2


@pytest.mark.skip(reason="todo")
def test_st_orderingequals():
    2 + 2


@pytest.mark.skip(reason="todo")
def test_st_overlaps():
    2 + 2


@pytest.mark.skip(reason="todo")
def test_st_relate():
    2 + 2


@pytest.mark.skip(reason="todo")
def test_st_relatematch():
    2 + 2


@pytest.mark.skip(reason="todo")
def test_st_touches():
    2 + 2


@pytest.mark.skip(reason="todo")
def test_st_within():
    2 + 2


@pytest.mark.skip(reason="todo")
def test_st_covers():
    2 + 2


@pytest.mark.skip(reason="todo")
def test_st_coveredby():
    2 + 2
