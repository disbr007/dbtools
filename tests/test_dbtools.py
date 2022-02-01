import json
import unittest

import pandas as pd
import geopandas as gpd
import pytest_check as check

from dbtools.pg import Postgres

HOST = 'aoassetdb.cpvfmepzslta.us-west-2.rds.amazonaws.com'
DATABASE = 'ao_dev'
USER = 'jeffdisbrow'
TESTING_SCHEMA = '_dev_testing'
TESTING_TABLE = 'ev_miso_subs'

# TODO:
# - loading connection parameters test - make sure using intended params


class TestPostgres(unittest.TestCase):
    def test_connection(self):
        with Postgres(host=HOST, database=DATABASE, user=USER) as db_src:
            cxn = db_src.connection
            check.is_false(cxn.closed)

    def test_engine(self):
        with Postgres(host=HOST, database=DATABASE, user=USER) as db_src:
            engine = db_src.get_engine()
            began = engine.begin()
            cxn = began.conn
            check.is_false(cxn.closed)

    def test_list_schemas(self):
        with Postgres(host=HOST, database=DATABASE, user=USER) as db_src:
            schemas = db_src.list_schemas()
            check.is_in('public', schemas)

    def test_list_tables(self):
        with Postgres(host=HOST, database=DATABASE, user=USER) as db_src:
            tables = db_src.list_tables()
            check.is_not_none(tables)
            check.is_true(len(tables) > 0)

    def test_retrieve_records(self):
        with Postgres(host=HOST, database=DATABASE, user=USER) as db_src:
            df = db_src.sql2df(f"SELECT * FROM {TESTING_SCHEMA}.{TESTING_TABLE} LIMIT 10;")
            check.is_instance(df, pd.DataFrame)

    def test_retrieve_geospatial_records(self):
        with Postgres(host=HOST, database=DATABASE, user=USER) as db_src:
            gdf = db_src.sql2gdf(f"SELECT * FROM {TESTING_SCHEMA}.{TESTING_TABLE} LIMIT 10;")
            check.is_instance(gdf, gpd.GeoDataFrame)
