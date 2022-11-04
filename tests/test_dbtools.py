import logging
import os
import sys

import dotenv
import pandas as pd
import geopandas as gpd
import pytest_check as check

from dbtools.pg import Postgres, load_pgconfig

logger = logging.getLogger(__name__)

dotenv.load_dotenv('.env.test')
pgconfig = load_pgconfig()

TESTING_SCHEMA = os.getenv('TESTING_SCHEMA')
TESTING_TABLE = os.getenv('TESTING_TABLE')
if any([TESTING_SCHEMA is None, TESTING_TABLE is None]):
    logger.error('Must set TESTING_SCHEMA and TESTING_TABLE environmental variables.')
    sys.exit(-1)


class TestPostgres():
    def test_connection(self):
        with Postgres(**pgconfig.non_wildcard_atts) as db_src:
            cxn = db_src.connection
            check.is_false(cxn.closed)
        check.is_true(cxn.closed)

    def test_engine(self):
        with Postgres(**pgconfig.non_wildcard_atts) as db_src:
            engine = db_src.get_engine()
            began = engine.begin()
            cxn = began.conn
            check.is_false(cxn.closed)

    def test_list_schemas(self):
        with Postgres(**pgconfig.non_wildcard_atts) as db_src:
            schemas = db_src.list_schemas()
            check.is_in('public', schemas)

    def test_list_tables(self):
        with Postgres(**pgconfig.non_wildcard_atts) as db_src:
            tables = db_src.list_tables()
            check.is_not_none(tables)
            check.is_true(len(tables) > 0)

    def test_sql2df(self):
        with Postgres(**pgconfig.non_wildcard_atts) as db_src:
            df = db_src.sql2df(f"SELECT * FROM {TESTING_SCHEMA}.{TESTING_TABLE} LIMIT 10;")
            check.is_instance(df, pd.DataFrame)

    def test_sql2gdf(self):
        with Postgres(**pgconfig.non_wildcard_atts) as db_src:
            gdf = db_src.sql2gdf(f"SELECT * FROM {TESTING_SCHEMA}.{TESTING_TABLE} LIMIT 10;")
            check.is_instance(gdf, gpd.GeoDataFrame)
    
    def test_table2df(self):
        with Postgres(**pgconfig.non_wildcard_atts) as db_src:
            df = db_src.table2df(table=TESTING_TABLE,
                                 schema=TESTING_SCHEMA)
            gdf = db_src.table2df(table=TESTING_TABLE,
                                  schema=TESTING_SCHEMA,
                                  gdf=True)
            check.is_instance(df, pd.DataFrame)
            check.is_instance(gdf, gpd.GeoDataFrame)
