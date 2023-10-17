import logging
import os
import sys

import geopandas as gpd
import pandas as pd
import pytest_check as check

from dbtools.pg import Postgres, ColumnDetails

logger = logging.getLogger(__name__)

TESTING_SCHEMA = os.getenv("TESTING_SCHEMA")
TESTING_TABLE = os.getenv("TESTING_TABLE")
TESTING_MATVIEW = os.getenv("TESTING_MATVIEW")
if any([TESTING_SCHEMA is None, TESTING_TABLE is None]):
    logger.error("Must set TESTING_SCHEMA and TESTING_TABLE environmental variables.")
    sys.exit(-1)


class TestPostgres:
    def test_connection(self, pgconfig):
        with Postgres(**pgconfig.non_wildcard_atts) as db_src:
            cxn = db_src.connection
            check.is_false(cxn.closed)
        check.is_true(cxn.closed)

    def test_engine(self, db_src):
        engine = db_src.get_engine()
        began = engine.begin()
        cxn = began.conn
        check.is_false(cxn.closed)

    def test_list_schemas(self, db_src: Postgres):
        schemas = db_src.list_schemas()
        check.is_in("public", schemas)

    def test_list_tables(self, db_src: Postgres):
        tables = db_src.list_tables()
        check.is_not_none(tables)
        check.is_true(len(tables) > 0)

    def test_sql2df(self, db_src: Postgres):
        df = db_src.sql2df(f"SELECT * FROM {TESTING_SCHEMA}.{TESTING_TABLE} LIMIT 10;")
        check.is_instance(df, pd.DataFrame)

    def test_sql2gdf(self, db_src: Postgres):
        gdf = db_src.sql2gdf(f"SELECT * FROM {TESTING_SCHEMA}.{TESTING_TABLE} LIMIT 10;")
        check.is_instance(gdf, gpd.GeoDataFrame)

    def test_table2df(self, db_src: Postgres):
        df = db_src.table2df(table=TESTING_TABLE, schema=TESTING_SCHEMA)
        gdf = db_src.table2df(table=TESTING_TABLE, schema=TESTING_SCHEMA, gdf=True)
        check.is_instance(df, pd.DataFrame)
        check.is_instance(gdf, gpd.GeoDataFrame)

    def test_get_matview_srid(self, db_src: Postgres):
        srid = db_src.get_geometry_srid(table=TESTING_MATVIEW, schema=TESTING_SCHEMA)
        check.is_not_none(srid)

    def test_get_table_columns(self, db_src: Postgres):
        existing_columns = [
            "index",
            "geometry",
            "Year",
            "Month",
            "Entity_ID",
            "Entity_Name",
            "Plant_Name",
            "Plant_State",
            "Generator_ID",
            "Net_Summer_Capacity__MW_",
            "Technology",
            "Prime_Mover_Code",
            "X",
            "Y",
            "ObjectId"
            ]
        retrieved_columns = db_src.get_table_columns(table=TESTING_TABLE, schema=TESTING_SCHEMA)
        check.is_not_none(retrieved_columns)
        check.greater(len(retrieved_columns), 0)
        check.equal(len(retrieved_columns), 15)
        check.equal(sorted(existing_columns), sorted(retrieved_columns))

    def test_get_column_details(self, db_src: Postgres):
        column_details = db_src.get_table_column_details(table=TESTING_TABLE, schema=TESTING_SCHEMA)
        test_column = column_details[0]
        check.is_instance(test_column, ColumnDetails)
        check.equal(test_column.column_name, "index")
        check.equal(test_column.data_type, "bigint")
