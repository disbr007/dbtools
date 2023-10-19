import logging
import os
import sys

import geopandas as gpd
import pandas as pd
import pytest
import pytest_check as check

from dbtools.pg import Postgres, ColumnDetails

logger = logging.getLogger(__name__)

TESTING_SCHEMA = os.getenv("TESTING_SCHEMA")
TESTING_TABLE = os.getenv("TESTING_TABLE")
TESTING_MATVIEW = os.getenv("TESTING_MATVIEW")
TESTING_VIEW = os.getenv("TESTING_VIEW")
if any([obj is None for obj in [TESTING_SCHEMA, TESTING_TABLE, TESTING_VIEW, TESTING_MATVIEW]]):
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

    @pytest.mark.parametrize("relname,expected_relkind", [
        (TESTING_TABLE, "r"),
        (TESTING_VIEW, "v"),
        (TESTING_MATVIEW, "m")
    ])
    def test_get_pg_relkind(self, db_src: Postgres, relname: str, expected_relkind: str, schema: str = TESTING_SCHEMA):
        relkind = db_src.get_pg_relkind(relname=relname, schema=schema)
        check.equal(relkind, expected_relkind)

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

    def test_get_table_column_details(self, db_src: Postgres):
        column_details = db_src.get_table_column_details(table=TESTING_TABLE, schema=TESTING_SCHEMA)
        test_column = column_details[0]
        check.is_instance(test_column, ColumnDetails)
        check.equal(test_column.column_name, "index")
        check.equal(test_column.data_type, "bigint")

    @pytest.mark.parametrize("view_name,col_index,col_name,col_type", [
        (TESTING_VIEW, 5, "Entity_Name", "text"),
        (TESTING_MATVIEW, 0, "year", "bigint")
    ])
    def test_get_view_column_details(self, db_src: Postgres, view_name: str, 
                                     col_index: int, col_name: str, col_type: str):
        column_details = db_src.get_view_column_details(matview=view_name, schema=TESTING_SCHEMA)
        test_column = column_details[col_index]
        check.is_instance(test_column, ColumnDetails)
        check.equal(test_column.column_name, col_name)
        check.equal(test_column.data_type, col_type)

    @pytest.mark.parametrize("name,column_index,column_name,column_type", [
        (TESTING_VIEW, 0, "index", "bigint"),
        (TESTING_VIEW, 5, "Entity_Name", "text"),
        (TESTING_MATVIEW, 0, "year", "bigint")
    ])
    def test_get_column_details(self, db_src: Postgres, name: str, column_name: str, column_index: int, column_type: str, schema: str = TESTING_SCHEMA):
        column_details = db_src.get_column_details(table=name, schema=schema)
        test_column = column_details[column_index]
        check.is_instance(test_column, ColumnDetails)
        check.equal(test_column.column_name, column_name)
        check.equal(test_column.data_type, column_type)
