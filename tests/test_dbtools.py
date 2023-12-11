import logging
import os
import sys
from typing import Tuple

import geopandas as gpd
import pandas as pd
import pytest
import pytest_check as check
from psycopg2 import sql

from dbtools.pg import ColumnDetails, Postgres

logger = logging.getLogger(__name__)

TESTING_SCHEMA = os.getenv("TESTING_SCHEMA")
TESTING_TABLE = os.getenv("TESTING_TABLE")
TESTING_MATVIEW = os.getenv("TESTING_MATVIEW")
TESTING_VIEW = os.getenv("TESTING_VIEW")
if None in [TESTING_SCHEMA, TESTING_TABLE, TESTING_VIEW, TESTING_MATVIEW]:
    logger.error("Must set TESTING_SCHEMA and TESTING_TABLE environmental variables.")
    sys.exit(-1)


@pytest.fixture(name="table_to_drop")
def fixture_table_to_drop(db_src: Postgres) -> str:
    table_to_drop_name = "table_to_drop"
    db_src.execute_sql(
        sql.SQL(
            "CREATE TABLE IF NOT EXISTS {testing_schema}.{table_to_drop_name} AS "
            "SELECT * FROM {testing_schema}.{testing_table};"
        ).format(
            testing_schema=sql.Identifier(TESTING_SCHEMA),
            table_to_drop_name=sql.Identifier(table_to_drop_name),
            testing_table=sql.Identifier(TESTING_TABLE),
        ),
        no_result_expected=True,
    )
    yield table_to_drop_name
    db_src.execute_sql(
        sql.SQL("DROP TABLE IF EXISTS {testing_schema}.{table_to_drop_name};").format(
            testing_schema=sql.Identifier(TESTING_SCHEMA),
            table_to_drop_name=sql.Identifier(table_to_drop_name),
        ),
        no_result_expected=True,
    )


@pytest.fixture(name="matview_to_drop")
def fixture_matview_to_drop(db_src: Postgres) -> str:
    matview_to_drop_name = "matview_to_drop"
    db_src.execute_sql(
        sql.SQL(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS {testing_schema}.{matview_to_drop_name} AS "
            "SELECT * FROM {testing_schema}.{existing_table};"
        ).format(
            testing_schema=sql.Identifier(TESTING_SCHEMA),
            matview_to_drop_name=sql.Identifier(matview_to_drop_name),
            existing_table=sql.Identifier(TESTING_TABLE),
        ),
        no_result_expected=True,
    )
    yield matview_to_drop_name
    db_src.execute_sql(
        sql.SQL("DROP MATERIALIZED VIEW IF EXISTS {testing_schema}.{matview_to_drop_name};").format(
            testing_schema=sql.Identifier(TESTING_SCHEMA),
            matview_to_drop_name=sql.Identifier(matview_to_drop_name),
        ),
        no_result_expected=True,
    )


@pytest.fixture(name="hotswap")
def fixture_hotswap(db_src: Postgres) -> Tuple[str, str]:
    active_table_name = "hotswap_active_table"
    new_table_name = "hotswap_active_table_temp"
    old_table_suffix = "old_active"
    renamed_active = f"{active_table_name}_{old_table_suffix}"
    # Create tables
    active_create_tbl_sql = sql.SQL(
        "CREATE TABLE IF NOT EXISTS {testing_schema}.{active_table_name} ("
        "id SERIAL PRIMARY KEY, "
        "sample_value INT"
        ")"
    ).format(
        testing_schema=sql.Identifier(TESTING_SCHEMA),
        active_table_name=sql.Identifier(active_table_name),
    )
    active_populate_tbl_sql = sql.SQL(
        "INSERT INTO {testing_schema}.{active_table_name} (sample_value) VALUES "
        "(1), "
        "(2), "
        "(3)"
    ).format(
        testing_schema=sql.Identifier(TESTING_SCHEMA),
        active_table_name=sql.Identifier(active_table_name),
    )
    new_table_create_sql = sql.SQL(
        "CREATE TABLE IF NOT EXISTS {testing_schema}.{new_table_name} AS SELECT * FROM {testing_schema}.{active_table_name}"
    ).format(
        testing_schema=sql.Identifier(TESTING_SCHEMA),
        new_table_name=sql.Identifier(new_table_name),
        active_table_name=sql.Identifier(active_table_name),
    )
    differentiate_new_table_sql = sql.SQL(
        "UPDATE {testing_schema}.{new_table_name} SET sample_value = 0;"
    ).format(
        testing_schema=sql.Identifier(TESTING_SCHEMA),
        new_table_name=sql.Identifier(new_table_name),
    )
    db_src.execute_sql(active_create_tbl_sql, no_result_expected=True)
    db_src.execute_sql(active_populate_tbl_sql, no_result_expected=True)
    db_src.execute_sql(new_table_create_sql, no_result_expected=True)
    db_src.execute_sql(differentiate_new_table_sql, no_result_expected=True)
    yield (active_table_name, new_table_name, old_table_suffix)
    # Drop tables
    active_drop_sql = sql.SQL("DROP TABLE IF EXISTS {testing_schema}.{active_table_name};").format(
        testing_schema=sql.Identifier(TESTING_SCHEMA),
        active_table_name=sql.Identifier(active_table_name),
    )
    new_drop_sql = sql.SQL("DROP TABLE IF EXISTS {testing_schema}.{new_table_name};").format(
        testing_schema=sql.Identifier(TESTING_SCHEMA),
        new_table_name=sql.Identifier(new_table_name),
    )
    renamed_active_drop_sql = sql.SQL(
        "DROP TABLE IF EXISTS {testing_schema}.{renamed_active};"
    ).format(
        testing_schema=sql.Identifier(TESTING_SCHEMA),
        renamed_active=sql.Identifier(renamed_active),
    )
    db_src.execute_sql(active_drop_sql, no_result_expected=True)
    db_src.execute_sql(new_drop_sql, no_result_expected=True)
    db_src.execute_sql(renamed_active_drop_sql, no_result_expected=True)


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

    @pytest.mark.parametrize(
        "relname,expected_relkind",
        [(TESTING_TABLE, "r"), (TESTING_VIEW, "v"), (TESTING_MATVIEW, "m")],
    )
    def test_get_pg_relkind(
        self, db_src: Postgres, relname: str, expected_relkind: str, schema: str = TESTING_SCHEMA
    ):
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
            "ObjectId",
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

    @pytest.mark.parametrize(
        "view_name,col_index,col_name,col_type",
        [(TESTING_VIEW, 5, "Entity_Name", "text"), (TESTING_MATVIEW, 0, "year", "bigint")],
    )
    def test_get_view_column_details(
        self, db_src: Postgres, view_name: str, col_index: int, col_name: str, col_type: str
    ):
        column_details = db_src.get_view_column_details(matview=view_name, schema=TESTING_SCHEMA)
        test_column = column_details[col_index]
        check.is_instance(test_column, ColumnDetails)
        check.equal(test_column.column_name, col_name)
        check.equal(test_column.data_type, col_type)

    @pytest.mark.parametrize(
        "name,column_index,column_name,column_type",
        [
            (TESTING_VIEW, 0, "index", "bigint"),
            (TESTING_VIEW, 5, "Entity_Name", "text"),
            (TESTING_MATVIEW, 0, "year", "bigint"),
        ],
    )
    def test_get_column_details(
        self,
        db_src: Postgres,
        name: str,
        column_name: str,
        column_index: int,
        column_type: str,
        schema: str = TESTING_SCHEMA,
    ):
        column_details = db_src.get_column_details(table=name, schema=schema)
        test_column = column_details[column_index]
        check.is_instance(test_column, ColumnDetails)
        check.equal(test_column.column_name, column_name)
        check.equal(test_column.data_type, column_type)

    def test_get_view_definition(self, db_src: Postgres):
        view_definition = db_src.get_view_definition(view=TESTING_MATVIEW, schema=TESTING_SCHEMA)
        check.is_in("SELECT", view_definition)
        check.is_in("FROM", view_definition)

    def test_drop_table(self, db_src: Postgres, table_to_drop: str):
        db_src.drop_table(table=table_to_drop, schema=TESTING_SCHEMA)
        check.is_false(db_src.table_exists(table=table_to_drop, schema=TESTING_SCHEMA))

    def test_drop_table_or_view(self, db_src: Postgres, matview_to_drop: str):
        db_src.drop_table_or_view(database_object=matview_to_drop, schema=TESTING_SCHEMA)
        check.is_false(db_src.table_or_view_exists(database_object=matview_to_drop))

    def test_hotswap(self, db_src: Postgres, hotswap: Tuple[str, str]):
        active_table_name, new_table_name, old_table_suffix = hotswap
        db_src.hotswap_table(
            active_table=active_table_name,
            temp_table=new_table_name,
            schema=TESTING_SCHEMA,
            old_table_suffix=old_table_suffix,
        )
        # Ensure new table is being used by confirming values
        get_values_sql = sql.SQL(
            "SELECT sample_value FROM {testing_schema}.{active_table_name} LIMIT 1;"
        ).format(
            testing_schema=sql.Identifier(TESTING_SCHEMA),
            active_table_name=sql.Identifier(active_table_name),
        )
        results = db_src.execute_sql(get_values_sql)
        check.equal(results[0][0], 0)
