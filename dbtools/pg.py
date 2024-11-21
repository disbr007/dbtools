from dataclasses import dataclass
import datetime
import decimal
import json
import logging
import re
import os
import urllib
from pathlib import Path
from typing import Literal, List, Union, Optional, Tuple

from dotenv import load_dotenv
import geopandas as gpd
import pandas as pd
import psycopg2
from psycopg2 import sql
from pyproj import CRS
from pyproj.exceptions import CRSError
import shapely
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import sqlalchemy
from tqdm import tqdm


logger = logging.getLogger(__name__)

load_dotenv()

# Supress pandas SettingWithCopyWarning
pd.set_option("mode.chained_assignment", None)


# Constants
DEF_SKIP_SCHEMAS = ("information_schema", "pg_catalog")
FAIL = "fail"


@dataclass
class PGConfig:
    host: str
    port: str
    database: str
    user: str
    password: str

    def __post_init__(self):
        self.non_wildcard_atts = {att: value for att, value in self.__dict__.items() if value != "*"}


@dataclass
class ColumnDetails:
    column_name: str
    data_type: str
    is_nullable: Optional[str]
    character_maximum_length: Optional[int]
    numeric_precision: Optional[int]


def load_pgconfig(host: str = None) -> PGConfig:
    pghost = os.environ.get("PGHOST", host)
    if pghost is None:
        logger.error(
            "Must provided PGHOST, either as argument or via PGHOST environmental " "variable."
        )
        raise Exception("No PGHOST specified.")
    pgport = os.environ.get("PGPORT")
    pgdatabase = os.environ.get("PGDATABASE")
    pguser = os.environ.get("PGUSER")
    pgpassword = os.environ.get("PGPASSWORD")
    pgpassfile = os.environ.get("PGPASSFILE", Path.home() / ".pgpass")
    try:
        with open(pgpassfile, "r") as src:
            logger.info("using pgpassfile")
            connection_configs = [cc.strip().split(":") for cc in src.readlines()]
            logger.info(f"len connection configs: {len(connection_configs)}")
            logger.info(f"connection configs: {connection_configs}")
    except FileNotFoundError:
        logger.debug("pgpass file not found, reading from environmental variables.")
        connection_configs = []
    if len(connection_configs) == 0:
        # Use environmental variables
        pgconfig = PGConfig(
            host=pghost, port=pgport, database=pgdatabase, user=pguser, password=pgpassword
        )
    else:
        logger.info(f"connection configs: {connection_configs} {len(connection_configs)}")
        configs = {}
        for host, port, database, user, *password in connection_configs:
            if len(password) == 1:
                password = password[0]
            configs[host] = PGConfig(
                host=host if pghost is None else pghost,
                port=port if pgport is None else pgport,
                database=database if pgdatabase is None else pgdatabase,
                user=user if pguser is None else pguser,
                password=password if pgpassword is None else pgpassword,
            )
        pgconfig = configs.get(pghost)
    if pgconfig is None:
        raise KeyError(f"Error retrieving config for host: {pghost}. Available hosts: "
                       f"{configs.keys()}")
    return pgconfig


def get_geometry_cols(gdf):
    """Gets all columns in a geodataframe that are of type geometry.
    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        GeoDataFrame to find geometry columns in

    Returns
    -------
    list : Names of columns that are of type 'geometry'
    """
    shapely_geoms = (
        shapely.geometry.collection.GeometryCollection,
        shapely.geometry.linestring.LineString,
        shapely.geometry.polygon.LinearRing,
        shapely.geometry.multilinestring.MultiLineString,
        shapely.geometry.multipoint.MultiPoint,
        shapely.geometry.multipolygon.MultiPolygon,
        shapely.geometry.point.Point,
        shapely.geometry.polygon.Polygon,
    )
    geom_cols = []
    for col in gdf.columns:
        if type(gdf[col].values[0]) in shapely_geoms:
            geom_cols.append(col)

    return geom_cols


def check_where(where, join="AND"):
    if where:
        where += """ {} """.format(join)
    else:
        where = ""

    return where


def ids2sql(ids):
    return str(ids)[1:-1]


def columns_to_sql(columns: list) -> sql.SQL:
    """
    Convert a list of column names as strings to sql.Identifiers, which
    ensures proper quoting.
    """
    # TODO: Fix this 't' - find out what needs 't' and add arugment for "qualifier"
    columns_sql = [sql.Identifier("t", c) for c in columns]
    columns_sql = sql.SQL(", ").join(columns_sql)
    return columns_sql


def encode_geom_sql(geom_col, encode_geom_col):
    """
    SQL statement to encode geometry column in non-PostGIS Postgres
    database for reading by geopandas.read_postgis.
    """
    geom_sql = "encode(ST_AsBinary({}), 'hex') AS " "{}".format(geom_col, encode_geom_col)

    return geom_sql


def make_identifier(sql_str):
    if sql_str is not None and not isinstance(sql_str, sql.Identifier):
        return sql.Identifier(sql_str)


def generate_sql(
    layer,
    columns=None,
    where=None,
    orderby=False,
    schema=None,
    orderby_asc=False,
    distinct=False,
    limit=False,
    offset=None,
    geom_col=None,
    encode_geom_col_as="geometry",
    remove_id_tbl=None,
    remove_id_tbl_cols=None,
    remove_id_src_cols=None,
):
    """
    geom_col not needed for PostGIS if loading SQL with geopandas -
        gpd can interpet the geometry column without encoding
    """
    if distinct:
        sql_select = "SELECT DISTINCT"
    else:
        sql_select = "SELECT"
    if isinstance(columns, str):
        columns = [columns]
    if columns is not None:
        fields = sql.SQL(",").join([sql.Identifier(f) for f in columns])
    else:
        fields = sql.SQL("*")

    # Only necessary for geometries in non-PostGIS DBs
    if geom_col:
        # Create base query object, with geometry encoding
        geom_encode_str = encode_geom_sql(geom_col=geom_col, encode_geom_col=encode_geom_col_as)
        # geom_encode_str = "encode(ST_AsBinary({}), 'hex') AS {}".format(geom_col, encode_geom_col_as)
        query = sql.SQL("{select} {fields}, {geom_encode_str} FROM {table}").format(
            select=sql.SQL(sql_select),
            fields=fields,
            geom_encode_str=sql.SQL(geom_encode_str),
            table=sql.Identifier(layer),
        )
    else:
        # Create base query object
        query = sql.SQL("{select} {fields} FROM {schema}.{table}").format(
            select=sql.SQL(sql_select),
            fields=fields,
            schema=sql.Identifier(schema),
            table=sql.Identifier(layer),
        )

    # Add any provided additional parameters
    # Drop records that have ID in other table
    if all([remove_id_tbl, remove_id_tbl_cols, remove_id_src_cols]):
        if isinstance(remove_id_src_cols, str):
            remove_id_src_cols = [remove_id_src_cols]
        if isinstance(remove_id_tbl_cols, str):
            remove_id_tbl_cols = [remove_id_tbl_cols]
        if not len(remove_id_tbl_cols) == len(remove_id_src_cols):
            logger.error(
                "Error creating LEFT JOIN clause: "
                "length of remove_id_tbl_cols ({}) != "
                "length of remove_ids_src_cols ({})".format(
                    len(remove_id_tbl_cols), len(remove_id_src_cols)
                )
            )
            raise Exception
        jss = []
        for i, col in enumerate(remove_id_tbl_cols):
            js = "{0}.{1} = {2}.{3}".format(remove_id_tbl, col, layer, remove_id_src_cols[i])
            jss.append(js)

        join_stmts = " AND ".join(jss)
        join_stmts = " LEFT JOIN {} ON {}".format(remove_id_tbl, join_stmts)
        # join_stmt = "LEFT JOIN {0} " \
        #             "ON {0}.{1} = {2}.{3}".format(remove_id_tbl,
        #                                           remove_id_tbl_cols,
        #                                           layer,
        #                                           remove_id_src_cols)

        join_where = "{}.{} IS NULL".format(remove_id_tbl, remove_id_tbl_cols[0])
        query += sql.SQL(join_stmts)
        if where is not None:
            where = "{} AND {}".format(join_where, where)
        else:
            where = join_stmts
    if where:
        sql_where = " WHERE {}".format(where)
        query += sql.SQL(sql_where)
    if orderby:
        if orderby_asc:
            asc = "ASC"
        else:
            asc = "DESC"
        sql_orderby = sql.SQL("ORDER BY {field} {asc}").format(
            field=sql.Identifier(orderby), asc=sql.Literal(asc)
        )
        query += sql_orderby
    if limit:
        sql_limit = sql.SQL("LIMIT {}".format(limit))
        query += sql_limit
    if offset:
        sql_offset = sql.SQL("OFFSET {}".format(offset))
        query += sql_offset

    logger.debug("Generated SQL: {}".format(query))

    return query


def intersect_aoi_where(aoi, geom_col):
    """
    Create a where statement for a PostGIS intersection between the
    geometry(s) in the aoi geodataframe and a PostGIS table with
    geometry in geom_col.
    """
    aoi_epsg = aoi.crs.to_epsg()
    aoi_wkts = [geom.wkt for geom in aoi.geometry]
    intersect_wheres = [
        "ST_Intersects({}, ST_SetSRID('{}'::geometry, "
        "{}))".format(
            geom_col,
            wkt,
            aoi_epsg,
        )
        for wkt in aoi_wkts
    ]
    aoi_where = " OR ".join(intersect_wheres)

    return aoi_where


def drop_z_dim(gdf: gpd.GeoDataFrame):
    # TODO: Move to gpdtools
    """Drop Z values from geodataframe geometries"""
    gdf.geometry = gdf.geometry.apply(
        lambda x: shapely.wkb.loads(shapely.wkb.dumps(x, output_dimension=2))
    )
    return gdf


@dataclass
class Postgres(object):
    """Class for interacting with Postgres database using psycopg2.

    This allows keeping a connection and cursor open while performing multiple operations.
    Best used with a context manager, i.e.: with Postgres(db_name) as db:
        db.execute_sql(...)
    """

    _instance = None

    def __init__(
        self,
        host: str = None,
        port: str = 5432,
        database: str = None,
        user: str = None,
        password: str = None,
        connect_args: dict = None
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

        if connect_args is None:
            connect_args = {}
        self.connect_args: dict = connect_args
        self._engine: Engine = None
        self._connection = None
        self._cursor = None
        self._py2sql_types = {
            int: sqlalchemy.sql.sqltypes.BigInteger,
            str: sqlalchemy.sql.sqltypes.Unicode,
            float: sqlalchemy.sql.sqltypes.Float,
            decimal.Decimal: sqlalchemy.sql.sqltypes.Numeric,
            datetime.datetime: sqlalchemy.sql.sqltypes.DateTime,
            bytes: sqlalchemy.sql.sqltypes.LargeBinary,
            bool: sqlalchemy.sql.sqltypes.Boolean,
            datetime.date: sqlalchemy.sql.sqltypes.Date,
            datetime.time: sqlalchemy.sql.sqltypes.Time,
            datetime.timedelta: sqlalchemy.sql.sqltypes.Interval,
            list: sqlalchemy.sql.sqltypes.ARRAY,
            dict: sqlalchemy.sql.sqltypes.JSON,
        }
        self._relkind_to_table_type = {"r": "TABLE", "v": "VIEW", "m": "MATERIALIZED VIEW"}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.connection is not None and not self.connection.closed:
                if not self.cursor.closed:
                    self.cursor.close()
                self.connection.close()
        except psycopg2.OperationalError as e:
            logger.error("Error attempt to ensure connection closed.")

    def __del__(self):
        try:
            if self.connection is not None and not self.connection.closed:
                if not self.cursor.closed:
                    self.cursor.close()
                self.connection.close()
        except psycopg2.OperationalError as e:
            logger.error("Error attempt to ensure connection closed.")

    @property
    def connection(self):
        """Establish connection to database."""
        # TODO: fix this - make function get_connection() that's called when a connection is needed
        if self._connection is None:
            try:
                self._connection = psycopg2.connect(
                    host=self.host,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    port=self.port,
                    # **self.db_config,
                )
            except (psycopg2.Error, psycopg2.OperationalError) as error:
                Postgres._instance = None
                logger.error(f"Error connecting to {self.database} at {self.host}")
                raise error
            else:
                logger.debug(f"Connection to {self.database} at {self.host} established.")

        return self._connection

    @property
    def cursor(self):
        if self._cursor is None:
            self._cursor = self.connection.cursor()
        if self._cursor.closed:
            self._cursor = self.connection.cursor()

        return self._cursor

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = self.get_engine()
        return self._engine

    def get_engine(self) -> Engine:
        """Create sqlalchemy.engine object."""
        if self.password is not None:
            passwd = urllib.parse.quote(self.password)
            cxn_str = (
                f"postgresql+psycopg2://{self.user}:{passwd}@{self.host}/{self.database}"
            )
        else:
            cxn_str = f"postgresql+psycopg2://{self.user}@{self.host}/{self.database}"
        engine = create_engine(cxn_str, connect_args=self.connect_args)

        return engine

    def get_engine_pools(self, pool_size=5, max_overflow=10):
        """Create sqlalchemy.engine object."""
        engine = create_engine(
            "postgresql+psycopg2://" "{user}:{password}@{host}/{database}".format(**self.db_config),
            pool_size=pool_size,
            max_overflow=max_overflow,
            connect_args=self.connect_args
        )

        return engine

    def get_pg_relkind(
        self, relname: str, schema: str
    ) -> Literal["r", "i", "S", "v", "m", "c", "t", "f"]:
        """Get the type of object that relname is.

        r = ordinary table
        i = index
        S = sequence
        v = view
        m = materialized view
        c = composite type
        t = TOAST table
        f = foreign table
        """
        relkind_sql = sql.SQL(
            "SELECT relkind "
            "FROM pg_catalog.pg_class AS c "
            "JOIN pg_catalog.pg_namespace AS ns "
            "  ON c.relnamespace = ns.oid "
            "WHERE relname = {relname} AND nspname = {schema}; "
        ).format(relname=sql.Literal(relname), schema=sql.Literal(schema))
        results = self.execute_sql(relkind_sql)
        if len(results) == 0:
            logger.error(f"No results found for relname {schema}.{relname}")
            return None
        elif len(results) > 1:
            logger.error(f"More than one result found for relname: {schema}.{relname}")
        return results[0][0]

    def list_schemas(self, include_infoschemas=False):
        """List all schemas in the database"""
        logger.debug("Listing schemas...")
        schemas_sql = sql.SQL("""SELECT nspname FROM pg_catalog.pg_namespace""")
        self.cursor.execute(schemas_sql)
        schemas = self.cursor.fetchall()
        if include_infoschemas:
            # TODO: permissions issue on information_schema
            infoschemas_sql = sql.SQL("SELECT schema_name FROM information_schema.schemata")
            self.cursor.execute(infoschemas_sql)
            infoschemas = self.cursor.fetchall()
            schemas.extend(infoschemas)
        # Remove duplicates
        schemas = set([s[0] for s in schemas])
        logger.debug(f"Schema count: {len(schemas)}")
        return schemas

    def list_tables(
        self, schemas=None, qualified: bool = True, skip_schemas=DEF_SKIP_SCHEMAS
    ) -> List[str]:
        """List all tables in the database."""
        logger.debug("Listing tables...")
        tables_sql = sql.SQL("SELECT schemaname, tablename " "FROM pg_catalog.pg_tables")
        if schemas:
            if not isinstance(schemas, list):
                schemas = [schemas]
            schemas_str = str(schemas)[1:-1]
            tables_sql = tables_sql + sql.SQL(f" WHERE schemaname IN ({schemas_str})")
        self.cursor.execute(tables_sql)
        schemas_tables = self.cursor.fetchall()
        if qualified:
            schemas_tables = [
                "{}.{}".format(s, t) for s, t in schemas_tables if s not in skip_schemas
            ]
        else:
            schemas_tables = [t for s, t in schemas_tables]
        logger.debug(f"Table count: {len(schemas_tables)}")

        return schemas_tables

    def list_views(
        self, schemas: list = None, qualified: bool = True, skip_schemas=DEF_SKIP_SCHEMAS
    ) -> List[str]:
        logger.debug("Listing views...")
        views_sql = sql.SQL(
            """SELECT schemaname, viewname
                               FROM pg_catalog.pg_views"""
        )
        if schemas:
            if not isinstance(schemas, list):
                schemas = [schemas]
            schemas_str = str(schemas)[1:-1]
            views_sql = views_sql + sql.SQL(f" WHERE schemaname IN ({schemas_str})")
        self.cursor.execute(views_sql)
        schemas_views = self.cursor.fetchall()
        if qualified:
            schemas_views = ["{}.{}".format(s, t) for s, t in schemas_views if s not in skip_schemas]
        else:
            schemas_views = [v for s, v in schemas_views]
        logger.debug("Views: {}".format(schemas_views))

        return schemas_views

    def list_matviews(
        self, schemas: list = None, qualified: bool = True, skip_schemas=DEF_SKIP_SCHEMAS
    ) -> List[str]:
        logger.debug("Listing Materialized Views...")
        matviews_sql = sql.SQL(
            """SELECT schemaname, matviewname
                                  FROM pg_catalog.pg_matviews"""
        )
        if schemas:
            if not isinstance(schemas, list):
                schemas = [schemas]
            schemas_str = str(schemas)[1:-1]
            matviews_sql = matviews_sql + sql.SQL(f" WHERE schemaname IN ({schemas_str})")
        self.cursor.execute(matviews_sql)
        schemas_matviews = self.cursor.fetchall()
        if qualified:
            schemas_matviews = [
                "{}.{}".format(s, t) for s, t in schemas_matviews if s not in skip_schemas
            ]
        else:
            schemas_matviews = [t for s, t in schemas_matviews]
        logger.debug("Materialized Views: {}".format(schemas_matviews))

        return schemas_matviews

    def list_db_all(self, schemas: Union[str, list], qualified: bool = True) -> List[str]:
        tables = self.list_tables(schemas=schemas, qualified=qualified)
        views = self.list_views(schemas=schemas, qualified=qualified)
        matviews = self.list_matviews(schemas=schemas, qualified=qualified)

        all_layers = tables + views + matviews

        return all_layers

    def execute_sql(self, sql_query, commit=True, no_result_expected=False):
        """Execute the passed query on the database."""
        if not isinstance(sql_query, (sql.SQL, sql.Composable, sql.Composed)):
            sql_query = sql.SQL(sql_query)
        logger.debug("SQL query: {}".format(sql_query.as_string(self.connection)))
        self.cursor.execute(sql_query)
        if not no_result_expected:
            try:
                results = self.cursor.fetchall()
            except psycopg2.ProgrammingError as e:
                logger.error(e)
                raise (e)
        else:
            results = None
            # if 'no results to fetch' in e.args:
        #         TODO: Do this without an exception catch
        # results = None
        # else:
        #     logger.error(e)
        if commit:
            self.connection.commit()

        return results

    def schema_exists(self, schema):
        """True if schema exists"""
        db_schemas = self.list_schemas()
        return schema in db_schemas

    def table_exists(self, table: str, schema: str = None, qualified: bool = True):
        """True if table exists in schema.

        Args:
        ----
            table: str
                Name of table to check existence of
            schema: str
                Name of schema to check in
            qualified: bool
                Whether the [table] passed is qualified

        Returns: bool
            True if table exists in schema
        """
        if not qualified or schema is not None:
            table = f"{schema}.{table}"
        schema_tables = self.list_tables(schemas=schema)
        return table in schema_tables

    def view_exists(self, view: str, schema: str = None, qualified: bool = True):
        if not qualified or schema is not None:
            view = f"{schema}.{view}"
        schema_matviews = self.list_matviews(schemas=schema)
        schema_views = self.list_views(schemas=schema)
        all_views = schema_matviews + schema_views
        return view in all_views

    def table_or_view_exists(self, database_object: str, schema: str = None) -> bool:
        qualified_database_object = f"{schema}.{database_object}"
        return qualified_database_object in self.list_db_all(schemas=schema)

    def create_schema(self, schema_name, if_not_exists=True, dryrun=False) -> bool:
        """Creates a new schema of [schema_name]."""
        schema_name_exists = self.schema_exists(schema_name)
        if schema_name_exists:
            logger.debug(f"Schema already exists: {schema_name}")
        else:
            if if_not_exists:
                # This shouldn't matter as the check is done above, but keeping
                # it as a back up incase the above check fails for some reason
                create_schema_sql = sql.SQL(f"CREATE SCHEMA IF NOT EXISTS {schema_name}").format(
                    schema_name=schema_name
                )
            else:
                create_schema_sql = sql.SQL(f"CREATE SCHEMA {schema_name}").format(
                    schema_name=schema_name
                )
            logger.info(f"Creating schema: {schema_name}")
            logger.debug("Creating schema:\n{}".format(create_schema_sql.as_string(self.connection)))
            if not dryrun:
                self.execute_sql(create_schema_sql, no_result_expected=True)
            else:
                logger.info("--dryrun--")

            schema_name_exists = self.schema_exists(schema_name)

        return schema_name_exists

    def create_table_like_df(
        self,
        table_name: str,
        df: Union[pd.DataFrame, gpd.GeoDataFrame],
        schema_name: str = None,
        qualified: bool = False,
        index: bool = True,
        dtype: dict = None,
        dryrun: bool = False,
        modifyTableName: bool = False,
    ):
        """
        Creates a new, empty table, using pandas/geopandas to infer
        column types.
        table_name: str
            Name of table to be created.
        df: Union[pd.DataFrame, gpd.GeoDataFrame]
            DataFrame to create table like
        schema_name: str
            Schema to create table in
        qualified: bool
            True if table is qualified (schema.table)
        index: bool
            True to write DataFrame index as column
        dryrun: bool
            Run without performing actions
        """
        # TODO: do some type interpolation, mainly for
        #  dict->sqlalchemy.sql.sqltypes.JSON
        df = df[0:0]
        if qualified:
            schema_name, table_name = table_name.split(".")
        table_name = self.validate_pgtable_name_Length(table_name, modifyTableName=modifyTableName)
        logger.info(f"Creating table: {table_name}")
        if schema_name:
            logger.info(f"In schema: {schema_name}")

        if not isinstance(df, (pd.DataFrame, gpd.GeoDataFrame)):
            logger.error(f"Unrecognized df type: {df}")

        if not dryrun:
            # Check for GeoDataFrame must be first because GDFs are DFs as well
            if isinstance(df, gpd.GeoDataFrame):
                df.to_postgis(
                    name=table_name, schema=schema_name, con=self.engine, index=index
                )
            elif isinstance(df, pd.DataFrame):
                df.to_sql(
                    name=table_name,
                    schema=schema_name,
                    con=self.engine,
                    if_exists=FAIL,
                    index=index,
                    dtype=dtype,
                )
        table_exists = self.table_exists(table_name, schema=schema_name)
        return table_exists

    def get_sql_count(self, sql_str):
        """Get count of records returned by passed query. Query should
        not have COUNT() in it already."""
        if not isinstance(sql_str, (sql.SQL, sql.Composed)):
            sql_str = sql.SQL(sql_str)
        count_sql = sql.SQL(
            re.sub("SELECT (.*) FROM", "SELECT COUNT(*) FROM", sql_str.as_string(self.connection))
        )
        logger.debug("Count sql: {}".format(count_sql))
        self.cursor.execute(count_sql)
        count = self.cursor.fetchall()[0][0]

        return count

    def get_table_count(self, table, schema=None):
        """Get total count for the passed table."""
        # if not isinstance(table, sql.Identifier):
        #     table = sql.Identifier(table)
        if schema:
            qualified_table = f"{schema}.{table}"
        else:
            qualified_table = table
        self.cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}".format(qualified_table)))
        count = self.cursor.fetchall()[0][0]
        logger.debug("{} count: {:,}".format(qualified_table, count))

        return count

    def get_table_columns(self, table: str, schema: str) -> List[str]:
        columns_sql = sql.SQL(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = {} AND table_name = {};"
        ).format(sql.Literal(schema), sql.Literal(table))
        results = self.execute_sql(columns_sql)
        columns = [d[0] for d in results]

        return columns

    def get_table_unique_cols(self, table, schema=None):
        """Gets the pkey(s) of table"""
        if schema is None:
            schema = "public"
        unique_sql = sql.SQL(
            """
            SELECT
                a.attname
            FROM pg_class c
                INNER JOIN pg_namespace n ON n.oid = c.relnamespace
                INNER JOIN pg_attribute a ON a.attrelid = c.oid
                LEFT JOIN pg_index i
                    ON i.indrelid = c.oid
                        AND a.attnum = ANY (i.indkey[0:(i.indnkeyatts - 1)])
            WHERE
                a.attnum > 0 AND
                nspname = '{}' AND
                relname = '{}' AND
                i.indisunique is true;
            """.format(
                schema, table
            )
        )
        results = self.execute_sql(unique_sql)
        unique_cols = [record[0] for record in results]
        logger.debug("Unique columns located for {}.{}: {}".format(schema, table, unique_cols))

        return unique_cols

    def get_non_geo_columns(self, table: str, schema: str, geometry_col: str = "geometry") -> List:
        """Get the columns in the source table.

        They have to be listed explicitly in order to not select the existing
        geometry when creating new tables/views with a transformed geometry.
        """
        columns = self.get_table_columns(table=table, schema=schema)
        columns = [c for c in columns if c != geometry_col]

        return columns

    def get_table_column_details(self, table: str, schema: str) -> List[ColumnDetails]:
        columns_sql = sql.SQL(
            "SELECT column_name, data_type, is_nullable, character_maximum_length, numeric_precision "
            "FROM information_schema.columns "
            "WHERE table_schema = {} AND table_name = {};"
        ).format(sql.Literal(schema), sql.Literal(table))
        results = self.execute_sql(columns_sql)
        # Note the creation of ColumnDetails below - the results must
        # have columns in the same order as they are defined in the class.
        detailed_columns = [ColumnDetails(*r) for r in results]
        return detailed_columns

    def get_view_column_details(self, matview: str, schema: str) -> List[ColumnDetails]:
        columns_sql = sql.SQL(
            "SELECT a.attname as column_name, "
            "pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type, "
            "CASE "
            "WHEN a.attnotnull = true THEN 'YES' "
            "WHEN a.attnotnull = false THEN 'NO' "
            "END AS is_nullable, "
            "null as character_maximum_length, "
            "null as numeric_precision "
            "FROM pg_attribute a "
            "JOIN pg_class t on a.attrelid = t.oid "
            "JOIN pg_namespace s on t.relnamespace = s.oid "
            "WHERE a.attnum > 0 "
            "AND NOT a.attisdropped "
            "AND t.relname = {matview} "
            "AND s.nspname = {schema} "
            "ORDER BY a.attnum;"
        ).format(matview=sql.Literal(matview), schema=sql.Literal(schema))
        results = self.execute_sql(columns_sql)
        detailed_columns = [ColumnDetails(*r) for r in results]
        return detailed_columns

    def get_column_details(self, table: str, schema: str) -> List[ColumnDetails]:
        """Get column information for any object passed.

        Can pass table, view, or matview as "table".
        """
        object_type = self.get_pg_relkind(relname=table, schema=schema)
        if object_type == "r":
            column_details = self.get_table_column_details(table=table, schema=schema)
        elif object_type in ["v", "m"]:
            column_details = self.get_view_column_details(matview=table, schema=schema)
        else:
            msg = (
                f"Unsupported object type ('{object_type}') for getting columns. ({schema}.{table})"
            )
            logger.error(msg)
            raise Exception(msg)
        return column_details

    def get_values(self, table, columns, schema=None, distinct=False, where=None):
        """Get values in the passed columns(s) in the passed table.

        If distinct, unique values returned (across all columns passed).
        """
        if isinstance(columns, str):
            columns = [columns]

        sql_statement = generate_sql(
            layer=table, schema=schema, columns=columns, distinct=distinct, where=where
        )
        values = self.execute_sql(sql_statement)

        # Convert from list of tuples to flat list if only one column
        if len(columns) == 1:
            values = [a[0] for a in values]

        return values

    def get_geom_col_type_srid(self, table: str, schema: str) -> Tuple[str, str]:
        # Get source table geometry column name, srid, and type
        where = f"f_table_name = '{table}' AND " f"f_table_schema = '{schema}'"
        values = self.get_values(
            table="geometry_columns",
            schema="public",
            columns=["f_geometry_column", "srid", "type"],
            where=where,
        )
        geometry_col = values[0][0]
        srid = values[0][1]
        geom_type = values[0][2]
        return geometry_col, geom_type, srid

    def sql2gdf(self, sql_str, geom_col="geometry", crs: Union[str, int] = None) -> gpd.GeoDataFrame:
        """Get a GeoDataFrame from a passed SQL query"""
        if crs is None:
            # Old default, behavior, not ideal
            logger.warning("No CRS passed, defaulting to EPSG:4326")
            crs = 4326
        if isinstance(sql_str, sql.Composed):
            sql_str = sql_str.as_string(self.connection)
        gdf = gpd.GeoDataFrame.from_postgis(
            sql=sql_str, con=self.engine.connect(), geom_col=geom_col, crs=crs
        )
        return gdf

    def sql2df(self, sql_str, columns=None, **kwargs) -> pd.DataFrame:
        """Get a DataFrame from a passed SQL query"""
        if isinstance(sql_str, sql.Composed):
            sql_str = sql_str.as_string(self.connection)
        if isinstance(columns, str):
            columns = [columns]

        df = pd.read_sql(sql=sql_str, con=self.engine.connect(), columns=columns, **kwargs)

        return df

    def table2df(self, table: str, schema: str = None, where: str = None, gdf: bool = False):
        sql_str = sql.SQL("SELECT * FROM {schema}.{table}").format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
        )
        if where:
            sql_str += sql.SQL(" WHERE {where}").format(where=where)

        if gdf:
            geom_col, _geom_type, srid = self.get_geom_col_type_srid(table=table, schema=schema)
            df = self.sql2gdf(sql_str=sql_str, geom_col=geom_col, crs=srid)
        else:
            df = self.sql2df(sql_str=sql_str)

        return df

    def prep_db_for_upload(
        self,
        df: Union[pd.DataFrame, gpd.GeoDataFrame],
        table: str,
        schema: str = None,
        index: bool = False,
        dtype: dict = None,
        dryrun: bool = False,
    ):
        """
        Checks if schema and table exist in database, creates them if not.

        Args:
            df: Union[pd.DataFrame, gpd.GeoDataFrame]
                DataFrame to be added
            table: str
                Name of destination table (unqualified)
            schema: str
                Name of destination schema
            dtype: dict
                Column type mappings to sql types
            dryrun: bool
                Run without performing actions

        Returns: tuple
            indicating whether the schema and/or table exist, e.g.
            (True, False) for schema exists after prep, table does not
        """
        # Get tables and schemas to check if the destination already exists
        # Check if schema exists, create if not
        schema_exists = self.schema_exists(schema)
        if not schema_exists:
            logger.info(f'Schema "{schema}" not found, will be created.')
            se = self.create_schema(schema_name=schema, if_not_exists=True, dryrun=dryrun)
        else:
            logger.debug(f'Existing schema "{schema}" located.')
            se = True

        # Check if table exists in schema, create if not
        qualified_table = f"{schema}.{table}"
        table_exists = self.table_exists(table=qualified_table, qualified=True)
        if table_exists:
            logger.debug(f'Existing table "{qualified_table}" located.')
            te = True
            # Report count of table (if it exists (not a dryrun))
            starting_table_count = self.get_table_count(table=table, schema=schema)
            logger.debug(f"{schema}.{table} starting count: " f"{starting_table_count}")
        else:
            logger.debug(f"Table {qualified_table} not found.")
            te = False
            # te = self.create_table_like_df(table_name=qualified_table,
            #                                df=df,
            #                                qualified=True,
            #                                index=index,
            #                                dtype=dtype,
            #                                dryrun=dryrun)

        return se, te

    def df2postgres(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = None,
        if_exists: str = "fail",
        index: bool = False,
        con: Union[sqlalchemy.engine.Engine, sqlalchemy.engine.Connection] = None,
        dtype: dict = None,
        handle_json: bool = True,
        dryrun: bool = False,
        modifyTableName: bool = False,
    ):
        table = self.validate_pgtable_name_Length(table, modifyTableName=modifyTableName)
        logger.info(f"Inserting {len(df):,} records into {table}...")
        if schema:
            logger.info(f"In schema: {schema}")
        if not con:
            con = self.engine
        te, se = self.prep_db_for_upload(
            df=df, table=table, schema=schema, dtype=dtype, dryrun=dryrun
        )
        # Convert to json if requested
        if handle_json:
            dict_cols = [col for col in df.columns if isinstance(df[col][0], dict)]
            for dc in dict_cols:
                df[dc] = df[dc].apply(lambda x: json.dumps(x))
        if not dryrun:
            df.to_sql(
                name=table, schema=schema, con=con, if_exists=if_exists, index=index, dtype=dtype
            )
            logger.debug(f"{schema}.{table} ending count: " f"{self.get_table_count(table, schema)}")
        else:
            logger.debug("--dryrun--")

    def gdf2postgis(
        self,
        gdf: gpd.GeoDataFrame,
        table,
        schema=None,
        con=None,
        unique_on=None,
        if_exists="fail",
        index=True,
        drop_z: bool = False,
        chunksize=None,
        pool_size=None,
        max_overflow=None,
        dryrun=False,
        modifyTableName: bool = False,
    ):
        """
        TODO: write docstrings
        """
        # Verify args
        if_exists_opts = {"fail", "replace", "append"}
        if if_exists not in if_exists_opts:
            logger.error(
                f'Invalid options for "if_exists": "{if_exists}".\n'
                f"Must be one of: {if_exists_opts}"
            )
        table = self.validate_pgtable_name_Length(table, modifyTableName=modifyTableName)
        se, te = self.prep_db_for_upload(df=gdf, table=table, schema=schema, dryrun=dryrun)

        # Get starting count if table exists
        if te:
            logger.debug(f'Table "{table}" exists. INSERT method provided: {if_exists}')
            # Get starting count
            logger.info(
                f"Starting count for {schema}.{table}: "
                f"{self.get_table_count(table, schema=schema):,}"
            )

        # Remove existing values
        if unique_on is not None and te:
            logger.info("Removing duplicates based on: {}...".format(unique_on))
            existing_values = self.get_values(table=table, schema=schema, columns=unique_on)
            gdf = gdf[~gdf[unique_on].isin(existing_values)]

        if drop_z:
            if any(gdf.has_z):
                logger.info("Dropping Z dimension.")
                gdf = drop_z_dim(gdf)
            else:
                logger.info("No Z dimension found.")

        # INSERT
        if not dryrun:
            logger.info(f"Inserting {len(gdf):,} records into {table}...")
            if not con:
                if pool_size:
                    con = self.get_engine_pools(
                        pool_size=pool_size,
                        max_overflow=max_overflow,
                        connect_args=self.connect_args
                        )
                else:
                    con = self.engine

            gdf.to_postgis(
                name=table,
                con=con,
                schema=schema,
                if_exists=if_exists,
                index=index,
                chunksize=chunksize,
            )
            if te:
                logger.info(
                    f"Ending count for {schema}.{table}: "
                    f"{self.get_table_count(table, schema=schema):,}"
                )
        else:
            logger.info("--dryrun--\n")

    def _make_insert(self, insert_statement, values, row):
        with self.cursor as cursor:
            try:
                cursor.execute(self.cursor.mogrify(insert_statement, values))
                self.connection.commit()
                success = True
            except Exception as e:
                success = False
                if e == psycopg2.errors.UniqueViolation:
                    logger.warning(
                        "Skipping record due to unique violation " "for: " "{}".format(row)
                    )
                    logger.warning(e)
                    self.connection.rollback()
                elif e == psycopg2.errors.IntegrityError:
                    logger.warning(
                        "Skipping record due to integrity error " "for: " "{}".format(row)
                    )
                    logger.warning(e)
                    self.connection.rollback()
                else:
                    logger.debug(
                        "Error on statement: {}".format(
                            f"{str(self.cursor.mogrify(insert_statement, values))}"
                        )
                    )
                    logger.error(e)
                    self.connection.rollback()
                    raise e

        return success

    def insert_new_records(
        self, records, table, schema=None, unique_on=None, sde_objectid=None, sde=False, dryrun=False
    ):
        """
        Add records to table, converting data types as necessary for INSERT.
        Optionally using a unique_id (or combination of columns) to skip
        duplicates.
        Parameters
        ----------
        records : pd.DataFrame / gpd.GeoDataFrame
            DataFrame containing rows to be inserted to table
        table : str
            Name of table to be inserted into
        unique_on: str, list
            Name of column, or list of names of columns that are
            present in both records and the destination table, to
            use to remove duplicates prior to attempting the
            INSERT
        sde_objectid: str
            Use to add an ID field using sde.next_rowid() with
            the name provided here.
        sde: bool
            True to signal that the destination table is an SDE
            database, and that sde functions (currently only
            sde.st_geometry()) should be used, rather than PostGIS
            functions (i.e. ST_GeomFromText())
            TODO: convert this to be db_type (or similar) that
                takes any database type from a list (sde,
                postgis, etc) -> prefereable autodetect type of
                database and choose fxns appropriately
        dryrun: bool
            True to create insert statements but not actually
            perform the INSERT

        Returns
        --------
        None:
            TODO: Return records with added bool column if inserted or not
                (or just bool series)
        # TODO: Create overwrite records option that removes any scenes in the
            input from the DB before writing them
        # TODO: autodetect unique constraint from column
        """

        def _remove_dups_from_insert(records, table, unique_on=None):
            starting_count = len(records)
            # Remove duplicates/existing records based on single column, this
            # is done on the database side by selecting any records from the
            # destination table that have an ID in common with the rows to be
            # added.
            if len(unique_on) == 1 or isinstance(unique_on, str):
                if len(unique_on) == 1:
                    unique_on = unique_on[0]
                records_ids = list(records[unique_on])
                get_existing_sql = (
                    f"SELECT {unique_on} FROM {table} "
                    f"WHERE {unique_on} IN ({str(records_ids)[1:-1]})"
                )
                logger.debug(get_existing_sql)
                already_in_table = self.sql2df(get_existing_sql)
                logger.info("Duplicate records found: {:,}".format(len(already_in_table)))
                if len(already_in_table) != 0:
                    logger.info("Removing duplicates...")
                    # Remove duplicates
                    records = records[~records[unique_on].isin(already_in_table[unique_on])]

            else:
                # Remove duplicate values from rows to insert based on multiple
                # columns.
                # TODO: Rewrite this to be done on the database side, maybe:
                #  "WHERE column1 IN records[column1] AND
                #  column2 IN records[column2] AND..
                logger.info("Removing any existing records from search results...")
                existing_ids = self.get_values(table=table, columns=unique_on, distinct=True)
                logger.info(
                    'Existing unique records in table "{}": ' "{:,}".format(table, len(existing_ids))
                )
                # Remove dups
                starting_count = len(records)
                records = records[
                    ~records.apply(lambda x: _row_columns_unique(x, unique_on, existing_ids), axis=1)
                ]
            if len(records) != starting_count:
                logger.info("Duplicates removed: {:,}".format(starting_count - len(records)))
            else:
                logger.info("No duplicates found.")

            return records

        def _row_columns_unique(row, unique_on, values):
            """Determines if row has combination of columns in unique_on that
            are in values.
            Parameters
            ----------
            row : pd.Series
                Table row to be inserted
            unique_on : list / tuple
                Column names that when combined indicate a unique row
            values : list / tuple
                Values to check row against
            Returns
            -------
            bool
            """
            if isinstance(unique_on, str):
                unique_on = [unique_on]
            row_values = [row[c] for c in unique_on]
            if len(row_values) > 1:
                row_values = tuple(row_values)
            else:
                row_values = row_values[0]

            return row_values in values

        def _create_geom_statement(geom_cols, srid, sde=False):
            geom_statements = [sql.SQL(", ")]
            # TODO: Clean this up, explicity add pre+post statement commas, etc.
            for i, gc in enumerate(geom_cols):
                if i != len(geom_cols) - 1:
                    if sde:
                        geom_statements.append(
                            sql.SQL(" sde.st_geometry({gc}, {srid}),").format(
                                gc=sql.Placeholder(gc), srid=sql.Literal(srid)
                            )
                        )
                    else:
                        geom_statements.append(
                            sql.SQL(" ST_GeomFromText({gc}, {srid}),").format(
                                gc=sql.Placeholder(gc), srid=sql.Literal(srid)
                            )
                        )
                else:
                    if sde:
                        geom_statements.append(
                            sql.SQL(" sde.st_geometry({gc}, {srid})").format(
                                gc=sql.Placeholder(gc), srid=sql.Literal(srid)
                            )
                        )
                    else:
                        geom_statements.append(
                            sql.SQL(" ST_GeomFromText({gc}, {srid})").format(
                                gc=sql.Placeholder(gc), srid=sql.Literal(srid)
                            )
                        )
            geom_statement = sql.Composed(geom_statements)

            return geom_statement

        # Get type of records (pd.DataFrame or gpd.GeoDataFrame
        records_type = type(records)

        # Check that records is not empty
        if len(records) == 0:
            logger.warning("No records to be added.")
            return

        # Check if table exists
        if schema is not None:
            check_table = f"{schema}.{table}"
        else:
            check_table = table
        if check_table not in self.list_db_all():
            logger.warning(
                f'Table "{check_table}" not found in database "{self.database}" '
                "If a fully qualified table name was provided "
                "(i.e. db.schema.table_name) this message may be "
                "displayed in error. Support for checking for "
                "presence of tables using fully qualified names "
                "under development."
            )
            logger.debug(
                f'Table "{table}" not found in database "{self.database}", ' f"it will be created."
            )
            # Get table starting count
            logger.debug(
                "Starting count for {}: "
                "{:,}".format(table, self.get_table_count(table, schema=schema))
            )
        else:
            logger.info(f"Inserting records into {table}...")
        # Get unique IDs to remove duplicates if provided
        if unique_on is not None:
            records = _remove_dups_from_insert(records=records, table=table, unique_on=unique_on)

        logger.info("Records to add: {:,}".format(len(records)))
        if len(records) == 0:
            logger.info("No new records, skipping INSERT.")
            # TODO: make this return at the end
            return records_type().reindex_like(records), records_type().reindex_like(records)

        geom_cols = get_geometry_cols(records)
        if geom_cols:
            logger.debug("Geometry columns found: {}".format(geom_cols))
            # Get epsg code
            # TODO: check match with table
            srid = records.crs.to_epsg()
        else:
            geom_cols = []

        # Insert new records
        if dryrun:
            logger.info("--dryrun--")
        if len(records) != 0:
            logger.info(
                "Writing new records to {}.{}: " "{:,}".format(self.database, table, len(records))
            )
            successful_rows = []
            failed_rows = []
            for i, row in tqdm(
                records.iterrows(),
                desc="Adding new records to: {}".format(table),
                total=len(records),
            ):
                # Format the INSERT statement
                columns = [
                    sql.Identifier(c) for c in row.index if c not in geom_cols and c != sde_objectid
                ]
                # TODO: why are the geometry columns added separately?
                if geom_cols:
                    for gc in geom_cols:
                        columns.append(sql.Identifier(gc))
                if sde_objectid:
                    columns.append(sql.Identifier(sde_objectid))
                # Create INSERT statement, parenthesis left open intentionally
                # to accommodate adding geometry statements, e.g.:
                # "ST_GeomFromText(..)"
                # paranthesis, closed in else block if no geometry columns
                insert_statement = sql.SQL(
                    "INSERT INTO {schema}.{table} ({columns}) VALUES ({values}"
                ).format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table),
                    columns=sql.SQL(", ").join(columns),
                    values=sql.SQL(", ").join(
                        [
                            sql.Placeholder(f)
                            for f in row.index
                            if f not in geom_cols and f != sde_objectid
                        ]
                    ),  # TODO: generate list of 'standard cols' once
                )
                if geom_cols:
                    geom_statement = _create_geom_statement(geom_cols=geom_cols, srid=srid, sde=sde)
                    insert_statement = insert_statement + geom_statement
                if sde_objectid:
                    objectid_statement = sql.SQL(", sde.next_rowid({owner}, {table})").format(
                        owner=sql.Literal(
                            "sde"
                        ),  # TODO: add as arg to function or get automatically
                        table=sql.Literal(table),
                    )
                    insert_statement = insert_statement + objectid_statement
                # else:
                # Close paranthesis that was left open for geometries + objectids
                insert_statement = sql.SQL("{statement})").format(statement=insert_statement)

                values = {f: row[f] if f not in geom_cols else row[f].wkt for f in row.index}

                if dryrun:
                    if i == 0:
                        logger.debug("--dryrun--")
                        logger.debug(
                            f"Sample INSERT statement: "
                            f"{insert_statement.as_string(self.connection)}"
                        )
                        logger.debug(f"Sample values: {values}")
                    continue
                # Make the INSERT
                success = self._make_insert(
                    insert_statement=insert_statement, values=values, row=row
                )
                if success:
                    successful_rows.append(row)
                else:
                    failed_rows.append(row)

            successful_df = records_type(successful_rows)
            failed_df = records_type(failed_rows)
        else:
            logger.info("No new records to be written.")
            successful_df = records_type().reindex_like(records)
            failed_df = records_type().reindex_like(records)

        if not dryrun:
            logger.info(
                "New count for {}.{}: "
                "{:,}".format(self.database, table, self.get_table_count(table, schema=schema))
            )
        else:
            logger.info("--dryrun--")

        return successful_df, failed_df

    def get_geometry_type(self, table: str, schema: str = None):
        select_geom_cols_sql = (
            f"SELECT type FROM public.geometry_columns "
            f"WHERE f_table_schema = '{schema}' "
            f"AND f_table_name = '{table}'"
        )
        results = self.execute_sql(select_geom_cols_sql)
        if len(results) == 0:
            logger.warning(
                f"Geometry column for {schema}.{table} not found in public.geometry_columns - likely "
                "table does not have geometry."
            )
            geometry_type = None
        else:
            geometry_type = results[0][0]
        return geometry_type

    # def get_shapely_geom_type(self, table: str, schema: str):
    # TODO: look-up table for POSTGIS to shapely geom type
    #     get_one_sql = f"SELECT * FROM '{schema}.{table} LIMIT 1"
    #     gdf = self.sql2gdf(get_one_sql)
    #     geom_type = gdf.geometry.geom_type[0]

    def get_invalid(
        self, table: str, schema: str = None, geometry: str = "geometry", id_field: str = None
    ):
        """"""
        if id_field is None:
            id_field = "*"
        if schema is not None:
            table = f"{schema}.{table}"
        invalid_sql = f"SELECT {id_field} FROM {table} " f"WHERE ST_IsValid({geometry}) = false"
        results = self.execute_sql(sql_query=invalid_sql)
        return results

    def make_valid(
        self,
        table: str,
        schema: str = None,
        geometry: str = "geometry",
        multi_geom: bool = False,
        id_field: str = None,
        ids: list = None,
    ):
        geom_lut = {
            "POINT": 1,
            "LINESTRING": 2,
            "POLYGON": 3,
            "MULTIPOINT": 1,
            "MULTILINESTRING": 2,
            "MULTIPOLYGON": 3,
        }

        geometry_type = self.get_geometry_type(table=table, schema=schema)
        if multi_geom:
            make_valid_sql = (
                f"UPDATE {schema}.{table} "
                f"SET {geometry} = ST_MULTI(ST_CollectionExtract("
                f"ST_MakeValid({geometry}), {geom_lut[geometry_type]}))"
            )
        else:
            make_valid_sql = (
                f"UPDATE {schema}.{table} "
                f"SET {geometry} = "
                f"ST_MakeValid({geometry}), {geom_lut[geometry_type]})"
            )
        if ids:
            make_valid_sql += f" WHERE {id_field} IN ({str(ids)[1:-1]})"
        logger.info("Performing ST_MakeValid...")
        logger.debug(f"SQL: {make_valid_sql}")
        self.execute_sql(sql_query=make_valid_sql, no_result_expected=True)

    def get_table_owner(self, table: str, schema: str):
        owner_where = f"tablename = '{table}' AND schemaname = '{schema}'"
        owner = self.get_values(
            table="pg_tables", schema="pg_catalog", columns="tableowner", where=owner_where
        )
        return owner

    def alter_table_owner(self, table: str, schema: str, new_owner: str, table_type: str = "TABLE"):
        # Validate inputs
        valid_table_types = ["table", "view", "materialized view"]
        if table_type.lower() not in valid_table_types:
            logger.error(
                f"Invalid table type specified for RENAME: {table_type}, "
                f"Must be one of: {valid_table_types}."
            )

        alter_table_sql = f"ALTER {table_type} {schema}.{table} " f"OWNER TO {new_owner}"
        logger.info(f"Updating {schema}.{table} owner to: {new_owner}")
        self.execute_sql(sql_query=alter_table_sql, no_result_expected=True)

    def alter_schema_owner(self, schema: str, new_owner: str):
        alter_schema_sql = f"ALTER SCHEMA {schema} " f"OWNER TO {new_owner}"
        logger.info(f"Updating {schema} owner to: {new_owner}")
        self.execute_sql(alter_schema_sql, no_result_expected=True)

    def refresh_materialized_view(self, matview: str, schema: str):
        logger.info(f"Refreshing materialized view: {schema}.{matview}")
        refresh_statement = sql.SQL("REFRESH MATERIALIZED VIEW " "{schema}.{matview}").format(
            schema=sql.Identifier(schema), matview=sql.Identifier(matview)
        )
        logger.debug(refresh_statement.as_string(self.connection))
        self.execute_sql(refresh_statement, no_result_expected=True)

    def drop_table(
        self,
        table: str,
        schema: str,
        table_type: str = "TABLE",
        if_exists: bool = True,
        cascade: bool = False,
    ):
        logger.debug(f"Dropping {table_type}: {schema}.{table}")
        drop_statement = f"DROP {table_type}"
        if if_exists:
            drop_statement += " IF EXISTS"

        drop_statement += " {schema}.{table}"
        if cascade:
            drop_statement += " CASCADE"
        drop_statement = sql.SQL(drop_statement).format(
            schema=sql.Identifier(schema), table=sql.Identifier(table)
        )
        self.execute_sql(drop_statement, no_result_expected=True)

    def drop_table_or_view(self, database_object: str, schema: str, **kwargs):
        """Determines the type of object passed, then drops it."""
        relkind = self.get_pg_relkind(relname=database_object, schema=schema)
        self.drop_table(
            table=database_object,
            schema=schema,
            table_type=self._relkind_to_table_type[relkind],
            **kwargs,
        )

    def rename_table(
        self,
        existing_table: str,
        new_table: str,
        schema: str,
        modifyTableName: bool = False,
        table_type: str = "TABLE",
    ):
        valid_table_types = ["table", "view", "materialized view"]
        if table_type.lower() not in valid_table_types:
            logger.error(
                f"Invalid table type specified for RENAME: {table_type}, "
                f"Must be one of: {valid_table_types}."
            )
        new_table = self.validate_pgtable_name_Length(new_table, modifyTableName=modifyTableName)
        logger.debug(f"Renaming table: {schema}.{existing_table}")
        rename_statement = sql.SQL(
            f"ALTER {table_type} {schema}.{existing_table} " f"RENAME TO {new_table}"
        ).format(
            table_type=sql.Literal(table_type),
            schema=sql.Identifier(schema),
            existing_table=sql.Identifier(existing_table),
            new_table=sql.Identifier(new_table),
        )
        logger.debug(rename_statement.as_string(self.connection))
        self.execute_sql(rename_statement, no_result_expected=True)

    def rename_table_or_view(self, existing_object: str, new_object: str, schema: str, **kwargs):
        relkind = self.get_pg_relkind(relname=existing_object, schema=schema)
        self.rename_table(
            existing_table=existing_object,
            new_table=new_object,
            schema=schema,
            table_type=self._relkind_to_table_type[relkind],
            **kwargs,
        )

    def hotswap_table(
        self,
        active_table: str,
        temp_table: str,
        schema: str,
        max_count_diff=0,
        old_table_suffix: str = "outdated",
        drop_old: bool = False,
    ):
        """Replaces one table with another.

        After validating that both tables exist, drops the "active_table"
        and then renames the "temp_table" to have the name that the
        activate table previously had.
        """
        logger.debug(f"Hotswapping tables: {temp_table}->{active_table}")
        # TODO: ideally some validation happens before the drop
        #       - [x] counts are within expected difference range
        #       - [ ] geometries are valid
        #       - etc.
        #       -> move all validation to new _validate_hotswap() method
        if max_count_diff is not None:
            counts_ok = self._compare_counts(
                table1=active_table, table2=temp_table, schema1=schema, max_diff=max_count_diff
            )
            if counts_ok is False:
                logger.warning("Count validation failed, aborting hotswap.")
                return -1

        # Rename active to dated table name
        outdated_table = f"{active_table}_{old_table_suffix}"
        if self.table_or_view_exists(database_object=outdated_table, schema=schema):
            self.drop_table_or_view(database_object=outdated_table, schema=schema, cascade=True)
        self.rename_table(existing_table=active_table, new_table=outdated_table, schema=schema)

        # Rename temp table to active table
        self.rename_table(existing_table=temp_table, new_table=active_table, schema=schema)

        # Drop old table if requested
        if drop_old:
            self.drop_table_or_view(database_object=outdated_table, schema=schema)
        # TODO: Ideally more validation, rollback if needed
        return 1

    def _compare_counts(
        self, table1: str, table2: str, schema1: str, schema2: str = None, max_diff=0
    ):
        if schema2 is None:
            schema2 = schema1
        logger.debug(f"Comparing counts for {schema1}.{table1} and {schema2}.{table2}")
        t1_count = self.get_table_count(table=table1, schema=schema1)
        t2_count = self.get_table_count(table=table2, schema=schema2)
        counts_ok = abs(t1_count - t2_count) <= max_diff
        logger.debug(f"Counts OK: {counts_ok}")
        if counts_ok is False:
            logger.warning(
                f"Counts are not OK:\n"
                f"{schema1}.{table1}: {t1_count}\n"
                f"{schema2}.{table2}: {t2_count}\n"
                f"{t1_count - t2_count} <= {max_diff} = False"
            )
        return counts_ok

    def validate_pgtable_name_Length(self, table_name: str, modifyTableName: bool = False):
        """Preform check on table name length to ensure that it falls
        within PostgreSQL limits of 63 characters.
        modifyName will try to replace "-" and "_" to get name length
        within limit."""
        tn = table_name
        replacementChars = ["_", "-"]

        wInLimit = True if len(tn) <= 63 else False
        if not wInLimit and modifyTableName:
            for ct in replacementChars:
                tn = tn.replace(ct, "")
                wInLimit = True if len(tn) <= 63 else False
                if wInLimit:
                    break

        if not wInLimit:
            raise ValueError(
                f"""
                Table name \n\t{table_name}\nexceeds PostgreSQL limit
                of 63 and modifyName set to false. Failing
            """
            )

        return tn

    def validate_geometry(self, table: str, schema: str, geometry_field="geometry"):
        validate_sql = (
            f"UPDATE {schema}.{table} "
            f"SET {geometry_field} = ST_MakeValid({geometry_field}) "
            f"WHERE ST_IsValid({geometry_field}) = 'f' ; "
        )
        logger.info(f"Validating geometry for {schema}.{table}")
        self.execute_sql(sql_query=validate_sql, no_result_expected=True)

    def get_geometry_srid(self, table: str, schema: str, geometry_field="geometry"):
        select_geom_cols_sql = (
            f"SELECT srid FROM public.geometry_columns "
            f"WHERE f_table_schema = '{schema}' "
            f"AND f_table_name = '{table}';"
        )
        results = self.execute_sql(select_geom_cols_sql)
        if len(results) == 0 or results[0][0] == 0:
            # Method 2
            select_srid_sql = sql.SQL(
                "SELECT DISTINCT ST_SRID({geometry_field}) " "FROM {schema}.{table};"
            ).format(
                geometry_field=sql.Identifier(geometry_field),
                schema=sql.Identifier(schema),
                table=sql.Identifier(table),
            )
            results = self.execute_sql(select_srid_sql)
            if len(results) == 0:
                logger.warning(
                    f"Geometry column {geometry_field} for {schema}.{table} not found "
                    f"in public.geometry_columns. Attempting to SELECT ST_SRID"
                )
            else:
                srid = results[0][0]
        else:
            srid = results[0][0]
        return srid

    def create_simplified_matviews(
        self,
        table: str,
        source_schema: str,
        simplify_tolerances: List[float],
        dest_schema: str = None,
        dryrun: bool = False,
    ) -> List[str]:
        """
        Creates materialized views that are simplified versions of the source table. For each
        tolerance provided, one materialized view will be created. Tolerances are in units of
        [table].
        """
        if dest_schema is None:
            dest_schema = source_schema

        # Ensure table to work on exists
        te = self.table_exists(table=table, schema=source_schema, qualified=False)
        if te is False:
            logger.error(f"Source table does not exist: {source_schema}.{table}")
            raise Exception

        # Get columns
        source_cols = self.get_table_columns(table=table, schema=source_schema)
        geometry_col, _geom_type, srid = self.get_geom_col_type_srid(
            table=table, schema=source_schema
        )
        non_geo_cols = [sql.Identifier(col) for col in source_cols if col != geometry_col]

        # Log CRS info - warning if not projected
        try:
            crs = CRS(f"EPSG:{srid}")
            logger.info(
                f"Simplify tolerance is in units of source table: " f"{crs.axis_info[0].unit_name}"
            )
            if not crs.is_projected:
                logger.warning(
                    "Source table is not projected. Simplify tolerances will be in " "degrees."
                )
        except CRSError as e:
            logger.error(f"Error determining CRS for source table: {source_schema}.{table}: {e}")

        simplified_matviews = []
        for simp_tol in simplify_tolerances:
            if simp_tol.is_integer():
                simp_tol = round(simp_tol)
            logger.info(f"Simplifying {source_schema}.{table} - Tolerance: {simp_tol}")
            # Create name of new matview
            simplified_matview_name = f"{table}_simplify_{simp_tol}".replace(".", "x")
            # Determine if matview exists
            ve = self.view_exists(view=simplified_matview_name, schema=dest_schema, qualified=False)
            if ve is True:
                logger.warning(
                    f"View exists with destination name, skipping: "
                    f"{dest_schema}.{simplified_matview_name} ({self.host})"
                )
                simplified_matviews.append(f"{dest_schema}.{simplified_matview_name}")
                continue
            # Create SQL to simplify
            simplify_sql = sql.SQL(
                "CREATE MATERIALIZED VIEW {dest_schema}.{simplified_matview} AS "
                "SELECT {non_geo_cols}, "
                "ST_SetSRID(ST_SimplifyPreserveTopology({geometry_col}, {simp_tol}), {srid}) as geometry "
                "FROM {source_schema}.{source_table};"
            ).format(
                dest_schema=sql.Identifier(dest_schema),
                simplified_matview=sql.Identifier(simplified_matview_name),
                non_geo_cols=sql.SQL(",").join(non_geo_cols),
                geometry_col=sql.Identifier(geometry_col),
                simp_tol=sql.Literal(simp_tol),
                srid=sql.Literal(srid),
                source_schema=sql.Identifier(source_schema),
                source_table=sql.Identifier(table),
            )
            logger.debug(simplify_sql.as_string(self.connection))
            if dryrun is False:
                # Execute SQL
                self.execute_sql(simplify_sql, no_result_expected=True)
            simplified_matviews.append(f"{dest_schema}.{simplified_matview_name}")
        matviews_log = "\n".join(simplified_matviews)
        logger.info(
            f"Created {len(simplified_matviews)} simplified materialized views:\n" f"{matviews_log}"
        )
        return simplified_matviews

    def get_view_definition(self, view: str, schema: str) -> str:
        """Retrieve the definition of the passed view as text."""
        view_def_sql = sql.SQL("SELECT pg_get_viewdef('{schema}.{view}')").format(
            schema=sql.SQL(schema), view=sql.SQL(view)
        )
        result = self.execute_sql(view_def_sql)
        if len(result) != 1:
            msg = (
                f"Error retrieving view definition. Expected exactly one result, got: {len(result)}"
            )
            logger.error(msg)
            raise Exception(msg)
        return result[0][0]


# TODO:
#  Create SQLQuery class
#   - .select .where .fields .join etc.
#  convert string variables in SQL querys (pgcatalog, etc. to constants)
#  Make PostGIS subclass
#   move:
#     - make_valid
#     - get_invalid
#     - get_geometry_type
