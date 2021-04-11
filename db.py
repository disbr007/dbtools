import json
import re
from pathlib import Path, PurePath
import sys
from typing import Union, Tuple, List, AnyStr

import geopandas as gpd
import pandas as pd
import psycopg2
from psycopg2 import sql
import shapely
from shapely.geometry import Point
from sqlalchemy import create_engine
from tqdm import tqdm

from .logging_utils import create_logger
# TODO: turn back to info
logger = create_logger(__name__, 'sh', 'INFO')

# Supress pandas SettingWithCopyWarning
pd.set_option('mode.chained_assignment', None)

CONFIG_FILE = Path(__file__).parent / "config.json"
if not CONFIG_FILE.exists():
    logger.error('config.json not found. Should be created at: '
                 '{}'.format(CONFIG_FILE))
    sys.exit(-1)

# Config keys
HOSTS = 'hosts'
DATABASES = 'databases'
GLOBAL_USER = 'global_user'
GLOBAL_PASSWORD = 'global_password'
HOST = 'host'
USER = 'user'
PASSWORD = 'password'
DATABASE = 'database'

# Constants
DEF_SKIP_SCHEMAS = ['information_schema',
                    'pg_catalog']


def get_db_config(host_name, db_name, config_file=CONFIG_FILE):
    # TODO: validate config entries
    if not isinstance(config_file, PurePath):
        config_file = Path(config_file)
    if not config_file.exists():
        logger.error('Config file not found at: {}'.format(config_file))
        logger.error('Please create a config.json file based on the example.')
        raise FileNotFoundError

    try:
        config = json.load(open(config_file))
    except json.decoder.JSONDecodeError as e:
        logger.error('Error loading config file: {}'.format(config_file))
        logger.error(e)
        sys.exit(-1)

    # Locate host
    host_short_names = config[HOSTS].keys()
    host_full_names = [k[1][HOST] for k in config[HOSTS].items()]
    if host_name in host_short_names:
        db_config = config[HOSTS][host_name]
    elif host_name in host_full_names:
        for short_name, params in config[HOSTS].items():
            if host_name == params[HOST]:
                db_config = params
                break
    else:
        logger.error('Config for host "{}" not found.'.format(host_name))
        raise KeyError
    # Confirm database is listed in host's config
    databases = db_config.pop(DATABASES)
    if db_name in databases:
        db_config[DATABASE] = db_name
    else:
        logger.error('Database "{}" not listed in {} config.'.format(db_name,
                                                                     host_name))
        raise ConnectionError

    # Add username and password
    if USER not in db_config.keys():
        db_config[USER] = config[GLOBAL_USER]
    if PASSWORD not in db_config.keys():
        db_config[PASSWORD] = config[GLOBAL_PASSWORD]

    return db_config


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
    shapely_geoms = (shapely.geometry.collection.GeometryCollection,
                     shapely.geometry.linestring.LineString,
                     shapely.geometry.polygon.LinearRing,
                     shapely.geometry.multilinestring.MultiLineString,
                     shapely.geometry.multipoint.MultiPoint,
                     shapely.geometry.multipolygon.MultiPolygon,
                     shapely.geometry.point.Point,
                     shapely.geometry.polygon.Polygon)
    geom_cols = []
    for col in gdf.columns:
        if type(gdf[col].values[0]) in shapely_geoms:
            geom_cols.append(col)

    return geom_cols


def check_where(where, join='AND'):
    if where:
        where += """ {} """.format(join)
    else:
        where = ""

    return where


def ids2sql(ids):
    return str(ids)[1:-1]


def encode_geom_sql(geom_col, encode_geom_col):
    """
    SQL statement to encode geometry column in non-PostGIS Postgres
    database for reading by geopandas.read_postgis.
    """
    geom_sql = "encode(ST_AsBinary({}), 'hex') AS " \
               "{}".format(geom_col, encode_geom_col)

    return geom_sql


def make_identifier(sql_str):
    if (sql_str is not None and
            not isinstance(sql_str, sql.Identifier)):
        return sql.Identifier(sql_str)


def generate_sql(layer, columns=None, where=None, orderby=False, schema=None,
                 orderby_asc=False, distinct=False, limit=False, offset=None,
                 geom_col=None, encode_geom_col_as='geometry', remove_id_tbl=None,
                 remove_id_tbl_cols=None, remove_id_src_cols=None):
    """
    geom_col not needed for PostGIS if loading SQL with geopandas -
        gpd can interpet the geometry column without encoding
    """
    if distinct:
        sql_select = 'SELECT DISTINCT'
    else:
        sql_select = "SELECT"
    if isinstance(columns, str):
        columns = [columns]
    if columns is not None:
        fields = sql.SQL(',').join([sql.Identifier(f) for f in columns])
    else:
        fields = sql.SQL('*')

    # Only necessary for geometries in non-PostGIS DBs
    if geom_col:
        # Create base query object, with geometry encoding
        geom_encode_str = encode_geom_sql(geom_col=geom_col, encode_geom_col=encode_geom_col_as)
        # geom_encode_str = "encode(ST_AsBinary({}), 'hex') AS {}".format(geom_col, encode_geom_col_as)
        query = sql.SQL("{select} {fields}, {geom_encode_str} FROM {table}").format(
            select=sql.SQL(sql_select),
            fields=fields,
            geom_encode_str=sql.SQL(geom_encode_str),
            table=sql.Identifier(layer))
    else:
        # Create base query object
        query = sql.SQL("{select} {fields} FROM {schema}.{table}").format(
            select=sql.SQL(sql_select),
            fields=fields,
            schema=sql.Identifier(schema),
            table=sql.Identifier(layer))

    # Add any provided additional parameters
    # Drop records that have ID in other table
    if all([remove_id_tbl, remove_id_tbl_cols, remove_id_src_cols]):
        if isinstance(remove_id_src_cols, str):
            remove_id_src_cols = [remove_id_src_cols]
        if isinstance(remove_id_tbl_cols, str):
            remove_id_tbl_cols = [remove_id_tbl_cols]
        if not len(remove_id_tbl_cols) == len(remove_id_src_cols):
            logger.error('Error creating LEFT JOIN clause: '
                         'length of remove_id_tbl_cols ({}) != '
                         'length of remove_ids_src_cols ({})'.format(len(remove_id_tbl_cols),
                                                                     len(remove_id_src_cols)))
            raise Exception
        jss = []
        for i, col in enumerate(remove_id_tbl_cols):
            js = "{0}.{1} = {2}.{3}".format(remove_id_tbl,
                                            col,
                                            layer,
                                            remove_id_src_cols[i])
            jss.append(js)

        join_stmts = " AND ".join(jss)
        join_stmts = " LEFT JOIN {} ON {}".format(remove_id_tbl,
                                                  join_stmts)
        # join_stmt = "LEFT JOIN {0} " \
        #             "ON {0}.{1} = {2}.{3}".format(remove_id_tbl,
        #                                           remove_id_tbl_cols,
        #                                           layer,
        #                                           remove_id_src_cols)

        join_where = "{}.{} IS NULL".format(remove_id_tbl,
                                            remove_id_tbl_cols[0])
        query += sql.SQL(join_stmts)
        if where is not None:
            where = '{} AND {}'.format(join_where, where)
        else:
            where = join_stmts
    if where:
        sql_where = " WHERE {}".format(where)
        query += sql.SQL(sql_where)
    if orderby:
        if orderby_asc:
            asc = 'ASC'
        else:
            asc = 'DESC'
        sql_orderby = sql.SQL("ORDER BY {field} {asc}").format(
            field=sql.Identifier(orderby),
            asc=sql.Literal(asc))
        query += sql_orderby
    if limit:
        sql_limit = sql.SQL("LIMIT {}".format(limit))
        query += sql_limit
    if offset:
        sql_offset = sql.SQL("OFFSET {}".format(offset))
        query += sql_offset

    logger.debug('Generated SQL: {}'.format(query))

    return query


def add2sql(sql_str):
    pass


def intersect_aoi_where(aoi, geom_col):
    """
    Create a where statement for a PostGIS intersection between the
    geometry(s) in the aoi geodataframe and a PostGIS table with
    geometry in geom_col.
    """
    aoi_epsg = aoi.crs.to_epsg()
    aoi_wkts = [geom.wkt for geom in aoi.geometry]
    intersect_wheres = ["ST_Intersects({}, ST_SetSRID('{}'::geometry, " \
                        "{}))".format(geom_col, wkt, aoi_epsg,)
                        for wkt in aoi_wkts]
    aoi_where = " OR ".join(intersect_wheres)

    return aoi_where


class Postgres(object):
    """
    Class for interacting with Postgres database using psycopg2. This
    allows keeping a connection and cursor open while performing multiple
    operations. Best used with a context manager, i.e.:
    with Postgres(db_name) as db:
        ...
    """
    _instance = None

    def __init__(self, host, database):
        self.db_config = get_db_config(host, database)
        self.host = host
        self.database = database
        self._connection = None
        self._cursor = None

    @property
    def connection(self):
        """Establish connection to database."""
        if self._connection is None:
            try:
                self._connection = psycopg2.connect(**self.db_config)

            except (psycopg2.Error, psycopg2.OperationalError) as error:
                Postgres._instance = None
                logger.error('Error connecting to {database} at '
                             '{host}'.format(**self.db_config))
                logger.error(error)
                sys.exit(-1)
            else:
                logger.debug('Connection to {database} at {host} '
                             'established.'.format(**self.db_config))

        return self._connection

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.connection.closed:
            if not self.cursor.closed:
                self.cursor.close()
            self.connection.close()

    def __del__(self):
        if not self.connection.closed:
            if not self.cursor.closed:
                self.cursor.close()
            self.connection.close()

    @property
    def cursor(self):
        if self._cursor is None:
            self._cursor = self.connection.cursor()
        if self._cursor.closed:
            self._cursor = self.connection.cursor()

        return self._cursor

    def get_engine(self):
        """Create sqlalchemy.engine object."""
        engine = create_engine('postgresql+psycopg2://'
                               '{user}:{password}@{host}/{database}'.format(**self.db_config))

        return engine

    def get_engine_pools(self, pool_size=5, max_overflow=10):
        """Create sqlalchemy.engine object."""
        engine = create_engine('postgresql+psycopg2://'
                               '{user}:{password}@{host}/{database}'.format(**self.db_config),
                               pool_size=pool_size,
                               max_overflow=max_overflow)

        return engine

    def list_schemas(self, include_infoschemas=False):
        """List all schemas in the database"""
        logger.debug('Listing schemas...')
        schemas_sql = sql.SQL("""SELECT schemaname from pg_catalog.pg_tables""")
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
        logger.debug('Schemas: {}'.format(schemas))
        return schemas

    def list_tables(self, schemas=None, skip_schemas=DEF_SKIP_SCHEMAS):
        """List all tables in the database."""
        logger.debug('Listing tables...')
        tables_sql = sql.SQL("SELECT schemaname, tablename "
                             "FROM pg_catalog.pg_tables")
        if schemas:
            if not isinstance(schemas, list):
                schemas = [schemas]
            schemas_str = str(schemas)[1:-1]
            tables_sql = tables_sql + sql.SQL(f" WHERE schemaname IN ({schemas_str})")
        self.cursor.execute(tables_sql)
        schemas_tables = self.cursor.fetchall()

        qualified_tables = ['{}.{}'.format(s, t) for s, t in schemas_tables
                            if s not in skip_schemas]
        logger.debug('Tables: {}'.format(qualified_tables))

        return qualified_tables

    def list_views(self, skip_schemas=DEF_SKIP_SCHEMAS):
        logger.debug('Listing views...')
        views_sql = sql.SQL("""SELECT schemaname, viewname
                               FROM pg_catalog.pg_views""")
        self.cursor.execute(views_sql)
        schemas_views = self.cursor.fetchall()
        qualified_views = ['{}.{}'.format(s, t) for s, t in schemas_views
                           if s not in skip_schemas]
        logger.debug('Views: {}'.format(qualified_views))

        return qualified_views

    def list_matviews(self, skip_schemas=DEF_SKIP_SCHEMAS):
        logger.debug('Listing Materialized Views...')
        matviews_sql = sql.SQL("""SELECT schemaname, matviewname
                                  FROM pg_catalog.pg_matviews""")
        self.cursor.execute(matviews_sql)
        schemas_matviews = self.cursor.fetchall()
        qualified_matviews = ['{}.{}'.format(s, t) for s, t in schemas_matviews
                              if s not in skip_schemas]
        logger.debug('Materialized Views: {}'.format(qualified_matviews))

        return qualified_matviews

    def list_db_all(self):
        tables = self.list_tables()
        views = self.list_views()
        matviews = self.list_matviews()

        all_layers = tables + views + matviews

        return all_layers

    def execute_sql(self, sql_query, commit=True):
        """Execute the passed query on the database."""
        if not isinstance(sql_query, (sql.SQL, sql.Composable, sql.Composed)):
            sql_query = sql.SQL(sql_query)
        # logger.debug('SQL query: {}'.format(sql_query))
        self.cursor.execute(sql_query)
        try:
            results = self.cursor.fetchall()
        except psycopg2.ProgrammingError as e:
            if 'no results to fetch' in e.args:
                # TODO: Do this without an exception catch
                results = None
            else:
                logger.error(e)
        if commit:
            self.connection.commit()

        return results

    def create_schema(self, schema_name, if_not_exists=True, dryrun=False):

        if if_not_exists:
            create_schema_sql = (sql.SQL(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                                 .format(schema_name=schema_name))
        else:
            create_schema_sql = (sql.SQL(f"CREATE SCHEMA {schema_name}")
                                 .format(schema_name=schema_name))
        logger.debug('Creating schema:\n{}'
                     .format(create_schema_sql.as_string(self.connection)))
        if not dryrun:
            results = self.execute_sql(create_schema_sql)
        else:
            logger.debug('--dryrun--')
            results = None

        return results

    def get_sql_count(self, sql_str):
        """Get count of records returned by passed query. Query should
        not have COUNT() in it already."""
        if not isinstance(sql_str, (sql.SQL, sql.Composed)):
            sql_str = sql.SQL(sql_str)
        count_sql = sql.SQL(re.sub('SELECT (.*) FROM',
                                   'SELECT COUNT(*) FROM',
                                   sql_str.as_string(self.connection)))
        logger.debug('Count sql: {}'.format(count_sql))
        self.cursor.execute(count_sql)
        count = self.cursor.fetchall()[0][0]

        return count

    def get_table_count(self, table, schema=None):
        """Get total count for the passed table."""
        # if not isinstance(table, sql.Identifier):
        #     table = sql.Identifier(table)
        if schema:
            qualified_table = f'{schema}.{table}'
        else:
            qualified_table = table
        self.cursor.execute(sql.SQL(
            "SELECT COUNT(*) FROM {}".format(qualified_table)))
        count = self.cursor.fetchall()[0][0]
        logger.debug('{} count: {:,}'.format(qualified_table, count))

        return count

    def get_table_columns(self, table):
        """Get columns in passed table."""
        self.cursor.execute(
            sql.SQL(
            "SELECT * FROM {} LIMIT 0").format(sql.Identifier(table)))
        columns = [d[0] for d in self.cursor.description]

        return columns

    def get_values(self, table, columns, schema=None, distinct=False, where=None):
        """Get values in the passed columns(s) in the passed table. If
        distinct, unique values returned (across all columns passed)"""
        if isinstance(columns, str):
            columns = [columns]

        sql_statement = generate_sql(layer=table, schema=schema, columns=columns,
                                     distinct=distinct, where=where)
        values = self.execute_sql(sql_statement)

        # Convert from list of tuples to flat list if only one column
        if len(columns) == 1:
            values = [a[0] for a in values]

        return values

    def sql2gdf(self, sql_str, geom_col='geometry', crs=4326,):
        """Get a GeoDataFrame from a passed SQL query"""
        if isinstance(sql_str, sql.Composed):
            sql_str = sql_str.as_string(self.connection)
        gdf = gpd.GeoDataFrame.from_postgis(sql=sql_str,
                                            con=self.get_engine().connect(),
                                            geom_col=geom_col, crs=crs)
        return gdf

    def sql2df(self, sql_str, columns=None, **kwargs):
        """Get a DataFrame from a passed SQL query"""
        if isinstance(sql_str, sql.Composed):
            sql_str = sql_str.as_string(self.connection)
        if isinstance(columns, str):
            columns = [columns]

        df = pd.read_sql(sql=sql_str, con=self.get_engine().connect(),
                         columns=columns, **kwargs)

        return df

    def gdf2postgis(self, gdf: gpd.GeoDataFrame, table, schema=None, con=None,
                    unique_on=None, if_exists='append', index=True,
                    chunksize=None,
                    pool_size=None, max_overflow=None,
                    dryrun=False):
        """
        TODO: write docstrings
        """
        # Verify args
        # if_exists_opts = {'fail', 'replace', 'append'}
        # if if_exists not in if_exists_opts:
        #     logger.error(f'Invalid options for "if_exists": "{if_exists}".\n'
        #                  f'Must be one of: {if_exists_opts}')
        # # Check if table exists
        # table_exists = f'{schema}.{table}' in self.list_db_all()
        # if table_exists:
        #     logger.debug(f'Table "{table}" exists. INSERT method provided: {if_exists}')
        #     # Get starting count
        #     logger.info(f'Starting count for {schema}.{table}: '
        #                 f'{self.get_table_count(table, schema=schema):,}')
        # else:
        #     logger.info(f'Table "{table}" does not exist. Will be created.')

        # if unique_on is not None and table_exists:
        #     logger.debug('Removing duplicates based on: {}...'.format(unique_on))
        #     existing_values = self.get_values(table=table, schema=schema,
        #                                       columns=unique_on)
        #     gdf = gdf[~gdf[unique_on].isin(existing_values)]

        # Write
        if not dryrun:
            logger.info(f'Inserting {len(gdf):,} records into {table}...')
            if not con:
                if pool_size:
                    con = self.get_engine_pools(pool_size=pool_size, max_overflow=max_overflow)
                else:
                    con = self.get_engine()

            gdf.to_postgis(name=table, con=con, schema=schema,
                           if_exists=if_exists, index=index, chunksize=chunksize)
            # logger.info(f'Ending count for {schema}.{table}: '
            #             f'{self.get_table_count(table, schema=schema):,}')
        else:
            logger.info('--dryrun--\n')

    def _make_insert(self, insert_statement, values, row):
        with self.cursor as cursor:
            try:
                cursor.execute(self.cursor.mogrify(insert_statement,
                                                   values))
                self.connection.commit()
                success = True
            except Exception as e:
                success = False
                if e == psycopg2.errors.UniqueViolation:
                    logger.warning('Skipping record due to unique violation '
                                   'for: '
                                   '{}'.format(row))
                    logger.warning(e)
                    self.connection.rollback()
                elif e == psycopg2.errors.IntegrityError:
                    logger.warning('Skipping record due to integrity error '
                                   'for: '
                                   '{}'.format(row))
                    logger.warning(e)
                    self.connection.rollback()
                else:
                    logger.debug('Error on statement: {}'.format(
                        f"{str(self.cursor.mogrify(insert_statement, values))}"))
                    logger.error(e)
                    self.connection.rollback()
                    raise e

        return success

    def insert_new_records(self, records, table, schema=None,
                           unique_on=None,
                           sde_objectid=None,
                           sde=False,
                           dryrun=False):
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
                get_existing_sql = f"SELECT {unique_on} FROM {table} " \
                                   f"WHERE {unique_on} IN ({str(records_ids)[1:-1]})"
                logger.debug(get_existing_sql)
                already_in_table = self.sql2df(get_existing_sql)
                logger.info('Duplicate records found: {:,}'.format(len(already_in_table)))
                if len(already_in_table) != 0:
                    logger.info('Removing duplicates...')
                    # Remove duplicates
                    records = records[~records[unique_on].isin(already_in_table[unique_on])]

            else:
                # Remove duplicate values from rows to insert based on multiple
                # columns.
                # TODO: Rewrite this to be done on the database side, maybe:
                #  "WHERE column1 IN records[column1] AND
                #  column2 IN records[column2] AND..
                logger.info('Removing any existing records from search results...')
                existing_ids = self.get_values(table=table,
                                               columns=unique_on,
                                               distinct=True)
                logger.info('Existing unique records in table "{}": '
                             '{:,}'.format(table, len(existing_ids)))
                # Remove dups
                starting_count = len(records)
                records = records[~records.apply(lambda x: _row_columns_unique(
                    x, unique_on, existing_ids), axis=1)]
            if len(records) != starting_count:
                logger.info('Duplicates removed: {:,}'.format(starting_count -
                                                              len(records)))
            else:
                logger.info('No duplicates found.')

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
            geom_statements = [sql.SQL(', ')]
            # TODO: Clean this up, explicity add pre+post statement commas, etc.
            for i, gc in enumerate(geom_cols):
                if i != len(geom_cols) - 1:
                    if sde:
                        geom_statements.append(
                            sql.SQL(" sde.st_geometry({gc}, {srid}),").format(
                                gc=sql.Placeholder(gc),
                                srid=sql.Literal(srid)))
                    else:
                        geom_statements.append(
                            sql.SQL(" ST_GeomFromText({gc}, {srid}),").format(
                                gc=sql.Placeholder(gc),
                                srid=sql.Literal(srid)))
                else:
                    if sde:
                        geom_statements.append(
                            sql.SQL(" sde.st_geometry({gc}, {srid})").format(
                                gc=sql.Placeholder(gc),
                                srid=sql.Literal(srid)))
                    else:
                        geom_statements.append(
                            sql.SQL(" ST_GeomFromText({gc}, {srid})").format(
                                gc=sql.Placeholder(gc),
                                srid=sql.Literal(srid)))
            geom_statement = sql.Composed(geom_statements)

            return geom_statement
        # Get type of records (pd.DataFrame or gpd.GeoDataFrame
        records_type = type(records)

        # Check that records is not empty
        if len(records) == 0:
            logger.warning('No records to be added.')
            return

        # Check if table exists
        if table not in self.list_db_all():
            logger.warning('Table "{}" not found in database "{}" '
                           'If a fully qualified table name was provided '
                           '(i.e. db.schema.table_name) this message may be '
                           'displayed in error. Support for checking for '
                           'presence of tables using fully qualified names '
                           'under development.'.format(table, self.database))
            logger.info(f'Table "{table}" not found in database "{self.database}", '
                        f'it will be created.')
        else:
            logger.info('Inserting records into {}...'.format(table))
            # Get table starting count
            logger.info('Starting count for {}: '
                        '{:,}'.format(table, self.get_table_count(table)))

        # Get unique IDs to remove duplicates if provided
        if unique_on is not None:
            records = _remove_dups_from_insert(records=records,
                                               table=table,
                                               unique_on=unique_on)

        logger.info('Records to add: {:,}'.format(len(records)))
        if len(records) == 0:
            logger.info('No new records, skipping INSERT.')
            # TODO: make this return at the end
            return records_type().reindex_like(records), records_type().reindex_like(records)

        geom_cols = get_geometry_cols(records)
        if geom_cols:
            logger.debug('Geometry columns found: {}'.format(geom_cols))
            # Get epsg code
            # TODO: check match with table
            srid = records.crs.to_epsg()
        else:
            geom_cols = []

        # Insert new records
        if dryrun:
            logger.info('--dryrun--')
        if len(records) != 0:
            logger.info('Writing new records to {}.{}: '
                        '{:,}'.format(self.database, table, len(records)))
            successful_rows = []
            failed_rows = []
            for i, row in tqdm(records.iterrows(),
                               desc='Adding new records to: {}'.format(table),
                               total=len(records)):
                # Format the INSERT statement
                columns = [sql.Identifier(c) for c in row.index
                           if c not in geom_cols and c != sde_objectid]
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
                    "INSERT INTO {schema}.{table} ({columns}) VALUES ({values}").format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table),
                    columns=sql.SQL(', ').join(columns),
                    values=sql.SQL(', ').join([sql.Placeholder(f)
                                               for f in row.index
                                               if f not in geom_cols
                                               and f != sde_objectid]), # TODO: generate list of 'standard cols' once
                )
                if geom_cols:
                    geom_statement = _create_geom_statement(geom_cols=geom_cols,
                                                            srid=srid,
                                                            sde=sde)
                    insert_statement = insert_statement + geom_statement
                if sde_objectid:
                    objectid_statement = sql.SQL(", sde.next_rowid({owner}, {table})").format(
                        owner=sql.Literal('sde'), # TODO: add as arg to function or get automatically
                        table=sql.Literal(table))
                    insert_statement = insert_statement + objectid_statement
                # else:
                # Close paranthesis that was left open for geometries + objectids
                insert_statement = sql.SQL("{statement})").format(
                    statement=insert_statement)

                values = {f: row[f] if f not in geom_cols
                          else row[f].wkt for f in row.index}

                if dryrun:
                    if i == 0:
                        logger.debug('--dryrun--')
                        logger.debug(
                            f'Sample INSERT statement: '
                            f'{insert_statement.as_string(self.connection)}')
                        logger.debug(f'Sample values: {values}')
                    continue
                # Make the INSERT
                success = self._make_insert(insert_statement=insert_statement,
                                            values=values,
                                            row=row)
                if success:
                    successful_rows.append(row)
                else:
                    failed_rows.append(row)

            successful_df = records_type(successful_rows)
            failed_df = records_type(failed_rows)
        else:
            logger.info('No new records to be written.')
            successful_df = records_type().reindex_like(records)
            failed_df = records_type().reindex_like(records)

        if not dryrun:
            logger.info('New count for {}.{}: '
                        '{:,}'.format(self.database, table,
                                      self.get_table_count(table)))
        else:
            logger.info('--dryrun--')

        return successful_df, failed_df

# TODO: Create SQLQuery class
#   - .select .where .fields .join etc.
#  convert string variables in SQL querys (pgcatalog, etc. to constants)
