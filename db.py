import json
import re
from pathlib import Path, PurePath
import sys

import geopandas as gpd
import pandas as pd
import psycopg2
from psycopg2 import sql
import shapely
from shapely.geometry import Point
from sqlalchemy import create_engine
from tqdm import tqdm

from .logging_utils import create_logger

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
        raise
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


def generate_sql(layer, columns=None, where=None, orderby=False,
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
        query = sql.SQL("{select} {fields} FROM {table}").format(
            select=sql.SQL(sql_select),
            fields=fields,
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

    def list_db_tables(self):
        """List all tables in the database."""
        # TODO: Add support for fully qualified table/view/mat_view names
        #  i.e. db.schema.table
        logger.debug('Listing tables...')
        tables_sql = sql.SQL("""SELECT table_schema as schema_name,
                                       table_name as view_name
                                FROM information_schema.tables
                                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                                ORDER BY schema_name, view_name""")
        self.cursor.execute(tables_sql)
        tables = self.cursor.fetchall()
        logger.debug('Tables: {}'.format(tables))

        views_sql = sql.SQL("""SELECT table_schema as schema_name,
                                      table_name as view_name
                               FROM information_schema.views
                               WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                               ORDER BY schema_name, view_name""")
        self.cursor.execute(views_sql)

        # tables = self.cursor.fetchall()
        views = self.cursor.fetchall()
        tables.extend(views)
        logger.debug('Views: {}'.format(views))

        matviews_sql = sql.SQL("""SELECT schemaname as schema_name, 
                                      matviewname as view_name
                                  FROM pg_matviews""")
        self.cursor.execute(matviews_sql)
        matviews = self.cursor.fetchall()
        tables.extend(matviews)
        logger.debug("Materialized views: {}".format(matviews))

        tables = [x[1] for x in tables]
        tables = sorted(tables)

        return tables

    def execute_sql(self, sql_query):
        """Execute the passed query on the database."""
        if not isinstance(sql_query, (sql.SQL, sql.Composable, sql.Composed)):
            sql_query = sql.SQL(sql_query)
        # logger.debug('SQL query: {}'.format(sql_query))
        self.cursor.execute(sql_query)
        results = self.cursor.fetchall()

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

    def get_table_count(self, table):
        """Get total count for the passed table."""
        # if not isinstance(table, sql.Identifier):
        #     table = sql.Identifier(table)
        self.cursor.execute(sql.SQL(
            "SELECT COUNT(*) FROM {}".format(table)))
        count = self.cursor.fetchall()[0][0]
        logger.debug('{} count: {:,}'.format(table, count))

        return count

    def get_table_columns(self, table):
        """Get columns in passed table."""
        self.cursor.execute(
            sql.SQL(
            "SELECT * FROM {} LIMIT 0").format(sql.Identifier(table)))
        columns = [d[0] for d in self.cursor.description]

        return columns

    def get_values(self, table, columns, distinct=False, where=None):
        """Get values in the passed columns(s) in the passed table. If
        distinct, unique values returned (across all columns passed)"""
        if isinstance(columns, str):
            columns = [columns]

        sql_statement = generate_sql(layer=table, columns=columns,
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

    def insert_new_records(self, records, table, unique_on=None,
                           dryrun=False):
        """ # TODO: ST_ASBINARY option -- autodetect geometry type
        Add records to table, converting data types as necessary for INSERT.
        Optionally using a unique_id (or combination of columns) to skip
        duplicates.
        records : pd.DataFrame / gpd.GeoDataFrame
            DataFrame containing rows to be inserted to table
        table : str
            Name of table to be inserted into
        """
        # TODO: Create overwrite records option that removes any scenes in the
        #  input from the DB before writing them

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

        # Check that records is not empty
        if len(records) == 0:
            logger.warning('No records to be added.')
            return

        # Check if table exists, get table starting count, unique constraint
        logger.info('Inserting records into {}...'.format(table))
        if table not in self.list_db_tables():
            logger.warning('Table "{}" not found in database "{}" '
                           'If a fully qualified table name was provided '
                           '(i.e. db.schema.table_name) this message may be '
                           'displayed in error. Support for checking for '
                           'presence of tables using fully qualified names '
                           'under development.'.format(table, self.database))

        logger.info('Starting count for {}: '
                    '{:,}'.format(table, self.get_table_count(table)))

        # Get unique IDs to remove duplicates if provided
        # if table in self.list_db_tables() and unique_on is not None:
        if unique_on is not None:
            # TODO: _remove_dups_from_insert()
            starting_count = len(records)
            # Remove duplicates/existing records based on single column, this
            # is done on the database side by selecting any records from the
            # destination table that have an ID in common with the rows to be
            # added.
            if len(unique_on) == 1 or isinstance(unique_on, str):
                if len(unique_on) == 1:
                    unique_on = unique_on[0]
                records_ids = list(records[unique_on])
                get_existing_sql = f"SELECT {unique_on} FROM {table} WHERE {unique_on} IN ({str(records_ids)[1:-1]})"
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
                existing_ids = self.get_values(table=table, columns=unique_on,
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
        logger.info('Records to add: {:,}'.format(len(records)))
        if len(records) == 0:
            logger.info('No new records, skipping INSERT.')
            return

        geom_cols = get_geometry_cols(records)
        if geom_cols:
            logger.debug('Geometry columns found: {}'.format(geom_cols))
            # Get epsg code
            srid = records.crs.to_epsg()
        # else:
        #     geom_cols = []

        # Insert new records
        if dryrun:
            logger.info('--dryrun--')
        if len(records) != 0:
            logger.info('Writing new records to {}.{}: '
                        '{:,}'.format(self.database, table, len(records)))

            for i, row in tqdm(records.iterrows(),
                               desc='Adding new records to: {}'.format(table),
                               total=len(records)):
                if dryrun:
                    continue

                # Format the INSERT statement
                columns = [sql.Identifier(c) for c in row.index if c not in
                           geom_cols]
                if geom_cols:
                    for gc in geom_cols:
                        columns.append(sql.Identifier(gc))
                # Create INSERT statement, parenthesis left open intentionally
                # to accommodate adding geometry statements, e.g.:
                # "ST_GeomFromText(..)"
                # paranthesis, closed in else block if no geometry columns
                insert_statement = sql.SQL(
                    "INSERT INTO {table} ({columns}) VALUES ({values}").format(
                    table=sql.Identifier(table),
                    columns=sql.SQL(', ').join(columns),
                    values=sql.SQL(', ').join([sql.Placeholder(f)
                                               for f in row.index
                                               if f not in geom_cols]),
                )
                if geom_cols:
                    # TODO: def _create_geom_statement()
                    geom_statements = [sql.SQL(', ')]
                    for i, gc in enumerate(geom_cols):
                        if i != len(geom_cols) - 1:
                            geom_statements.append(
                                sql.SQL(
                                    " ST_GeomFromText({gc}, {srid}),").format(
                                    gc=sql.Placeholder(gc),
                                    srid=sql.Literal(srid)))
                        else:
                            geom_statements.append(
                                sql.SQL(
                                    " ST_GeomFromText({gc}, {srid}))").format(
                                    gc=sql.Placeholder(gc),
                                    srid=sql.Literal(srid)))
                    geom_statement = sql.Composed(geom_statements)
                    insert_statement = insert_statement + geom_statement
                else:
                    # Close paranthesis that was left open for geometries
                    insert_statement = sql.SQL("{statement})").format(
                        statement=insert_statement)

                values = {f: row[f] if f not in geom_cols
                          else row[f].wkt for f in row.index}

                # Make the INSERT
                # TODO: def _make_insert()
                with self.cursor as cursor:
                    try:
                        cursor.execute(self.cursor.mogrify(insert_statement,
                                                           values))
                        self.connection.commit()
                    except Exception as e:
                        if e == psycopg2.errors.UniqueViolation:
                            logger.warning('Skipping due to unique violation '
                                           'for record: '
                                           '{}'.format(row))
                            logger.warning(e)
                            self.connection.rollback()
                        elif e == psycopg2.errors.IntegrityError:
                            logger.warning('Skipping due to integrity error '
                                           'for record: '
                                           '{}'.format(row))
                            logger.warning(e)
                            self.connection.rollback()
                        else:
                            logger.debug('Error on statement: {}'.format(
                                f"{str(self.cursor.mogrify(insert_statement, values))}"))
                            logger.error(e)
                            self.connection.rollback()
        else:
            logger.info('No new records to be written.')
            
        logger.info('New count for {}.{}: '
                    '{:,}'.format(self.database, table,
                                  self.get_table_count(table)))

# TODO: Create SQLQuery class
#   - .select .where .fields .join etc.
