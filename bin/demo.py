from dbtools.db import Postgres


# Connection information
HOST = 'sandwich'
# host = 'sandwich.pool.pgc.umn.edu'
DATABASE = 'dgarchive'
TABLE = 'master_fp'

with Postgres(host=HOST, database=DATABASE) as db_src:
    # Make connection, for demo only, all other methods will do
    # the connecting automatically
    connection = db_src.connection

    # Example of querying table
    sql_str = "SELECT * FROM {} LIMIT 10".format(TABLE)
    results = db_src.execute_sql(sql_query=sql_str)

    # List tables in database, example of creating cursor,
    # defining SQL clause, executing, and getting results
    tables = db_src.list_db_tables()

    # Get SQL clause as GeoDataFrame
    gdf = db_src.sql2gdf(sql_str=sql_str,
                         geom_col='wkb_geometry')

    db_src.insert_new_records(records=gdf,
                              table='master_fp')


