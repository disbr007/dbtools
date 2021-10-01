from dbtools.pg import Postgres

# Connection information
HOST = 'host_specified_in_config.json'
# host = 'sandwich.pool.pgc.umn.edu'
DATABASE = 'database_name'
TABLE = 'mytable'

with Postgres(host=HOST, database=DATABASE) as db_src:
    # Make connection, for demonstration only, all other 
    # methods handle the connection automatically
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

    # Insert new records into table
    db_src.insert_new_records(records=gdf,
                              table='master_fp')


