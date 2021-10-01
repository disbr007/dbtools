#### dbtools
Tools for interacting with databases, specifically tested for 
PostgreSQL and PostgreSQL/PostGIS databases.

### Set Up
Create a copy of the `example_config.json` file and rename it to
`config.json`. Replace the `global_user` and `global_password` 
values with your own. Additional databases can be added following 
the same format. Database specific users and passwords can be 
specified within a given host's config, for example:
```json
{
  "hosts":
  {
    "danco": {
      "host": "danco.pgc.umn.edu",
      "databases": [
        "footprint",
        "imagery"
      ]
    },
    "sandwich": {
      "host": "sandwich-pool.pgc.umn.edu",
      "databases": [
        "planet",
        "dem",
        "dgarchive"
      ],
      "user": "user",
      "password": "password",
      "sslmode": "require"
    }
  },
  "global_user": "disbr007",
  "global_password": "******"
}
```

### Usage
Basic usage:
```python
with Postgres([host_name], [database_name]) as db_src:
    sql_str = "SELECT * FROM [table]"
    results = db_src.execute_sql(sql_str)
```

Results can also be converted directly to a pandas DataFrame 
or geopandas GeoDataFrame (assuming PostGIS database for this example):
```python
with Postgres([host_name], [database_name]) as db_src:
    sql_str = "SELECT * FROM [table]"
    df = db_src.sql2df(sql_str)
    gdf = db_src.sql2gdf(sql_str)
```

For non-PostGIS spatial databases, the geometry must be encoded in 
order to be converted to a GeoDataFrame:
```python
with Postgres([host_name], [database_name]) as db_src:
    sql_str = generate_sql([table], 
                            geom_col=[table_geometry_column], 
                            encode_geom_col_as='geometry')
    gdf = db_src.sql2gdf(sql_str)
```

<!--- TODO
-use .ini connection files, e.g. https://www.postgresql.org/docs/9.1/libpq-pgservice.html
-create setup.py to allow for installation elsewhere
---> 
