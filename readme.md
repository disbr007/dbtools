# dbtools
Tools for interacting with databases, specifically tested for 
PostgreSQL and PostgreSQL/PostGIS databases.

### Set Up
Postgres configurations are read from standard locations: `~/.pgpass` and
`PG*` environmental variables, with environmental variables overriding any
values specfied in `~/.pgpass`. To specify defaults, a `.env` file can be
created at the root of the project and any values specified there will 
take precedence. For example:
```
PGHOST: localhost
PGPORT: 5432
PGDATABASE: my_database
PGUSER: me
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

# Release process
Currently the release process is relatively manual. 
1. Identify most recent tag:  
`git tag`
2. Update `aoetl.__init__` with the new version.
3. Push changes to `main`
4. Create new tag with appropriate bump in patch, minor, or major number, matching `dbtools.__init__`:  
`git tag v0.1.2`
5. Push tag to GitHub:  
`git push origin v0.1.2`
6. GitHub Actions will create a release from any push of a version tag: [release.yml](.github/workflows/release.yml)


_TODO: read version from `__init__.py` automatically_
<!--- TODO
---> 
