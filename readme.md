#### db_utils
Tools for interacting with databases, specifically tested for PostgreSQL 
and PostgreSQL/PostGIS databases.

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
*Coming soon...*