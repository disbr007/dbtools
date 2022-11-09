import logging
from typing import List

from pyproj import CRS
from pyproj.exceptions import CRSError
from psycopg2 import sql
import typer
from typer import Argument, Option

from dbtools.pg import Postgres

from aoetl.lib import load_aoetl_config


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


aoetl_db_config = load_aoetl_config().database


def simplify_table_app(table: str = Argument(..., help='The table to simplify.'),
                       source_schema: str = Argument(..., help='The schema [table] is in.'), 
                       simplify_tolerances: List[float] = Argument(
                           ...,
                           help='The tolerance(s) to simplify to. One materialized view will be '
                           'created for each tolerance requested. Tolerances are in units '
                           'matching the SRID of [table].'),
                       dest_schema: str = Option(
                           None,
                           help='Destination schema. If not provided, [source_schema] is used.'),
                       database: str = Option(
                           aoetl_db_config.database, 
                           help='Database that [source_schema] is in. If not provided, repo '
                           'configuration will be used.'),
                       host: str = Option(
                           aoetl_db_config.host, 
                           help='Host that [database] is on. If not provided, repo configuration '
                           'will be used.'),
                       dryrun: bool = Option(
                           False, 
                           help='Log actions, but do not perform')) -> List[str]:
    """
    Creates materialized views that are simplified versions of the source table. For each 
    tolerance provided, one materialized view will be created. Tolerances are in units of [table].
    """
    with Postgres(host=host, database=database, user=aoetl_db_config.database.user) as db_src:
        db_src.simplify_table(table=table, source_schema=source_schema, 
                              simplify_tolerances=simplify_tolerances,
                              dest_schema=dest_schema, dryrun=dryrun)


if __name__ == '__main__':
    typer.run(simplify_table_app)
