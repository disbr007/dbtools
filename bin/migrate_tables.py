import logging
from datetime import datetime
from pathlib import Path
from typing import List

import typer

from dbtools.constants import LOGS_DIR
from dbtools.migrate_tables import migrate_tables

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

run_starttime = datetime.now().strftime("%Y%b%d_%H%M%S_%f").lower()
logfile = Path(LOGS_DIR) / f"{Path(__file__).stem}_{run_starttime}.log"
fh = logging.FileHandler(logfile)
fh.setFormatter(formatter)
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)


def main(
    username: str = typer.Argument(
        None, help=("Database user to perform actions as. " "Password read from environment.")
    ),
    source_host: List[str] = typer.Option(..., help="Source host to migrate from."),
    source_database: List[str] = typer.Option(..., help="Source database to migrate from"),
    source_schema: List[str] = typer.Option(..., help="Source schema(s) to migrate from."),
    source_tables: List[str] = typer.Option(
        ...,
        help="Source table(s) to migrate. Multiple can be provided using repeated flags or via text"
        "file with one table name per line.",
    ),
    dest_host: List[str] = typer.Option(..., help="Source host to migrate to."),
    dest_database: List[str] = typer.Option(
        [],
        help=(
            "Source database to migrate to. If not provided, will assume database of same name as "
            "source on dest_host."
        ),
    ),
    dest_schema: List[str] = typer.Option(
        [],
        help=(
            "Destination schemas to migrate to. If not provided, will assume schema of same name "
            "as source on dest_host."
        ),
    ),
    dest_tables: List[str] = typer.Option(
        [], help="Destination tables to migrate to. If not provided, will use source_table."
    ),
    dryrun: bool = typer.Option(False, help="Print commands but do not run."),
):
    migrate_tables(
        username=username,
        source_host=source_host,
        source_database=source_database,
        source_schema=source_schema,
        source_tables=source_tables,
        dest_host=dest_host,
        dest_database=dest_database,
        dest_schema=dest_schema,
        dest_tables=dest_tables,
        dryrun=dryrun,
    )


if __name__ == "__main__":
    typer.run(main)
