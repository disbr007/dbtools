import logging
import subprocess
import sys
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from subprocess import PIPE
from typing import List

from dbtools.pg import Postgres
from dbtools.constants import LOGS_DIR

import typer
from tqdm import tqdm

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


@dataclass
class TableRef:
    host: str
    database: str
    schema: str
    table: str

    def __str__(self):
        return f"{self.host}.{self.database}.{self.schema}.{self.table}"


def run_subprocess(command: str, shell: bool = True):
    logger.debug(f"Running command: {command}")
    proc = subprocess.Popen(command, stdout=PIPE, stderr=PIPE, shell=shell)
    proc_out = []
    for line in iter(proc.stdout.readline, b""):
        logger.debug("(subprocess) {}".format(line.decode()))
        proc_out.append(line.decode().strip())
    proc_err = []
    for line in iter(proc.stderr.readline, b""):
        proc_err.append(line.decode().strip())
    if proc_err:
        logger.debug("(subprocess) {}".format(proc_err))
    # output, error = proc.communicate()
    # logger.debug('Output: {}'.format(output.decode()))
    # logger.debug('Err: {}'.format(error.decode()))
    return proc_out, proc_err


def migrate_table(
    source_table: TableRef, dest_table: TableRef, username: str = None, dryrun: bool = False
):
    # Ensure destination table does not exist already (if it does there may be unexpected results
    # from the upload command, e.g. duplicate records)
    with Postgres(host=dest_table.host, database=dest_table.database, user=username) as db_src:
        dest_table_exists = db_src.table_or_view_exists(dest_table.table, schema=dest_table.schema)
        if dest_table_exists:
            logger.error(
                f"Destination table exists, skipping upload: {dest_table.schema}.{dest_table.table}"
            )
            success = False
            return success

    logger.info(f"Migrating {source_table} -> {dest_table}")
    dump_file = f"{source_table.table}.sql"
    all_commands = []
    dump_cmd = [
        "pg_dump",
        "--host",
        source_table.host,
        "--encoding=utf8",
        "--no-owner",
        f"--username={username}",
        "-t",
        f"{source_table.schema}.{source_table.table}",
        source_table.database,
        ">",
        f"{dump_file}",
    ]
    all_commands.append(dump_cmd)
    if source_table.database != dest_table.database:
        replace_db_in_file_cmd = [
            "sed",
            "-i",
            f"'s/{source_table.database}/{dest_table.database}/g'",
            f"{dump_file}",
        ]
        all_commands.append(replace_db_in_file_cmd)
    if source_table.schema != dest_table.schema:
        replace_schema_in_file_cmd = [
            "sed",
            "-i",
            f"'s/{source_table.schema}/{dest_table.schema}/g'",
            f"{dump_file}",
        ]
        all_commands.append(replace_schema_in_file_cmd)
    if source_table.table != dest_table.table:
        replace_table_in_file_cmd = [
            "sed",
            "-i",
            f"'s/{source_table.table}/{dest_table.table}/g'",
            f"{dump_file}",
        ]
        all_commands.append(replace_table_in_file_cmd)

    upload_cmd = [
        "psql",
        "--host",
        dest_table.host,
        f"--username={username}",
        dest_table.database,
        "-f",
        dump_file,
    ]
    all_commands.append(upload_cmd)

    migrate_cmd = ""
    for i, cmd in enumerate(all_commands):
        migrate_cmd += " ".join(cmd)
        if i != len(all_commands) - 1:
            migrate_cmd += " && "
    migrate_cmd += ";"

    if not dryrun:
        logger.debug(migrate_cmd)
        out, err = run_subprocess(migrate_cmd)
        logger.info(f"\nOutput:\n{out}\nErr:\n{err}")
        success = True
    else:
        logger.info(migrate_cmd)
        logger.info("--dryrun--")
        success = False

    return success


def validate_source_args(
    source_host: List[str],
    source_database: List[str],
    source_schema: List[str],
    source_tables: List[str],
):
    """Validates counts of arguments make sense."""
    num_src_host_provided = len(source_host)
    num_src_database_provided = len(source_database)
    num_src_schema_provided = len(source_schema)
    num_src_tables_provided = len(source_tables)

    valid_source_args = True
    for arg_count in [num_src_host_provided, num_src_database_provided, num_src_schema_provided]:
        if not (arg_count == 1 or arg_count == num_src_tables_provided):
            valid_source_args = False
    if valid_source_args is False:
        error_msg = (
            "Source argument error. Must provide either one argument or the same number "
            "as source_tables."
        )
        logger.error(error_msg)
        logger.error(
            f"Tables: {num_src_tables_provided}.\nHosts: {num_src_host_provided}."
            f"\nDatabases: {num_src_database_provided}.\nSchema: {num_src_schema_provided}"
        )
        raise ValueError(error_msg)
    return valid_source_args


def validate_dest_args(
    dest_host: List[str], dest_database: List[str], dest_schema: List[str], dest_tables: List[str]
):
    """Validates counts of arguments make sense."""
    num_dest_host_provided = len(dest_host)
    num_dest_database_provided = len(dest_database)
    num_dest_schema_provided = len(dest_schema)
    num_dest_tables_provided = len(dest_tables)

    valid_source_args = True
    for arg_count in [num_dest_host_provided, num_dest_database_provided, num_dest_schema_provided]:
        if not (arg_count != 0 or arg_count != 1 or arg_count != num_dest_tables_provided):
            valid_source_args = False
    if valid_source_args is False:
        error_msg = (
            "Destination argument error. Must provide either 0, 1 or the same number of "
            "arguments as source_tables."
            f"\nTables: {num_dest_tables_provided}\nHosts: {num_dest_host_provided}"
            f"\nDatabases: {num_dest_database_provided}\nSchema: {num_dest_schema_provided}"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)
    return valid_source_args


def write_logs(successful_migrations: List[TableRef], failed_migrations: List[TableRef]):
    """Writes log files about script run.

    Two logs files are written, one for successful migrations, one for unsuccessful. The logs are
    formatted with each line having the destination [host].[database].[schema].[table]
    """
    timestamp = datetime.now().strftime("%Y%b%d_%H%M%S_%f").lower()
    success_out_file = LOGS_DIR / f"migrate_tables-successful-{timestamp}.txt"
    failed_out_file = LOGS_DIR / f"migrate_tables-failed-{timestamp}.txt"
    if len(successful_migrations) > 0:
        logger.info(f"Writing successful migration references to: {success_out_file}")
        with open(success_out_file, "w") as dst:
            for sm in successful_migrations:
                dst.write(f"{sm.host}.{sm.database}.{sm.schema}.{sm.table}\n")
    if len(failed_migrations) > 0:
        logger.info(f"Writing failed migration references to: {failed_out_file}")
        with open(failed_out_file, "w") as dst:
            for fm in failed_migrations:
                dst.write(f"{fm.host}.{fm.database}.{fm.schema}.{fm.table}\n")


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
    """Migrate database tables from one host to another.

    For source arguments, there must either be an equal number of source tables and
    hosts/database/schema passed, or multiple tables can be passed with a singular source
    host/database/schema.

    For destination arguments, any database/schema/table argument not passed will be assumed to
    be the same as the source. Otherwise, argument counts must either be singular (for
    database/schema) or match the number of source arguments.
    """
    if username is None:
        logger.error("No username provided.")
        sys.exit(-1)
    if Path(source_tables[0]).exists():
        with open(source_tables[0], "r") as src:
            source_tables = []
            for line in src.readlines():
                source_tables.append(line.strip())

    # Validate source args
    validate_source_args(
        source_host=source_host,
        source_database=source_database,
        source_schema=source_schema,
        source_tables=source_tables,
    )

    # Validate dest args
    validate_dest_args(
        dest_host=dest_host,
        dest_database=dest_database,
        dest_schema=dest_schema,
        dest_tables=dest_tables,
    )
    if len(dest_database) == 0:
        dest_database = source_database
    if len(dest_schema) == 0:
        dest_schema = source_schema
    if len(dest_tables) == 0:
        dest_tables = source_tables

    # Create table references
    if len(source_host) == 1:
        source_host = [source_host[0] for _ in range(len(source_tables))]
    if len(source_database) == 1:
        source_database = [source_database[0] for _ in range(len(source_tables))]
    if len(source_schema) == 1:
        source_schema = [source_schema[0] for _ in range(len(source_tables))]
    source_references = [
        TableRef(*args) for args in zip(source_host, source_database, source_schema, source_tables)
    ]

    if len(dest_host) == 1:
        dest_host = [dest_host[0] for _ in range(len(dest_tables))]
    if len(dest_database) == 1:
        dest_database = [dest_database[0] for _ in range(len(dest_tables))]
    if len(dest_schema) == 1:
        dest_schema = [dest_schema[0] for _ in range(len(dest_tables))]
    dest_references = [
        TableRef(*args) for args in zip(dest_host, dest_database, dest_schema, dest_tables)
    ]

    successful_migrations = []
    failed_migrations = []
    for source_ref, dest_ref in tqdm(
        zip(source_references, dest_references), total=len(source_references)
    ):
        success = migrate_table(
            source_table=source_ref, dest_table=dest_ref, username=username, dryrun=dryrun
        )
        if success is True:
            successful_migrations.append(dest_ref)
        elif success is False and dryrun is not True:
            failed_migrations.append(dest_ref)

    if dryrun is not True:
        write_logs(successful_migrations, failed_migrations)


if __name__ == "__main__":
    typer.run(main)
