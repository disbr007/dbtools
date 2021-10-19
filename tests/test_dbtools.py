import json
import unittest
import pytest_check as check

from dbtools.pg import Postgres

HOST = 'aoassetdb'
DATABASE = 'ao_dev'


class TestPostgres(unittest.TestCase):
    def test_connection(self):
        with Postgres(HOST, DATABASE) as db_src:
            cxn = db_src.connection
            check.is_false(cxn.closed)

    def test_enginge(self):
        with Postgres(HOST, DATABASE) as db_src:
            engine = db_src.get_engine()
            began = engine.begin()
            cxn = began.conn
            check.is_false(cxn.closed)

    def test_list_schemas(self):
        with Postgres(HOST, DATABASE) as db_src:
            schemas = db_src.list_schemas()
            check.is_in('public', schemas)

    def test_list_tables(self):
        with Postgres(HOST, DATABASE) as db_src:
            tables = db_src.list_tables()
            check.is_not_none(tables)
            check.is_true(len(tables > 0))
