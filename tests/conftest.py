import logging
import os
import sys

import dotenv
import pytest

from dbtools.pg import Postgres, load_pgconfig


dotenv.load_dotenv('.env.test')

@pytest.fixture(name="pgconfig")
def fixture_pgconfig():
    return load_pgconfig()


@pytest.fixture()
def db_src(pgconfig) -> Postgres:
    with Postgres(**pgconfig.non_wildcard_atts) as db:
        yield db
