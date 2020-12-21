import pytest
import testing.postgresql
from sqlalchemy import create_engine

from .data.model import metadata


def clean_tables(conn):
    with conn.begin():
        for table in metadata.sorted_tables:
            conn.execute(table.delete())
        sql = (
            "SELECT sequencename FROM pg_sequences "
            "WHERE schemaname IN (SELECT current_schema())"
        )
        for [sequence] in conn.execute(sql):
            conn.execute(f"ALTER SEQUENCE {sequence} RESTART WITH 1")


@pytest.fixture(scope="session")
def engine():
    with testing.postgresql.Postgresql(port=8421) as postgresql:
        yield create_engine(postgresql.url())


@pytest.fixture
def conn(engine):
    metadata.create_all(engine)
    yield engine.connect()
    clean_tables(engine.connect())
