from pathlib import Path
from typing import Generator

import pytest
from alembic.command import upgrade as alembic_upgrade
from alembic.config import Config as AlembicConfig
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from gobconfig.datastore.config import DATASTORE_CONFIGS


@pytest.fixture
def tests_dir() -> Path:
    """Returns directory which contains tests. Used to find files required for tests."""
    return Path(__file__).parent


@pytest.fixture
def bag_db_config() -> dict[str, str]:
    db_config = DATASTORE_CONFIGS["BAGExtract"]
    return {
        "username": db_config["username"],
        "password": db_config["password"],
        "host": db_config["host"],
        "port": db_config["port"],
        "database": db_config["database"],
        "drivername": "postgresql"
    }


@pytest.fixture
def recreate_database(bag_db_config) -> str:
    """Drop test database and recreate it to ensure the database is empty.

    :param bag_db_config: database config to connect to local copy of bag.
    :returns: de test database name it created.
    """
    test_db_name = f"{bag_db_config['database']}"
    tmp_config = bag_db_config.copy()
    # Cannot drop the currently open database, so do not open it.
    tmp_config.pop("database")
    engine_tmp: Engine = create_engine(URL(**tmp_config), echo=True)
    try:
        with engine_tmp.connect() as conn:
            conn.execute("commit")
            conn.execute(f"DROP DATABASE IF EXISTS {test_db_name}")
            conn.execute("commit")
            conn.execute(f"CREATE DATABASE {test_db_name}")
            conn.execute("commit")
    except OperationalError as e:
        raise Exception(
            "Could not connect to test bag database. "
            "Is the gobimport_database container running?"
        ) from e

    return test_db_name


@pytest.fixture
def database(tests_dir: Path, recreate_database, bag_db_config) -> Generator[Session, None, None]:
    """Fixture which sets up the database, returns a db session.

    :param tests_dir: path to current tests source.
    :param recreate_database: fixture which resets the test database.
    :param bag_db_config: database config to connect to local copy of bag.
    :return: a generator which yields a db session.
    """
    test_db_name = recreate_database
    bag_db_config["database"] = test_db_name
    engine: Engine = create_engine(URL(**bag_db_config), echo=True)
    session_factory = sessionmaker(bind=engine)
    session: Session = session_factory()
    # Migrate the database
    alembic_config = AlembicConfig(str(tests_dir / "alembic.ini"))
    alembic_config.set_main_option("script_location", str(tests_dir / "alembic"))
    alembic_upgrade(alembic_config, "head")
    # Worden de migraties wel echt gedraaid? Ik zie geen 'adding'
    try:
        yield session
    finally:
        engine.dispose()
