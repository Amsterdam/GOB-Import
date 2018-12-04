import os
import re

from sqlalchemy.engine.url import URL

ORACLE_DRIVER = 'oracle+cx_oracle'

DATABASE_CONFIGS = {
    'Grondslag': {
        'drivername': ORACLE_DRIVER,
        'username': os.getenv("DBIMBP01_DATABASE_USER", "gob"),
        'password': os.getenv("DBIMBP01_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("DBIMBP01_DATABASE_HOST", "hostname"),
        'port': os.getenv("DBIMBP01_DATABASE_PORT", 1521),
        'database': 'DBIMBP01'
    },
    'DGDialog': {
        'drivername': ORACLE_DRIVER,
        'username': os.getenv("BINBGT01_DATABASE_USER", "gob"),
        'password': os.getenv("BINBGT01_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("BINBGT01_DATABASE_HOST", "hostname"),
        'port': os.getenv("BINBGT01_DATABASE_PORT", 1521),
        'database': 'binbgt11.amsterdam.nl'
    },
    'DIVA': {
        'drivername': ORACLE_DRIVER,
        'username': os.getenv("DBIGMA01_DATABASE_USER", "gob"),
        'password': os.getenv("DBIGMA01_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("DBIGMA01_DATABASE_HOST", "hostname"),
        'port': os.getenv("DBIGMA01_DATABASE_PORT", 1521),
        'database': 'DBIGMA01'
    }
}

OBJECTSTORE_CONFIGS = {
    'Basisinformatie': {
        "VERSION": '2.0',
        "AUTHURL": 'https://identity.stack.cloudvps.com/v2.0',
        "TENANT_NAME": os.getenv("BASISINFORMATIE_OBJECTSTORE_TENANT_NAME"),
        "TENANT_ID": os.getenv("BASISINFORMATIE_OBJECTSTORE_TENANT_ID"),
        "USER": os.getenv("BASISINFORMATIE_OBJECTSTORE_USER"),
        "PASSWORD": os.getenv("BASISINFORMATIE_OBJECTSTORE_PASSWORD"),
        "REGION_NAME": 'NL'
    }
}


def get_url(db_config):
    """Get URL connection

    Get the URL for the given database config

    :param db_config: e.g. DATABASE_CONFIGS['DIVA']
    :return: url
    """
    # Default behaviour is to return the sqlalchemy url result
    url = URL(**db_config)
    if db_config["drivername"] == ORACLE_DRIVER:
        # The Oracle driver can accept a service name instead of a SID
        service_name_pattern = re.compile("^\w+\.\w+\.\w+$")
        if service_name_pattern.match(db_config["database"]):
            # Replace the SID by the service name
            url = str(url).replace(db_config["database"], '?service_name=' + db_config['database'])
    return url
