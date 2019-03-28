import os
import re

from sqlalchemy.engine.url import URL

ORACLE_DRIVER = 'oracle+cx_oracle'
POSTGRES_DRIVER = 'postgresql'

DATABASE_CONFIGS = {
    'Grondslag': {
        'drivername': ORACLE_DRIVER,
        'username': os.getenv("DBIMB_DATABASE_USER", "gob"),
        'password': os.getenv("DBIMB_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("DBIMB_DATABASE_HOST", "hostname"),
        'port': os.getenv("DBIMB_DATABASE_PORT", 1521),
        'database': os.getenv("DBIMB_DATABASE", "")
    },
    'DGDialog': {
        'drivername': ORACLE_DRIVER,
        'username': os.getenv("BINBG_DATABASE_USER", "gob"),
        'password': os.getenv("BINBG_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("BINBG_DATABASE_HOST", "hostname"),
        'port': os.getenv("BINBG_DATABASE_PORT", 1521),
        'database': os.getenv("BINBG_DATABASE", "")
    },
    'DIVA': {
        'drivername': ORACLE_DRIVER,
        'username': os.getenv("DBIGM_DATABASE_USER", "gob"),
        'password': os.getenv("DBIGM_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("DBIGM_DATABASE_HOST", "hostname"),
        'port': os.getenv("DBIGM_DATABASE_PORT", 1521),
        'database': os.getenv("DBIGM_DATABASE", ""),
    },
    'Neuron': {
        'drivername': ORACLE_DRIVER,
        'username': os.getenv("BINNRN_DATABASE_USER", "gob"),
        'password': os.getenv("BINNRN_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("BINNRN_DATABASE_HOST", "hostname"),
        'port': os.getenv("BINNRN_DATABASE_PORT", 1521),
        'database': os.getenv("BINNRN_DATABASE", ""),
    },
    'Decos': {
        'drivername': ORACLE_DRIVER,
        'username': os.getenv("DBIDC_DATABASE_USER", "gob"),
        'password': os.getenv("DBIDC_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("DBIDC_DATABASE_HOST", "hostname"),
        'port': os.getenv("DBIDC_DATABASE_PORT", 1521),
        'database': os.getenv("DBIDC_DATABASE", ""),
    },
    'GOBPrepare': {
        'drivername': POSTGRES_DRIVER,
        'username': os.getenv("GOB_PREPARE_DATABASE_USER", "gob"),
        'password': os.getenv("GOB_PREPARE_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("GOB_PREPARE_DATABASE_HOST", "hostname"),
        'port': os.getenv("GOB_PREPARE_DATABASE_PORT", 5408),
        'database': os.getenv("GOB_PREPARE_DATABASE", ""),
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
