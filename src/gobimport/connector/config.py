import os

DATABASE_CONFIGS = {
    'Grondslag': {
        'drivername': 'oracle+cx_oracle',
        'username': os.getenv("DBIMBP01_DATABASE_USER", "gob"),
        'password': os.getenv("DBIMBP01_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("DBIMBP01_DATABASE_HOST", "hostname"),
        'port': os.getenv("DBIMBP01_DATABASE_PORT", 1521),
        'database': 'DBIMBP01'
    },
    'DGDialog': {
        'drivername': 'oracle+cx_oracle',
        'username': os.getenv("BINBGT01_DATABASE_USER", "gob"),
        'password': os.getenv("BINBGT01_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("BINBGT01_DATABASE_HOST", "hostname"),
        'port': os.getenv("BINBGT01_DATABASE_PORT", 1521),
        'database': 'binbgt11.amsterdam.nl'
    },
    'DIVA': {
        'drivername': 'oracle+cx_oracle',
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
