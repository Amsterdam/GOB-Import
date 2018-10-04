import os

DATABASE_CONFIGS = {
    'Grondslag': {
        'drivername': 'oracle+cx_oracle',
        'username': os.getenv("DBIMBP01_DATABASE_USER", "gob"),
        'password': os.getenv("DBIMBP01_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("DBIMBP01_DATABASE_HOST", "hostname"),
        'port': 1521,
        'database': 'DBIMBP01'
    }
}
