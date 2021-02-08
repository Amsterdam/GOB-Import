import os

CONTAINER_BASE = os.getenv("CONTAINER_BASE", "acceptatie")
DATABASE_CONFIG = {
    'drivername': 'postgres',
    'username': os.getenv("GOB_IMPORT_DATABASE_USER", "gob_import"),
    'password': os.getenv("GOB_IMPORT_DATABASE_PASSWORD", "insecure"),
    'host': os.getenv("GOB_IMPORT_DATABASE_HOST", "localhost"),
    'port': os.getenv("GOB_IMPORT_DATABASE_PORT", 5412),
    'database': os.getenv("GOB_IMPORT_DATABASE", 'gob_import'),
}

BAGEXTRACT_DOWNLOAD_URL = os.getenv("BAGEXTRACT_DOWNLOAD_URL", "https://extracten.bag.kadaster.nl/lvbag/extracten")
MUTATIONS_IMPORT_CALLBACK_KEY = "mutations_import_callback"
MUTATIONS_IMPORT_CALLBACK_QUEUE = "gobimport.mutations.callback"
