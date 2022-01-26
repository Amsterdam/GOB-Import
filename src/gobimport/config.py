import os

CONTAINER_BASE = os.getenv("CONTAINER_BASE", "acceptatie")
# in env?
# zie datastore/config BAGExtract! Die env mocken.
# DATABASE_CONFIG = {
#     "drivername": "postgres",
#     "username": os.getenv("GOB_BAGEXTRACT_DATABASE_USER", "gob_bagextract"),
#     "password": os.getenv("GOB_BAGEXTRACT_DATABASE_PASSWORD", "insecure"),
#     "host": os.getenv("GOB_BAGEXTRACT_DATABASE_HOST", "localhost"),
#     "port": os.getenv("GOB_BAGEXTRACT_DATABASE_PORT", 5413),
#     "database": os.getenv("GOB_BAGEXTRACT_DATABASE", "gob_bagextract"),
# }
