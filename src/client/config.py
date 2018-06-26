import os

config = {
    "connector": {},
    "reader": {},
    "converter": {},
    "publisher": {
        "connection_address": os.environ["MESSAGE_BROKER_ADDRESS"],
        "queue": "import"
    }
}
