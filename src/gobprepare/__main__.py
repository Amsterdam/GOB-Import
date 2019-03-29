from gobprepare.prepare_client import PrepareClient

import json
import os
import time

if __name__ == "__main__":
    # Temp entry point for dev purposes
    with open(os.path.join(os.path.dirname(__file__), "data", "brk.prepare.json")) as file:
        s = time.time()
        prepare_client = PrepareClient(json.load(file))
        prepare_client.start_prepare_process()
        e = time.time()
        print(f"{e-s} seconds elapsed")
