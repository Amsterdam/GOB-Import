"""Start import

Requires one or more dataset description-files to run an import:

     python -m gobimport.start example/meetbouten.json
"""
import argparse

from gobcore.message_broker.config import IMPORT_QUEUE
from gobimport.mapping import get_mapping
from gobcore.message_broker import publish

parser = argparse.ArgumentParser(
    prog='python -m gobimport.start',
    description='Import one or more datasets into GOB',
    epilog='Generieke Ontsluiting Basisregistraties')

parser.add_argument('dataset_file',
                    nargs='+',
                    type=str,
                    help='a file containing the dataset definition')

args = parser.parse_args()

for dataset_file in args.dataset_file:
    dataset = get_mapping(dataset_file)
    print(f"Trigger import of {dataset['entity']} from source {dataset['source']['name']}")
    publish(IMPORT_QUEUE, "import.start", {"dataset": dataset})
