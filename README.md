# GOB-Import

Import and convert data to GOB format and publish the results on the GOB Message Broker.

Importing data consists of the following steps:

    1 Setup a connection to be able to access the external data
        * External data can be stored in files, databases, api's, ...
    2 Use the connection to read the required external data
        * External data can be formatted as csv, xml, StUF, database format, ...
    3 Convert the external data to GOB format
    4 Publish the results

# Infrastructure

A running [GOB infrastructure](https://github.com/Amsterdam/GOB-Infra)
is required to run this component.

# Examples

For the meetbouten catalog, example files have been added to be able to run
the import process without a connection to external database or other source.

These files have been added in the example folder and can be used to import
the following collections:

- meetbouten
- metingen
- rerentiepunten
- rollagen

# Docker

## Requirements

* docker-compose >= 1.17
* docker ce >= 18.03

## Run

```bash
docker-compose build
docker-compose up &

# Start a single import
docker exec gobimport python -m gobimport.start example/meetbouten.json
```

## Tests

```bash
docker-compose -f src/.jenkins/test/docker-compose.yml build
docker-compose -f src/.jenkins/test/docker-compose.yml run test
```

# Local

## Requirements

* python >= 3.6

## Initialisation

Create a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r src/requirements.txt
```

Or activate the previously created virtual environment

```bash
source venv/bin/activate
```

# Run

Start the service:

```bash
export $(cat .env | xargs)  # Copy from .env.example if missing
cd src
python -m gobimport
```

Start a single import in another window:

```bash
cd src
python -m gobimport.start example/meetbouten.json
```


## Tests

Run the tests:

```bash
cd src
sh test.sh
```
