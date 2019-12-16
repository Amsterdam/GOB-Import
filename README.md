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

## Secure data

In order to encrypt secure data you need to define environment variables:
- SECURE_SALT and SECURE_PASSWORD
  - shared with GOB API (symmetrical encryption).
    GOB Import is responsable for the encryption and GOB API uses the secrets for decryption

# Docker

## Requirements

* docker-compose >= 1.17
* docker ce >= 18.03

## Run

```bash
docker-compose build
docker-compose up &
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

Optional: Set environment if GOB-Import should connect to secure data sources:

```bash
export $(cat .env | xargs)  # Copy from .env.example if missing
```

Start the service:

```bash
cd src
python -m gobimport
```

## Tests

Run the tests:

```bash
cd src
sh test.sh
```

# Remarks

## Trigger imports

Imports are triggered by the GOB-Workflow module. See the GOB-Workflow README for more details
