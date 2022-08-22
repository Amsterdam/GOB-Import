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

A running [GOB infrastructure](https://github.com/Amsterdam/GOB-Infra) is required to run this component.

## Secure data

In order to encrypt secure data you need to define environment variables:

- `SECURE_SALT` and `SECURE_PASSWORD`
  - shared with [GOB API](https://github.com/Amsterdam/GOB-API) (symmetrical encryption).
    GOB Import is responsable for the encryption and GOB API uses the secrets for decryption

# Docker

## Requirements

* docker compose >= 1.25
* Docker CE >= 18.09

## Neuron database

Connect to the Neuron database with credentials retrieved with help of
[this guide](https://dev.azure.com/CloudCompetenceCenter/Datateam%20Basis%20en%20Kernregistraties/_wiki/wikis/Datateam-Basis-en-Kernregistraties.wiki/1700/Verbinden-met-acceptatieomgeving-en-bronsystemen).

When running the project in Docker, edit the `.env` file and set `NRBIN_*` parameters to the params retrieved from the previous guide.

Set the NRBIN host to `host.docker.internal`:

```bash
NRBIN_DATABASE_HOST=host.docker.internal
```

Forward the port using SSH:

```bash
ssh -L 0.0.0.0:1521:$DB:1521 $HOST
```


## Run

Finally, start docker.

```bash
docker compose build
docker compose up
```


## Tests

```bash
docker compose -f src/.jenkins/test/docker-compose.yml build
docker compose -f src/.jenkins/test/docker-compose.yml run test
```

# Local

## Requirements

* Python >= 3.6

## Initialisation

Create a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r src/requirements.txt
```

Or activate the previously created virtual environment:

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

Imports are triggered by the [GOB-Workflow](https://github.com/Amsterdam/GOB-Workflow) module. See the GOB-Workflow README for more details.
