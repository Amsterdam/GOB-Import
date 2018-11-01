# GOB-Import-Client-Template

Template for a GOB Import Client

The responsabiliy of a GOB import client is to import and convert data to GOB format and publish the results on the GOB Message Broker.

Importing data consists of the following steps:

    1 Setup a connection to be able to access the external data
        * External data can be stored in files, databases, api's, ...
    2 Use the connection to read the required external data
        * External data can be formatted as csv, xml, StUF, database format, ...
    3 Convert the external data to GOB format
    4 Publish the results

# Requirements

    * docker-compose >= 1.17
    * docker ce >= 18.03
    * python >= 3.6

# Local Installation

Start the [GOB Workflow](https://github.com/Amsterdam/GOB-Workflow)

You will end up with a running RabbitMQ instance, and a workflow manager listening to it.

Expose the IP address of the message queue in the environment:

```bash
export MESSAGE_BROKER_ADDRESS=localhost
```

Activate a previously created virtual environment for this project:

    source venv/bin/activate

or create a new virtual environment:

    python3 -m venv venv
    source venv/bin/activate
    pip install -r src/requirements.txt

## Meetbouten and Metingen from file

In its first version, only meetbouten and metingen are imported.

The import is started by:

    cd src
    python -m gobimport example/meetbouten.json example/metingen.json

This specific example require import-files, these can be placed in any directory you like,
but the importclient will automatically look for it in the `src/data` directory:

	# Custom datadir:
	export LOCAL_DATADIR=/any/directory/you/like
    python -m gobimport example/meetbouten.json example/metingen.json

## Meetbouten from database

A working connection to the oracle database DBIMBP01 is needed to import the data.
- For local development this requires the "Secure" VPN.

Make sure to install the ODPI-C libraries.
Follow the steps, to get the connection working:
- https://gist.github.com/DGrady/7fb5c2214f247dcff2cb5dd99e231483

Add the configuration for the database connection to a .env file in the root of this repository,
based on the .env.example.

    cp .env.example .env
    
Set the required shell variables:

    export $(cat .env | xargs)

The import is started by:

    cd src
    python -m gobimport example/meetbouten.json

Or if you would like to run in de Docker container:

    docker-compose build
    docker-compose run import_client python -m gobimport example/meetbouten.json

# Docker

Start the message broker, as described in the previous paragraph.

No need to set MESSAGE_BROKER_ADDRESS, this is set to rabbitmq in docker-compose.yml.
The message broker is accessed via the docker network "gob_network".

docker-compose build
docker-compose up

the docker container is named import_client.

# Tests

Three kind of tests are run:
  * Style checks ([flake8](http://flake8.pycqa.org/en/latest/))
  * Unit tests
  * Code Coverage tests

The tests can be started by the test.sh script in the src directory:

```bash
source venv/bin/activate
cd src
sh test.sh
```
