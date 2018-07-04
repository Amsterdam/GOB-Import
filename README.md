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
    
# Prerequisites

# Local Installation

Start the [GOB Message Broker](https://github.com/Amsterdam/GOB-Message-Broker)

Expose the IP address of the message broker in the environment:

```bash
export MESSAGE_BROKER_ADDRESS=localhost
```

Create a virtual environment:

    python3 -m venv venv
    pip install -r requirements.txt
    
Or activate the previously created virtual environment

    source venv/bin/activate
    
Start the client:

    cd src
    python -m client
