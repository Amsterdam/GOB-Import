from config import config

from connector import connect
from reader import read
from converter import convert
from publisher import publish


def main():
    connect

    connection = connect(config=config["connector"])
    raw_data = read(config=config["reader"], connection=connection)
    data = convert(config=config["converter"], raw_data=raw_data)
    publish(config=config["publisher"], data=data)


if __name__ == "__main__":
    main()
