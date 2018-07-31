"""The meetboutengis Meetbouten import gobimportclient

The import is implemented by deriving from the abstract ImportClient class.

"""
import datetime
import os.path

from pandas import pandas
from .util import as_str, as_decimal, as_date

from gobimportclient.import_client import ImportClient


class Meetbouten(ImportClient):
    """The Meetbouten class implements the ImportClient interface to import the Meetbouten data

    """

    def __init__(self, config, input_dir):
        """Initialize the import gobimportclient

        :param config: The configuration object for imports
        :param input_dir: The directort where the import file resides, e.g. /download
        """
        self._file_path = os.path.join(input_dir, "MEETBOUT.csv")
        self._meetbouten = None
        super().__init__(config)

    def id(self):
        """Provide for an identification of this import gobimportclient

        :return:
        """
        return f"Import Meetbouten from {self._file_path}"

    def connect(self):
        """Connect to the datasource

        The pandas library is used to connect to the data source

        :return:
        """
        self._data = pandas.read_csv(
            filepath_or_buffer=self._file_path,
            sep=";",
            encoding="ISO-8859-1",
            dtype=str)

    def read(self):
        """Read the data from the data source

        No action required here, data is read by pandas in self._data

        :return:
        """
        pass

    def convert(self):
        """Convert the input data to GOB format

        :return:
        """
        self._meetbouten = []
        for index, row in self._data.iterrows():
            xcoordinaat = as_decimal(row["BOUT_XCOORD"])
            ycoordinaat = as_decimal(row["BOUT_YCOORD"])
            # todo
            # Get the identification of adres, bouwblok and stadsdeel
            # The values in meetbouten do not refer to an id
            # For each ref value the corresponding id has to be deteremined
            # Ideally the GOB API is used to get the id as this will
            # be common logic that is required by multiple parties
            # For now the GOB API doesn't yet know about these ref entities
            meetbout = {
                "meetboutid": as_str(row["BOUT_NR"]),
                "xcoordinaat": xcoordinaat,
                "ycoordinaat": ycoordinaat,
                "locatie": as_str(row["BOUT_LOCATIE"]),
                "bouwblokzijde": as_str(row["BOUT_BLOKZIJDE"]),
                "blokeenheid": as_str(row["BOUT_BLOKEENH"]),
                "status": as_str(row["BOUT_STATUS"]),
                "indicatie_beveiligd": as_str(row["BOUT_BEVEILIGD"]) != "N",
                "eigenaar": as_str(row["BOUT_EIGENAAR"]),
                "geometrie": f"POINT({xcoordinaat}, {ycoordinaat})",
                "nabij_ref_adressen": as_str(row["BOUT_ADRES"]),
                "ligt_in_ref_bouwblokken": as_str(row["BOUT_BLOKNR"]),
                "ligt_in_ref_stadsdelen": as_str(row["BOUT_DEELRAAD"]),
                "hoogte_tov_nap": as_decimal(row["BOUT_HOOGTENAP"]),
                "zakking_cumulatief": as_decimal(row["BOUT_ZAKKINGCUM"]),
                "zakkingssnelheid": as_decimal(row["BOUT_ZAKSNELH"]),
                "datum": as_date(row["BOUT_DATUM"], "%Y%m%d"),
            }
            self._meetbouten.append(meetbout)

    def publish(self):
        """Publish the import result

        :return:
        """
        result = {
            "header": {
                "version": "0.1",   # This version is used in the system to allow for migrations
                "entity": "meetbouten",
                "entity_id": "meetboutid",
                "source": "meetboutengis",
                "source_id": "meetboutid",
                "timestamp": datetime.datetime.now().isoformat(),
            },
            "summary": None,  # No log, metrics and qa indicators for now
            "contents": self._meetbouten
        }
        self.publish_result(result)
