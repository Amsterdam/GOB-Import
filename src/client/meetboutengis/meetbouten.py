import datetime
import os.path

from pandas import pandas
from .util import as_str, as_decimal, as_date

from client.import_client import ImportClient


class Meetbouten(ImportClient):
    def __init__(self, config, input_dir):
        self._file_path = os.path.join(input_dir, "MEETBOUT.csv")
        self._meetbouten = None
        super().__init__(config)

    def id(self):
        return f"Import Meetbouten from {self._file_path}"

    def connect(self):
        self._data = pandas.read_csv(
            filepath_or_buffer=self._file_path,
            sep=";",
            encoding="ISO-8859-1",
            dtype=str)

    def read(self):
        # No action required, data is read by pandas in self._data
        pass

    def convert(self):
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
                "nabij_ref_adres": as_str(row["BOUT_ADRES"]),
                "ligt_in_ref_bouwblok": as_str(row["BOUT_BLOKNR"]),
                "ligt_in_ref_stadsdeel": as_str(row["BOUT_DEELRAAD"]),
                "hoogte_tov_nap": as_decimal(row["BOUT_HOOGTENAP"]),
                "zakking_cumulatief": as_decimal(row["BOUT_ZAKKINGCUM"]),
                "zakkingssnelheid": as_decimal(row["BOUT_ZAKSNELH"]),
                "datum": as_date(row["BOUT_DATUM"], "%Y%m%d"),
            }
            self._meetbouten.append(meetbout)
            break

    def publish(self):
        results = {
            "source": "meetboutengis",
            "entity": "meetbouten",
            "version": "0.1",
            "timestamp": datetime.datetime.now().isoformat(),
            "contents": self._meetbouten
        }
        self.publish_results(results)
