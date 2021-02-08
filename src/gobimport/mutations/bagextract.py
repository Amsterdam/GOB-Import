import datetime
import re

import htmllistparse
from gobcore.exceptions import GOBException
from gobcore.enum import ImportMode
from gobimport.config import BAGEXTRACT_DOWNLOAD_URL
from gobimport.database.model import MutationImport
from gobimport.mutations.exception import NothingToDo


class BagExtractMutationsHandler:
    # Full import every 15th of the month
    FULL_IMPORT_DAY = 15

    def _last_full_import_date(self):
        now = datetime.date.today()
        if now.day < self.FULL_IMPORT_DAY:
            new_month = now.month - 1
            new_year = now.year - 1 if new_month <= 0 else now.year
            return now.replace(
                month=new_month if new_month > 0 else new_month + 12,
                year=new_year,
                day=self.FULL_IMPORT_DAY
            )
        else:
            return now.replace(day=self.FULL_IMPORT_DAY)

    def _date_gemeente_from_filename(self, filename: str):
        if m := re.match(r"^BAGGEM(\d{4})L-(\d{2})(\d{2})(\d{4}).zip$", filename):
            return datetime.date(int(m.group(4)), int(m.group(3)), int(m.group(2))), m.group(1)

        if m := re.match(r"^BAGNLDM-\d{8}-(\d{2})(\d{2})(\d{4}).zip$", filename):
            return datetime.date(int(m.group(3)), int(m.group(2)), int(m.group(1))), None

        raise GOBException(f"Could not parse filename. Unknown format: {filename}")

    def _datestr(self, date: datetime.date):
        return date.strftime("%d%m%Y")

    def _get_full_download_path(self, gemeente: str):
        return f"{BAGEXTRACT_DOWNLOAD_URL}/Gemeente LVC/{gemeente}/"

    def _get_mutations_download_path(self):
        return f"{BAGEXTRACT_DOWNLOAD_URL}/Nederland dagmutaties/"

    def _list_path(self, path: str):
        _, listing = htmllistparse.fetch_listing(path)
        return [entry.name for entry in listing]

    def _get_available_full_downloads(self, gemeente: str):
        return self._list_path(self._get_full_download_path(gemeente))

    def _get_available_mutation_downloads(self):
        return self._list_path(self._get_mutations_download_path())

    def restart_import(self, last_import: MutationImport) -> tuple[ImportMode, str]:
        mode = last_import.mode
        date, gemeente = self._date_gemeente_from_filename(last_import.filename)

        if mode == ImportMode.FULL.value:
            return self.start_full(date, gemeente)
        else:
            return self.start_mutations(date)

    def start_next(self, last_import: MutationImport, gemeente: str) -> tuple[ImportMode, str]:
        date, _ = self._date_gemeente_from_filename(last_import.filename)
        next_date = date + datetime.timedelta(days=1)

        if next_date.day == self.FULL_IMPORT_DAY:
            return self.start_full(next_date, gemeente)
        else:
            return self.start_mutations(next_date)

    def start_full(self, date: datetime.date, gemeente: str) -> tuple[ImportMode, str]:
        fname = f"BAGGEM{gemeente}L-{self._datestr(date)}.zip"

        if fname not in self._get_available_full_downloads(gemeente):
            raise NothingToDo.file_not_available(fname)

        return ImportMode.FULL, fname

    def start_mutations(self, date: datetime.date) -> tuple[ImportMode, str]:
        fname = f"BAGNLDM-{self._datestr(date - datetime.timedelta(days=1))}-{self._datestr(date)}.zip"

        if fname not in self._get_available_mutation_downloads():
            raise NothingToDo.file_not_available(fname)

        return ImportMode.MUTATIONS, fname

    def _get_gemeente(self, dataset: dict):
        read_config = dataset.get('source', {}).get('read_config', {})
        return read_config.get('gemeentes')[0]

    def handle_import(self, last_import: MutationImport, dataset: dict) -> tuple[MutationImport, dict]:
        gemeente = self._get_gemeente(dataset)

        if not last_import:
            date = self._last_full_import_date()
            mode, fname = self.start_full(date, gemeente)
        elif not last_import.is_ended():
            mode, fname = self.restart_import(last_import)
        else:
            mode, fname = self.start_next(last_import, gemeente)

        mutation_import = MutationImport()
        mutation_import.catalogue = dataset['catalogue']
        mutation_import.collection = dataset['entity']
        mutation_import.application = dataset['source']['application']
        mutation_import.mode = mode.value
        mutation_import.filename = fname

        download_base_path = self._get_full_download_path(gemeente) \
            if mode == ImportMode.FULL \
            else self._get_mutations_download_path()

        # Update read_config for importer
        dataset['source']['read_config'] |= {
            'download_location': f"{download_base_path}{fname}",
        }

        return mutation_import, dataset

    def have_next(self, mutation_import: MutationImport, dataset: dict) -> bool:
        gemeente = self._get_gemeente(dataset)
        try:
            self.start_next(mutation_import, gemeente)
        except NothingToDo:
            return False
        return True
