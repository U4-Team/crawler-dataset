from core.mixins import LoggerMixin

from crawler_dataset.services import XLSXDatasetService


class XLSXDatasetParseTask(LoggerMixin):
    service: XLSXDatasetService = XLSXDatasetService()

    def run(self, path: str):
        with open(path, mode='rb') as xlsx_file:
            self.service.parse_dataset_from_file(xlsx_file)
