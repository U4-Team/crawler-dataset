from core.mixins import LoggerMixin

from crawler_dataset.tasks import XLSXDatasetParseTask

class Application(LoggerMixin):
    tasks = (
        XLSXDatasetParseTask,
    )

    def run(self, path: str):
        self.logger.info('Run application! %s', path)
        for task_cls in self.tasks:
            task = task_cls()
            task.run(path)