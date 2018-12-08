import logging

import click

from crawler_dataset.app import Application

logger = logging.getLogger(__name__)

@click.group()
def cli() -> None:  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(process)-5s] %(name)-24s %(levelname)s %(message)s',
        datefmt='%d.%m.%Y[%H:%M:%S]',
    )


@cli.command()
@click.option('--path', type=str, default='./dataset.xlsx')
def run(path: str) -> None:
    logger.info('=== START ===')
    app = Application()
    app.run(path)
    logger.info(path)
    logger.info('done')
