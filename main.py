from dataGatherer import dataGatherer
from dataRefiner import cleanAndUploadPipelineAlkemy
from dataRefiner import serverPipeline
import logging
from decouple import config

LOADERALT = config('LOADERALT', cast=bool)


logging.basicConfig(filename='test.log', level=logging.DEBUG, force=True,
                    format='%(asctime)s:%(levelname)s:%(message)s')


def main():
    dataGatherer()

    if LOADERALT:
        logging.debug('Utilizando server loader sin SQLAlchemy')
        serverPipeline()

    else:
        cleanAndUploadPipelineAlkemy()


main()
