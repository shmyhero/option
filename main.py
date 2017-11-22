import traceback
from utils.logger import Logger
from common.pathmgr import PathMgr
from ingestion.dailyingestor import DailyIngestor


def main():
    logger = Logger(__name__, PathMgr.get_log_path())
    try:
        DailyIngestor().process_all()
    except Exception as e:
        logger.exception('Trace: ' + traceback.format_exc())
        logger.error('Error: ' + str(e))

if __name__ == '__main__':
    main()
