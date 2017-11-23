import datetime
from utils.logger import Logger
from common.pathmgr import PathMgr


class Raw2Db(object):

    def __init__(self, daily_raw_path = None):
        if daily_raw_path is None:
            daily_raw_path = PathMgr.get_raw_data_path(str(datetime.date.today()))
        self.daily_raw_path = daily_raw_path
        self.logger = Logger(__name__, PathMgr.get_log_path())

    def process_equity(self):
        pass

