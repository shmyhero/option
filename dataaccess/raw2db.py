import datetime
import json
import os.path
from utils.logger import Logger
from utils.stringhelper import string_fetch
from utils.iohelper import read_file_to_string, get_sub_files
from common.pathmgr import PathMgr
from entities.equity import Equity
from entities.option import Option
from dataaccess.equitydao import EquityDAO


class Raw2Db(object):

    def __init__(self, daily_raw_path=None):
        if daily_raw_path is None:
            daily_raw_path = PathMgr.get_raw_data_path(str(datetime.date.today()))
        self.daily_raw_path = daily_raw_path
        self.logger = Logger(__name__, PathMgr.get_log_path())

    def read_equity(self):
        equity_path = os.path.join(self.daily_raw_path, 'equity.txt')
        content = read_file_to_string(equity_path)
        items = content.split(',')
        equity = Equity()
        equity.symbol = items[1]
        equity.tradeTime = datetime.datetime.strptime(items[0], '%y%m%d')
        equity.openPrice = float(items[2])
        equity.highPrice = float(items[3])
        equity.lowPrice= float(items[4])
        equity.lastPrice = float(items[5])
        equity.priceChange = equity.lastPrice - float(items[6])
        equity.volume = float(items[7])
        return equity
        #EquityDAO().insert([equity])

    def read_contracts(self):
        equity_path = os.path.join(self.daily_raw_path, 'contact.json')
        content = read_file_to_string(equity_path)
        json_data = json.loads(content)
        return json_data['result']['data']['contractMonth']

    def read_expiration_dates(self):
        expiration_paths = filter(lambda x: 'expiration' in x, get_sub_files(self.daily_raw_path, 'json'))
        dic = {}
        for expiration_pah in expiration_paths:
            content = read_file_to_string(expiration_pah)
            json_data = json.loads(content)
            expire_day = datetime.datetime.strptime(json_data['result']['data']['expireDay'], '%Y-%m-%d')
            year_month_key = json_data['result']['data']['cateId'][7:]
            dic[year_month_key] = expire_day
        return dic

    def get_options(self, expiration_dic):
        option_paths = filter(lambda x: 'call' == x[0:4 or 'put' == x[0:3]], get_sub_files(self.daily_raw_path, 'txt'))
        for option_path in option_paths:
            head, tail = os.path.split(option_path)
            filename = string_fetch(tail, '', '.txt')
            items = filename.split('_')
            option = Option()
            option.underlingSymbol = '510050'
            option.optionType = items[0].capitalize()
            option.expirationDate = expiration_dic[items[1]]
            option.symbol = items[2]
            content = read_file_to_string(option_path)
            #TODO: parse the content for option.


if __name__ == '__main__':
    #Raw2Db().process_equity()
    print Raw2Db().read_expiration_dates()
