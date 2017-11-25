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
from dataaccess.optiondao import OptionDAO


class Raw2Db(object):

    def __init__(self, daily_raw_path=None):
        if daily_raw_path is None:
            daily_raw_path = PathMgr.get_raw_data_path(str(datetime.date.today()))
        self.daily_raw_path = daily_raw_path
        self.logger = Logger(__name__, PathMgr.get_log_path())


    def parse_lst(self, lst):
        equity = Equity()
        row = string_fetch(lst, '(', ')').split(' ')
        equity.symbol = '510050'
        equity.tradeTime = datetime.datetime.strptime(row[0], '%Y%m%d')
        equity.openPrice = float(row[1])
        equity.highPrice = float(row[2])
        equity.lowPrice = float(row[3])
        equity.lastPrice = float(row[4])
        equity.volume = float(row[5])
        return equity


    def insert_historical_equity(self):
        historical_path = os.path.join(PathMgr.get_data_path(), '510050.lst')
        content = read_file_to_string(historical_path)
        sub_contents = content.split('\n')[1:-1]
        equities = map(self.parse_lst, sub_contents)
        EquityDAO().insert(equities)

    def read_equity(self):
        equity_path = os.path.join(self.daily_raw_path, 'equity.txt')
        content = read_file_to_string(equity_path)
        items = content.split(',')
        equity = Equity()
        equity.symbol = items[1]
        equity.tradeTime = datetime.datetime.strptime(items[0], '%Y%m%d')
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
        all_text_files = get_sub_files(self.daily_raw_path, 'txt')
        option_paths = filter(lambda x: 'call' in x or 'put' in x, all_text_files)
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
            record1 = string_fetch(content, '_CON_OP_%s=\"'% option.symbol, ',M\"').split(',')
            record2 = string_fetch(content, '_CON_ZL_%s=\"' % option.symbol, ',M\"').split(',')
            option.tradeTime = datetime.datetime.strptime(record1[32], '%Y-%m-%d %H:%M:%S')
            option.date = datetime.datetime.strptime(record1[32][0:10], '%Y-%m-%d')
            option.daysToExpiration = (option.expirationDate - option.date).days
            option.openPrice = float(record1[9])
            option.lastPrice = float(record1[22])
            option.highPrice = float(record1[39])
            option.lowPrice = float(record1[40])
            option.askPrice = float(record1[1])
            option.bidPrice = float(record1[2])
            option.strikePrice = float(record1[7])
            option.priceChange = option.lastPrice - float(record1[8])
            option.volume = int(record1[41])
            option.delta = float(record2[13])
            option.gamma = float(record2[14])
            option.theta = float(record2[15])
            option.vega = float(record2[16])
            option.volatility = float(record2[17])
            #print record1, record2
            yield option

    def process_all(self):
        equity = self.read_equity()
        self.logger.info("insert equity")
        EquityDAO().insert([equity])
        expiration_dic = self.read_expiration_dates()
        options = list(self.get_options(expiration_dic))
        self.logger.info("insert options")
        OptionDAO().insert(options)


if __name__ == '__main__':
    #print Raw2Db().read_contracts()
    Raw2Db().process_all()
    #Raw2Db().insert_historical_equity()
