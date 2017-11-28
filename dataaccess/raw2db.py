import datetime
import json
import os.path
from utils.logger import Logger
from utils.stringhelper import string_fetch
from utils.iohelper import read_file_to_string, get_sub_files, get_sub_dir_names
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
        if os.path.exists(equity_path):
            content = read_file_to_string(equity_path)
            items = content.split(',')
            equity = Equity()
            equity.symbol = items[1]
            equity.tradeTime = datetime.datetime.strptime(items[0], '%Y%m%d')
            equity.openPrice = round(float(items[2]), 4)
            equity.highPrice = round(float(items[3]), 4)
            equity.lowPrice= round(float(items[4]), 4)
            equity.lastPrice = round(float(items[5]), 4)
            equity.priceChange = equity.lastPrice - round(float(items[6]), 4)
            equity.volume = round(float(items[7]), 4)
            return equity
        else:
            return None
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
            record1 = string_fetch(content, '_CON_OP_%s=\"'% option.symbol, '\"').split(',')
            record2 = string_fetch(content, '_CON_ZL_%s=\"' % option.symbol, '\"').split(',')
            option.tradeTime = datetime.datetime.strptime(record1[32], '%Y-%m-%d %H:%M:%S')
            option.date = datetime.datetime.strptime(record1[32][0:10], '%Y-%m-%d')
            option.daysToExpiration = (option.expirationDate - option.date).days
            option.openPrice = round(float(record1[9]), 4)
            option.lastPrice = round(float(record1[22]), 4)
            option.highPrice = round(float(record1[39]), 4)
            option.lowPrice = round(float(record1[40]), 4)
            option.askPrice = round(float(record1[1]), 4)
            option.bidPrice = round(float(record1[2]), 4)
            option.strikePrice = round(float(record1[7]), 4)
            option.priceChange = option.lastPrice - round(float(record1[8]), 4)
            option.volume = int(record1[41])
            option.delta = round(float(record2[13]), 4)
            option.gamma = round(float(record2[14]), 4)
            option.theta = round(float(record2[15]), 4)
            option.vega = round(float(record2[16]), 4)
            option.volatility = round(float(record2[17]), 4)
            #print record1, record2
            yield option

    def process_all(self):
        equity = self.read_equity()
        self.logger.info("insert equity")
        if equity is not None:
            EquityDAO().insert([equity])
        expiration_dic = self.read_expiration_dates()
        options = list(self.get_options(expiration_dic))
        self.logger.info("insert options")
        OptionDAO().insert(options)


if __name__ == '__main__':
    #print Raw2Db().read_contracts()
    #Raw2Db(PathMgr.get_raw_data_path('2017-11-25')).process_all()
    for raw_folder_name in get_sub_dir_names(PathMgr.get_raw_data_path()):
        print 'process for %s'%raw_folder_name
        Raw2Db(os.path.join(PathMgr.get_raw_data_path(), raw_folder_name)).process_all()
    #Raw2Db().insert_historical_equity()
