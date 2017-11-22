import datetime
import os
import json
from utils.stringhelper import string_fetch
from utils.iohelper import ensure_dir_exists, write_to_file
from utils.logger import Logger
from common.pathmgr import PathMgr
from webscraper import WebScraper


class DailyIngestor(object):

    def __init__(self, raw_data_path=PathMgr.get_raw_data_path()):
        self.date = datetime.date.today()
        self.raw_data_path = raw_data_path
        self.daily_path = os.path.join(self.raw_data_path, str(self.date))
        ensure_dir_exists(self.daily_path)
        self.logger = Logger(__name__, PathMgr.get_log_path())

    def gen_contract_month(self):
        content = WebScraper.get_contract()
        contract_path = os.path.join(self.daily_path, 'contact.json')
        write_to_file(contract_path, content)
        data = json.loads(content)
        contract_month = data['result']['data']['contractMonth']
        return contract_month

    def gen_expirations(self, contract_month):
        content = WebScraper.get_expiration_dates(contract_month)
        expiration_path = os.path.join(self.daily_path, 'expiration_{}.json'.format(contract_month))
        write_to_file(expiration_path, content)

    def get_call_option_symbols(self, yymm):
        content = WebScraper.get_call_option_symbols(yymm)
        sub = string_fetch(content, '=\"', ',\"')
        return map(lambda x: x.replace('CON_OP_', ''), sub.split(','))

    def get_put_option_symbols(self, yymm):
        content = WebScraper.get_put_option_symbols(yymm)
        sub = string_fetch(content, '=\"', ',\"')
        return map(lambda x: x.replace('CON_OP_', ''), sub.split(','))

    def gen_option(self, symbol, option_type, yymm):
        content = WebScraper.get_option_info(symbol)
        symbol_path = os.path.join(self.daily_path, '{}_{}_{}.txt'.format(option_type, yymm, symbol))
        write_to_file(symbol_path, content)

    def process_all(self):
        self.logger.info('ingest data for contract month...')
        contract_month_list  = self.gen_contract_month()
        for contract_month in contract_month_list:
            self.logger.info('ingest data for %s...' % contract_month)
            self.gen_expirations(contract_month)
            yymm = contract_month[2:].replace('-', '')
            call_symbols = self.get_call_option_symbols(yymm)
            put_symbols = self.get_put_option_symbols(yymm)
            for symbol in call_symbols:
                self.gen_option(symbol, 'call', yymm)
            for symbol in put_symbols:
                self.gen_option(symbol, 'put', yymm)




if __name__ == '__main__':
    ingestor = DailyIngestor()
    #ingestor.get_contract_month()
    #ingestor.get_expirations('2017-09')
    ingestor.process_all()

