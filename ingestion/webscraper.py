import time
import datetime
from utils.httphelper import HttpHelper
from utils.stringhelper import string_fetch


class WebScraper(object):

    def __init__(self):
        pass

    @staticmethod
    def http_get_with_retry(url, times = 3):
        try:
            return HttpHelper.http_get(url)
        except Exception:
            if times > 0:
                time.sleep(1)
                return WebScraper.http_get_with_retry(url, times-1)
            else:
                raise Exception('Failed to get data from %s'%url, )

    @staticmethod
    def get_underlying_equity():
        url = 'http://hq.sinajs.cn/list=sh510050'
        content = WebScraper.http_get_with_retry(url)
        return content

    @staticmethod
    def get_contract():
        url = 'http://stock.finance.sina.com.cn/futures/api/openapi.php/StockOptionService.getStockName'
        content = WebScraper.http_get_with_retry(url)
        return content

    @staticmethod
    def get_expiration_dates(contract_month):
        """
        :param contract_month: yyyy-mm
        :return:
        """
        url_template = 'http://stock.finance.sina.com.cn/futures/api/openapi.php/StockOptionService.getRemainderDay?date={}'
        url = url_template.format(contract_month)
        content = WebScraper.http_get_with_retry(url)
        return content

    @staticmethod
    def get_call_option_symbols(yymm):
        """
        :param yymm: ex: 1709
        :return:
        """
        url_template = "http://hq.sinajs.cn/list=OP_UP_510050{}"
        url = url_template.format(yymm)
        content = WebScraper.http_get_with_retry(url)
        return content

    @staticmethod
    def get_put_option_symbols(yymm):
        """
        :param yymm: ex: 1709
        :return:
        """
        url_template = "http://hq.sinajs.cn/list=OP_DOWN_510050{}"
        url = url_template.format(yymm)
        content = WebScraper.http_get_with_retry(url)
        return content

    @staticmethod
    def get_option_info(symbol):
        url_template = 'http://hq.sinajs.cn/list=CON_OP_{},CON_ZL_{}'
        url = url_template.format(symbol, symbol)
        content = WebScraper.http_get_with_retry(url)
        return content
