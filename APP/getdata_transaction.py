import tushare as ts
import pandas as pd
from common.base import Base
from sqlalchemy import create_engine
class GetDataTransaction(object):
    def set_data(self):
        self.code = '600848'
        self.start = '2017-11-03'
        self.end = '2017-11-07'
        self.df=pd.DataFrame()
    def __call__(self, conns):
        self.set_data()
        self.base = Base()
        self.financial_data = conns['financial_data']
        self.df = ts.get_stock_basics()
        self.base.batchwri(self.df, 'stock_basics1', self.financial_data)
    def get_today_all(self, conns):
        self.set_data()
        self.base = Base()
        self.financial_data = conns['financial_data']
        #self.df=ts.get_hist_data(self.code,self.start,self.end)
        #self.df=ts.get_stock_basics(self.code,self.start,self.end)
        #self.df=ts.get_today_all()
        #self.df=ts.get_tick_data(self.code,date='2017-01-09')
        #self.df=ts.get_realtime_quotes(self.code)
        #self.df=ts.get_today_ticks(self.code)
        #self.df=ts.get_index()
        #self.df=ts.get_sina_dd(self.code,date='2014-01-09')
        self.base.batchwri(self.df, 'realtime_quotes', self.financial_data)

if __name__ == "__main__":

    base = Base()
    financial_data = base.conn('financial_data')
    conns = {'financial_data': financial_data}
    t3s = GetDataTransaction()
    t3s(conns)
    #t3s.get_today_all()
    financial_data.close()