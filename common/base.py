# -*- coding:UTF-8 -*-
import os
import datetime
import time
import pandas as pd
from common.DbCommon import mysql2pd


class Base():
    def __init__(self):
        self.dbconfig = {
            'financial_data':('123.59.214.229', '33333', 'financial_data', 'fin_data', 'fin_data'),
        }

    def conn(self,db):
        if db=='financial_data' :
            return mysql2pd(*self.dbconfig[db])

    # 输出的日期格式为xxxx/xx/xx的形式
    def datelist(self, beginDate, endDate):
        # beginDate, endDate是形如‘20160601’的字符串或datetime格式
        date_l = [datetime.datetime.strftime(x, '%Y/%m/%d') for x in list(pd.date_range(start=beginDate, end=endDate))]
        return date_l

    def getYesterday(self, day=None, minus=1):
        if None==day:
            today = datetime.date.today()
        else:
            today=datetime.datetime(int(day[0:4]), int(day[5:7]), int(day[8:10]))
        oneday = datetime.timedelta(days=minus)
        yesterday = today - oneday
        yesterday = datetime.datetime.strftime(yesterday,'%Y/%m/%d')
        return yesterday

    def gettoday(self):
        today = datetime.date.today()
        res = datetime.datetime.strftime(today,'%Y/%m/%d')
        return res

    def getTomorrow(self,day=None,add=1):
        if None==day:
            today = datetime.date.today()
        else:
            today=datetime.datetime(int(day[0:4]), int(day[5:7]), int(day[8:10]))
        oneday = datetime.timedelta(days=add)
        tomorrow = today + oneday
        tomorrow = datetime.datetime.strftime(tomorrow,'%Y/%m/%d')
        return tomorrow

    @staticmethod
    def getTodayList():
        return [time.strftime('%Y/%m/%d')]

    # 批量写入mysql，每次1000条
    def batchwri(self, res, table,conn):
        print(res.shape)
        total = res.shape[0]
        nowrow = 0
        while nowrow < total - 1000:
            conn.write2mysql(res[nowrow:nowrow + 1000], table)
            nowrow += 1000
        conn.write2mysql(res[nowrow:], table)
