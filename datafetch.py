# -*- coding: utf-8 -*-
"""
阿尔法研究平台

Project: sustecher
Author: Moses
E-mail: 8342537@qq.com
"""
import os
import numpy as np
import pandas as pd
import tushare as ts
from retrying import retry
from dataservice import DataService
try:
    basestring
except NameError:
    basestring = str

#打印能完整显示
#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)
#pd.set_option('display.width', 50000)
#pd.set_option('max_colwidth', 1000)


class DataFetcher(DataService):

    @retry(stop_max_attempt_number=500, wait_random_min=1000, wait_random_max=2000)
    def ensure_data(self, func, save_dir, start_dt='20090101', end_dt='20210804'):
        """ 确保按交易日获取数据
        """
        tmp_dir = os.path.join(self.root, "stock", save_dir)
        dl = [pd.to_datetime(name.split(".")[0]) for name in os.listdir(tmp_dir)]
        dl = sorted(dl)
        s = pd.to_datetime(start_dt)
        e = pd.to_datetime(end_dt)
        tdays = pd.Series(self.tradedays, index=self.tradedays)
        tdays = tdays[(tdays>=s)&(tdays<=e)]
        tdays = tdays.index.tolist()
        for tday in tdays:
            if tday in dl: continue
            t = tday.strftime("%Y%m%d")
            datdf = func(t)
            path = os.path.join(tmp_dir, t+".csv")
            datdf.to_csv(path, encoding='gbk')
            print(t+".csv write ok !!!!!")

    def create_indicator(self, raw_data_dir, raw_data_field, indicator_name):
        ''' 主要用于通过日频数据创建日频指标
        '''
        tmp_dir = os.path.join(self.root, "stock", raw_data_dir)
        tdays = [pd.to_datetime(f.split(".")[0]) for f in os.listdir(tmp_dir)]
        tdays = sorted(tdays)
        all_stocks_info = self.meta
        df = pd.DataFrame(index=all_stocks_info.index, columns=tdays)
        for f in os.listdir(tmp_dir):
            tday = pd.to_datetime(f.split(".")[0])
            dat = pd.read_csv(os.path.join(tmp_dir, f), index_col=['ts_code'], engine='python', encoding='gbk')
            df[tday] = dat[raw_data_field]
            print(tday)
        df = df.dropna(how='all') #删掉全为空的一行
        diff = df.index.difference(all_stocks_info.index) #删除没在股票基础列表中多余的股票行
        df = df.drop(labels=diff)
        self.close_file(df, indicator_name)

    def _align_element(self, df1, df2):
        ''' 对齐股票和时间
        '''
        row_index = sorted(df1.index.intersection(df2.index))
        col_index = sorted(df1.columns.intersection(df2.columns))
        return df1.loc[row_index, col_index], df2.loc[row_index, col_index]

    def preprocess(self, datdf, suspend_days_process=False, val=np.nan):
        ''' 数据预处理
        '''
        datdf = datdf.copy()
        datdf = datdf.fillna(method='ffill', axis=1).fillna(method='bfill', axis=1)
        row_index, col_index = datdf.index, datdf.columns
        liststatus = self.listday_matrix.loc[row_index, col_index]
        cond = (liststatus==1)
        datdf = datdf.where(cond) #将不是上市日的数值替换为nan
        if suspend_days_process:
            tradestatus = self.trade_status.loc[row_index, col_index]
            cond = (liststatus==1) & (tradestatus==0)
            datdf = datdf.where(~cond, val) #将上市但停牌的数值设为指定值
        return datdf


class TushareFetcher(DataFetcher):

    def __init__(self):
        self.pro = ts.pro_api('70ebdf9cf1d553d72c74eca9e9f3bc8ad7addea54be96eba559a71f3')
        super().__init__()

    def fetch_meta_data(self):
        """ 股票基础信息
        """
        df_list = []
        df = self.pro.stock_basic(exchange='', fields='ts_code,name,list_date,delist_date')
        df_list.append(df)
        df = self.pro.stock_basic(exchange='', fields='ts_code,name,list_date,delist_date', list_status='D')
        df_list.append(df)
        df = self.pro.stock_basic(exchange='', fields='ts_code,name,list_date,delist_date', list_status='P')
        df_list.append(df)
        df = pd.concat(df_list)
        df = df.rename(columns={"list_date":"ipo_date"})
        df = df.rename(columns={'name':'sec_name'})
        df = df.rename(columns={"ts_code":"code"})
        df.drop_duplicates(subset=['code'], keep='first', inplace=True)
        df.sort_values(by=['ipo_date'], inplace=True)
        #print(pd.to_datetime(df['ipo_date']))
        #df.reset_index(drop=True, inplace=True)
        df.set_index(['code'], inplace=True)
        self.close_file(df, 'meta')

    def fetch_trade_day(self):
        """ 交易日列表
        """
        df = self.pro.trade_cal(is_open='1')
        df = df[['cal_date','is_open']]
        df = df.rename(columns={"cal_date":"tradedays"})
        df.set_index(['tradedays'], inplace=True)
        self.close_file(df, 'tradedays')

    #------------------------------------------------------------------------------------
    #日数据
    def daily(self, t):
        return self.pro.daily(trade_date=t)

    def suspend_d(self, t):
        return self.pro.suspend_d(trade_date=t)

    def adj_factor(self, t):
        return self.pro.adj_factor(trade_date=t)

    #------------------------------------------------------------------------------------
    #指数日行情
    def index_daily(self):
        index_list = ['000001.SH', '000300.SH', '000905.SH']
        tmp_dir = os.path.join(self.root, "stock", "__temp_index_daily__")
        for i in index_list:
            df = self.pro.index_daily(ts_code=i)
            path = os.path.join(tmp_dir, i+".csv")
            df.to_csv(path, encoding='gbk')
            print(i+".csv write ok !!!!!")

    #------------------------------------------------------------------------------------
    #创建各种指标

    def create_listday_matrix(self):
        ''' 股票上市存续周期日矩阵
        '''
        all_stocks_info = self.meta
        trade_days = self.tradedays

        def if_listed(series):
            nonlocal all_stocks_info
            code = series.name
            ipo_date = all_stocks_info.at[code, 'ipo_date']
            delist_date = all_stocks_info.at[code, 'delist_date']
            daterange = series.index
            if delist_date is pd.NaT:
                res = np.where(daterange >= ipo_date, 1, 0)
            else:
                res = np.where(daterange < ipo_date, 0, np.where(daterange <= delist_date, 1, 0))
            return pd.Series(res, index=series.index)

        listday_dat = pd.DataFrame(index=all_stocks_info.index, columns=trade_days)
        listday_dat = listday_dat.apply(if_listed, axis=1)
        self.close_file(listday_dat, 'listday_matrix')

    def create_trade_status(self):
        ''' 股票停复牌状态
        '''
        tmp_dir = os.path.join(self.root, "stock", "__temp_suspend_d__")
        tdays = [pd.to_datetime(f.split(".")[0]) for f in os.listdir(tmp_dir)]
        tdays = sorted(tdays)
        all_stocks_info = self.meta
        df = pd.DataFrame(index=all_stocks_info.index, columns=tdays)
        df.loc[:, :] = 1 #默认都是正常状态
        for f in os.listdir(tmp_dir):
            tday = pd.to_datetime(f.split(".")[0])
            dat = pd.read_csv(os.path.join(tmp_dir, f), index_col=['ts_code'], engine='python', encoding='gbk')
            df.loc[dat.index, tday] = 0 #停牌的设置为0
            print(tday)
        self.close_file(df, "trade_status")

    def create_daily_quote_indicators(self):
        '''
        '''
        #-------------------------------------------------------------
        #创建一些行情指标
        self.create_indicator("__temp_daily__", "open", "open")
        open = self.preprocess(self.open)
        self.close_file(open, 'open')

        self.create_indicator("__temp_daily__", "close", "close")
        close = self.preprocess(self.close)
        self.close_file(close, 'close')

        self.create_indicator("__temp_daily__", "high", "high")
        high = self.preprocess(self.high)
        self.close_file(high, 'high')

        self.create_indicator("__temp_daily__", "low", "low")
        low = self.preprocess(self.low)
        self.close_file(low, 'low')

        self.create_indicator("__temp_daily__", "vol", "vol")
        vol = self.preprocess(self.vol)
        self.close_file(vol, 'vol')

        self.create_indicator("__temp_daily__", "amount", "amount")
        amount = self.preprocess(self.amount)
        self.close_file(amount, 'amount')

        self.create_indicator("__temp_adj_factor__", "adj_factor", "adjfactor")
        adjfactor = self.preprocess(self.adjfactor)
        self.close_file(adjfactor, 'adjfactor')

        close, adjfactor = self._align_element(self.close, self.adjfactor)
        hfq_close = close * adjfactor
        self.close_file(hfq_close, 'hfq_close') #后复权收盘价

    def create_index_quote_indicators(self, indicator_name, start_dt='20090101', end_dt='20210804'):
        '''
        '''
        benchmarks = ['000001.SH', '000300.SH', '000905.SH'] #上证综指,沪深300,中证500
        s = pd.to_datetime(start_dt)
        e = pd.to_datetime(end_dt)
        tdays = pd.Series(self.tradedays, index=self.tradedays)
        tdays = tdays[(tdays>=s)&(tdays<=e)]
        tdays = tdays.index.tolist()
        df = pd.DataFrame(index=benchmarks, columns=tdays)
        tmp_dir = os.path.join(self.root, "stock", "__temp_index_daily__")
        for bm in benchmarks:
            dat = pd.read_csv(os.path.join(tmp_dir, bm+".csv"), index_col=['trade_date'], engine='python', encoding='gbk', parse_dates=['trade_date'])
            df.loc[bm] = dat[indicator_name][df.columns]
        self.close_file(df, 'bm_'+indicator_name)

    def create_index_members(self, index_name, indicator_name, start_dt='20090101', end_dt='20210804'):
        '''
        '''
        tmp_dir = os.path.join(self.root, "stock", "__temp_index_members__")
        df_im = pd.read_parquet(os.path.join(tmp_dir, 'AINDEXMEMBERS.parquet'), engine='pyarrow')
        df_im = df_im[df_im['S_INFO_WINDCODE']==index_name]
        df_im = df_im.drop('S_INFO_WINDCODE', axis=1)
        df_im = df_im.set_index('S_CON_WINDCODE')
        df_im['S_CON_INDATE'] = pd.to_datetime(df_im['S_CON_INDATE'])
        df_im['S_CON_OUTDATE'] = pd.to_datetime(df_im['S_CON_OUTDATE'])
        #
        all_stocks_info = self.meta
        s = pd.to_datetime(start_dt)
        e = pd.to_datetime(end_dt)
        tdays = pd.Series(self.tradedays, index=self.tradedays)
        tdays = tdays[(tdays>=s)&(tdays<=e)]
        tdays = tdays.index.tolist()
        #
        def if_index_member(series):
            nonlocal df_im
            dt = series.name
            isin = ((df_im['S_CON_INDATE'] <= dt) & ((df_im['S_CON_OUTDATE'] > dt) | (pd.isnull(df_im['S_CON_OUTDATE']))))
            isin = isin[isin]
            #print(len(isin))
            series[isin.index] = 1 #dt时间段是指数成员的话就设置为1
            return series
        #
        dat = pd.DataFrame(index=all_stocks_info.index, columns=tdays, data=0)
        dat = dat.apply(if_index_member, axis=0) #每次处理一列
        self.close_file(dat, indicator_name)


def TushareFetch():
    fetcher = TushareFetcher()
    #---------------------------------------------------------------
    # 先下载数据到本地
    #---------------------------------------------------------------
    #fetcher.fetch_meta_data()
    #fetcher.fetch_trade_day()
    #fetcher.ensure_data(fetcher.daily, "__temp_daily__") #日行情表
    #fetcher.ensure_data(fetcher.suspend_d, "__temp_suspend_d__") #停牌表
    #fetcher.ensure_data(fetcher.adj_factor, "__temp_adj_factor__") #复权因子表
    #fetcher.index_daily()
    #---------------------------------------------------------------
    # 然后从本地数据生成指标
    #---------------------------------------------------------------
    #fetcher.create_listday_matrix()
    #fetcher.create_trade_status()
    #fetcher.create_daily_quote_indicators()
    #fetcher.create_index_members('000300.SH', 'hs300_member')
    #fetcher.create_index_members('000905.SH', 'zz500_member')
    #fetcher.create_index_quote_indicators('open')
    #fetcher.create_index_quote_indicators('close')
    #fetcher.create_index_quote_indicators('high')
    #fetcher.create_index_quote_indicators('low')


if __name__ == '__main__':
    TushareFetch()