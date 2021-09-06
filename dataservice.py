# -*- coding: utf-8 -*-
"""
阿尔法研究平台

Project: sustecher
Author: Moses
E-mail: 8342537@qq.com
"""
import os
import warnings
import numpy as np
import pandas as pd
warnings.filterwarnings('ignore')

WORK_PATH = os.path.join(os.path.dirname(__file__), "raw_data")


class FileAlreadyExistError(Exception):
    pass


class lazyproperty:
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, cls):
        if instance is None:
            return self
        else:
            value = self.func(instance)
            setattr(instance, self.func.__name__, value)
            return value


class Data:

    root = WORK_PATH
    metafile = 'all_stocks.xlsx'
    tradedays_file = 'tradedays.xlsx'
    fieldmap = {}

    def __init__(self, base_dir="stock"):
        self._base_dir = base_dir
        self.__update_fieldmap()

    def __update_fieldmap(self):
        path = os.path.join(self.root, self._base_dir)
        self.fieldmap.update({name.split(".")[0]: path for name in os.listdir(path)})

    def open_file(self, name):
        if name == 'meta':
            return pd.read_excel(os.path.join(self.root, self._base_dir, 'src', self.metafile), index_col=[0], parse_dates=['ipo_date', "delist_date"], encoding='gbk')
        elif name == 'tradedays':
            return pd.read_excel(os.path.join(self.root, self._base_dir, 'src', self.tradedays_file), index_col=[0], parse_dates=True, encoding='gbk').index.tolist()
        path = self.fieldmap.get(name, None)
        if path is None:
            raise Exception(f'{name} is unrecognisable or not in file dir, please check and retry.')
        try:
            dat = pd.read_csv(os.path.join(path, name+'.csv'), index_col=[0], engine='python', encoding='gbk')
            dat = pd.DataFrame(data=dat, index=dat.index.union(self.meta.index), columns=dat.columns)
        except TypeError:
            print(name, path)
            raise
        dat.columns = pd.to_datetime(dat.columns)
        pdt = dat.columns.to_pydatetime()
        sdt = np.vectorize(lambda s: s.strftime('%Y%m%d'))(pdt)
        idt = sdt.astype('i4')
        dat.columns = idt
        return dat

    def close_file(self, df, name, **kwargs):
        if name == 'meta':
            df.to_excel(os.path.join(self.root, self._base_dir, 'src', self.metafile), encoding='gbk', **kwargs)
        elif name == 'tradedays':
            df.to_excel(os.path.join(self.root, self._base_dir, 'src', self.tradedays_file), encoding='gbk', **kwargs)
        else:
            path = self.fieldmap.get(name, None)
            if path is None:
                path = os.path.join(self.root, self._base_dir)
            df.to_csv(os.path.join(path, name+'.csv'), encoding='gbk', **kwargs)
            self.__update_fieldmap()

    def __getattr__(self, name):
        return self.open_file(name)


class DataService:

    def __init__(self):
        self.data = Data()

    def __getattr__(self, name):
        return getattr(self.data, name, None)

    def _get_trade_days(self, startday, endday, freq=None):
        if freq is None:
            freq = self.freq
        startday, endday = pd.to_datetime((startday, endday))
        if freq == 'd':
            try:
                start_idx = self._get_date_idx(startday, self.tradedays)
            except IndexError:
                return []
            else:
                try:
                    end_idx = self._get_date_idx(endday, self.tradedays)
                except IndexError:
                    return self.tradedays[start_idx:]
                else:
                    return self.tradedays[start_idx:end_idx+1]
        else:
            new_cdays_curfreq = pd.Series(index=self.tradedays).resample(freq).asfreq().index
            c_to_t_dict = {cday:tday for tday, cday in self.month_map.to_dict().items()}
            try:
                new_tdays_curfreq = [c_to_t_dict[cday] for cday in new_cdays_curfreq]
            except KeyError:
                new_tdays_curfreq = [c_to_t_dict[cday] for cday in new_cdays_curfreq[:-1]]
            start_idx = self._get_date_idx(c_to_t_dict.get(startday, startday), new_tdays_curfreq) + 1
            try:
                end_idx = self._get_date_idx(c_to_t_dict.get(endday, endday), new_tdays_curfreq)
            except IndexError:
                end_idx = len(new_tdays_curfreq) - 1
            return new_tdays_curfreq[start_idx:end_idx+1]

    @lazyproperty
    def trade_days(self):
        self.__trade_days = self._get_trade_days(self.startday, self.endday)
        return self.__trade_days

    def _get_daily_data(self, name, stocks, date, offset, datelist=None):
        dat = getattr(self, name, None)
        if dat is None:
            raise AttributeError("{} object has no attr: {}".format(self.__class__.__name__, name))

        dat = dat.loc[stocks, :].T
        if datelist is None:
            datelist = dat.index.tolist()
        idx = self._get_date_idx(date, datelist)
        start_idx, end_idx = max(idx-offset+1, 0), idx+1
        date_period = datelist[start_idx:end_idx]
        dat = dat.loc[date_period, :]
        return dat

    def _get_date_idx(self, date, datelist=None, ensurein=False):
        msg = """Date {} not in current tradedays list. If this date value is certainly a tradeday,  
              please reset tradedays list with longer periods or higher frequency."""
        date = pd.to_datetime(date)
        if datelist is None:
            datelist = self.trade_days
        try:
            datelist = sorted(datelist)
            idx = datelist.index(date)
        except ValueError:
            if ensurein:
                raise IndexError(msg.format(str(date)[:10]))
            dlist = list(datelist)
            dlist.append(date)
            dlist.sort()
            idx = dlist.index(date) 
            if idx == len(dlist)-1:
                raise IndexError(msg.format(str(date)[:10]))
            return idx - 1
        return idx

    def _get_date(self, date, offset=0, datelist=None):
        if datelist is None:
            datelist = self.trade_days
        try:
            idx = self._get_date_idx(date, datelist)
        except IndexError as e:
            print(e)
            idx = len(datelist) - 1
        finally:
            return datelist[idx+offset]

  