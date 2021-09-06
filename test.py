# encoding: utf-8
"""
阿尔法研究平台

Project: sustecher
Author: Moses
E-mail: 8342537@qq.com
"""
import os
import numpy as np
import pandas as pd

from jaqs.data import DataView
from jaqs.research import SignalDigger

from dataservice import DataService

datasrv = DataService()


def test_1():
    props = {'start_date': 20090501, 'end_date': 20210512, 'universe': None, 'benchmark': None}
    #props = {'start_date': 20150101, 'end_date': 20181231, 'universe': '000300.SH', 'benchmark': '000300.SH'}
    #props = {'start_date': 20171101, 'end_date': 20171231, 'universe': None, 'benchmark': None}
    dv = DataView(datasrv, props)
    dv.prepare_data()
    dv.set_index_member('000905.SH')
    dv.benchmark = '000300.SH'
    df = dv.benchmark
    dv.add_formula('not_index_member', '!index_member')
    dv.add_formula('limit_reached', 'Abs((open - Delay(close, 1)) / Delay(close, 1)) > 0.095')
    df = dv.get_ts('not_index_member', symbol='600519.SH,000005.SZ')
    df = dv.get_ts('limit_reached', symbol='600519.SH,000005.SZ')
    df = dv.get_ts('low', symbol='600519.SH,000005.SZ')
    df = dv.get_symbol(symbol='600519.SH', fields='open,low')
    df = dv.get_snapshot(20090105)
    dv.add_formula('in_', 'open / Delay(low, 1)', within_index=False)
    df = dv.get_ts('in_', symbol='600519.SH,000005.SZ')


def test_2():
    props = {'start_date': 20150101, 'end_date': 20181231, 'universe': '000300.SH', 'benchmark': '000300.SH'}
    dv = DataView(datasrv, props)
    dv.prepare_data()
    dv.add_formula('not_index_member', '!index_member') #不是指数成员都为1(真)
    dv.add_formula('limit_reached', 'Abs((open - Delay(close, 1)) / Delay(close, 1)) > 0.095') #涨停的都为1(真)
    trade_status = dv.get_ts('trade_status')
    mask_sus = (trade_status == 0) #停牌的都为真
    mask_index_member = dv.get_ts('not_index_member')
    mask_limit_reached = dv.get_ts('limit_reached')
    mask_all = np.logical_or(mask_sus, np.logical_or(mask_index_member, mask_limit_reached)) #为真的格子都忽略
    price = dv.get_ts('hfq_close')
    price_bench = dv.benchmark['close']
    dv.add_formula('open_jump', 'open / Delay(close, 1)')
    signal = dv.get_ts('open_jump')
    obj = SignalDigger(output_folder='output/test_2', output_format='pdf')
    obj.process_signal_before_analysis(signal,
                                       price=price,
                                       mask=mask_all,
                                       n_quantiles=5,
                                       period=3,
                                       benchmark_price=price_bench)
    res = obj.create_full_report()


def test_3():
    props = {'start_date': 20150101, 'end_date': 20181231, 'universe': '000300.SH', 'benchmark': '000300.SH'}
    dv = DataView(datasrv, props)
    dv.prepare_data()
    dv.add_formula('open_jump', 'open / Delay(close, 1)')
    signal = dv.get_ts('open_jump', symbol='600519.SH')
    price = dv.get_ts('hfq_close', symbol='600519.SH')
    obj = SignalDigger(output_folder='output/test_3', output_format='pdf')
    obj.create_single_signal_report(signal, price, [1, 5, 9, 21], 6, mask=None,
                                    trade_condition={'cond1': {'column': 'quantile',
                                                             'filter': lambda x: x > 3,
                                                             'hold': 5,
                                                             'direction': 1},
                                                   'cond2': {'column': 'quantile',
                                                             'filter': lambda x: x > 3,
                                                             'hold': 5,
                                                             'direction': -1},
                                                   'cond3': {'column': 'quantile',
                                                             'filter': lambda x: x > 4,
                                                             'hold': 9,
                                                             'direction': 1},
                                                     })


def test_4():
    props = {'start_date': 20150101, 'end_date': 20181231, 'universe': '000300.SH', 'benchmark': '000300.SH'}
    dv = DataView(datasrv, props)
    dv.prepare_data()
    dv.add_formula('not_index_member', '!index_member') #不是指数成员都为1(真)
    dv.add_formula('limit_reached', 'Abs((open - Delay(close, 1)) / Delay(close, 1)) > 0.095') #涨停的都为1(真)
    trade_status = dv.get_ts('trade_status')
    mask_sus = (trade_status == 0) #停牌的都为真
    mask_index_member = dv.get_ts('not_index_member')
    mask_limit_reached = dv.get_ts('limit_reached')
    mask_all = np.logical_or(mask_sus, np.logical_or(mask_index_member, mask_limit_reached)) #为真的格子都忽略
    price = dv.get_ts('hfq_close')
    price_bench = dv.benchmark['close']
    dv.add_formula('in_', '(Delay(index_member, 1) == 0) && (index_member > 0)')
    signal = dv.get_ts('in_').shift(1, axis=0)  # avoid look-ahead bias
    obj = SignalDigger(output_folder='output/test_4', output_format='pdf')
    obj.create_binary_event_report(signal, price, mask_all, price_bench, periods=[20, 40, 60, 120], group_by=None)


dataview_folder = 'datahouse/fastload'


def test_5():
    #props = {'start_date': 20090501, 'end_date': 20210512, 'universe': '000300.SH', 'benchmark': '000300.SH'}
    props = {'start_date': 20090501, 'end_date': 20210512, 'universe': None, 'benchmark': None}
    dv = DataView(datasrv, props)
    dv.prepare_data()
    #dv.add_formula('limit_reached', 'Abs((open - Delay(close, 1)) / Delay(close, 1)) > 0.095')
    dv.save_dataview(dataview_folder)


def test_6():
    dv = DataView()
    dv.load_dataview(dataview_folder)
    dv.source = datasrv
    dv.set_index_member('000905.SH')
    dv.benchmark = '000300.SH'
    dv.add_formula('not_index_member', '!index_member') #不是指数成员都为1(真)
    trade_status = dv.get_ts('trade_status')
    mask_sus = (trade_status == 0) #停牌的都为真
    mask_index_member = dv.get_ts('not_index_member')
    dv.add_formula('limit_reached', 'Abs((open - Delay(close, 1)) / Delay(close, 1)) > 0.095')
    mask_limit_reached = dv.get_ts('limit_reached')
    mask_all = np.logical_or(mask_sus, np.logical_or(mask_index_member, mask_limit_reached)) #为真的格子都忽略
    price = dv.get_ts('hfq_close')
    price_bench = dv.benchmark['close']
    dv.add_formula('in_', '(Delay(index_member, 1) == 0) && (index_member > 0)')
    signal = dv.get_ts('in_').shift(1, axis=0)  # avoid look-ahead bias
    obj = SignalDigger(output_folder='output/test_6', output_format='pdf')
    obj.create_binary_event_report(signal, price, mask_all, price_bench, periods=[1, 3, 5, 7], group_by=None)


def test_7():
    dv = DataView()
    dv.load_dataview(dataview_folder)
    dv.source = datasrv    
    dv.set_index_member('000300.SH')
    dv.add_formula('in_', '(Delay(index_member, 1) == 0) && (index_member > 0)')
    signal = dv.get_ts('in_').shift(1, axis=0)  # avoid look-ahead bias
    signal.to_csv(os.path.join('output/test_7', 'sample.csv'), encoding='gbk')


if __name__ == "__main__":
    #test_7()
    #test_6()
    #test_5()
    #test_4()
    #test_3()
    #test_2()
    test_1()