# encoding: utf-8
"""
阿尔法研究平台

Project: sustecher
Author: Moses
E-mail: 8342537@qq.com
"""
import os
import sys
import pymongo
import numpy as np
import pandas as pd
from urllib.parse import quote_plus
from bson.objectid import ObjectId

from dataservice import DataService
from jaqs.data import DataView
from jaqs.research import SignalDigger


dataview_folder = 'datahouse/fastload'


def _check_and_align_idx_col(base, target):
    """ 把target在行和列上都必须和base对齐
    """
    if target.index.dtype == 'datetime64[ns]': #需要的话就转换类型,我们需要20200202这样int型
        pdt = target.index.to_pydatetime()
        sdt = np.vectorize(lambda s: s.strftime('%Y%m%d'))(pdt)
        idt = sdt.astype('i4')
        target.index = idt

    df = pd.DataFrame(index=base.index, columns=base.columns)
    df.loc[:,:] = target
    return df


def task_factor_evaluate_report(researcher_id, task):
    """ 因子评估任务
    """
    work_dir = os.path.join("datahouse", researcher_id) #数据存放目录

    start_date = task["start_date"]     #开始时间
    end_date = task["end_date"]         #结束时间
    universe = task["universe"]         #选股空间
    benchmark = task["benchmark"]       #对比基准
    quantiles = task["quantiles"]       #分为几组
    period = task["period"]             #计算收益周期
    signal_name = task["signal_name"]   #因子名称(如果下面的公式字段不存在就直接从文件中读取)
    formula = task.get("formula", None) #因子(信号)计算公式

    datasrv = DataService()
    dv = DataView()
    dv.load_dataview(dataview_folder)
    dv.source = datasrv
    dv.set_index_member(universe)
    dv.benchmark = benchmark
    price = dv.get_ts('hfq_close', start_date=start_date, end_date=end_date)

    if formula: #公式字段存在就按公式计算,不然就直接读取相应文件
        dv.add_formula(signal_name, formula)
        signal = dv.get_ts(signal_name, start_date=start_date, end_date=end_date)
    else:
        signal = pd.read_csv(os.path.join(work_dir, signal_name+'.csv'), index_col=[0], engine='python', encoding='gbk')
        #要对signal格式进行检验,行是时间,列是股票,时间有可能是DatetimeIndex格式,要转换成类似20200202这样的int型
        #另外就是(股票)列需要和基础数据一致对齐
        signal = _check_and_align_idx_col(price, signal)

    dv.add_formula('not_index_member', '!index_member') #不是指数成员都为1(真)
    dv.add_formula('limit_reached', 'Abs((open - Delay(close, 1)) / Delay(close, 1)) > 0.095')
    trade_status = dv.get_ts('trade_status', start_date=start_date, end_date=end_date)
    mask_sus = (trade_status == 0) #停牌的都为真
    mask_index_member = dv.get_ts('not_index_member', start_date=start_date, end_date=end_date)
    mask_limit_reached = dv.get_ts('limit_reached', start_date=start_date, end_date=end_date)
    mask_all = np.logical_or(mask_sus, np.logical_or(mask_index_member, mask_limit_reached)) #为真的格子都忽略
    price_bench = dv.benchmark['close']
    obj = SignalDigger(output_folder=os.path.join(work_dir, 'output', signal_name), output_format='pdf')
    obj.process_signal_before_analysis(signal, price=price, mask=mask_all, n_quantiles=quantiles, period=period, benchmark_price=price_bench)
    obj.create_full_report()


def task_binary_event_report(researcher_id, task):
    """ 事件研究任务
    """
    work_dir = os.path.join("datahouse", researcher_id) #数据存放目录

    start_date = task["start_date"]     #开始时间
    end_date = task["end_date"]         #结束时间
    universe = task["universe"]         #选股空间
    benchmark = task["benchmark"]       #对比基准
    periods = task["periods"]           #计算收益周期
    signal_name = task["signal_name"]   #因子名称(如果下面的公式字段不存在就直接从文件中读取)
    formula = task.get("formula", None) #因子(信号)计算公式

    datasrv = DataService()
    dv = DataView()
    dv.load_dataview(dataview_folder)
    dv.source = datasrv
    dv.set_index_member(universe)
    dv.benchmark = benchmark
    price = dv.get_ts('hfq_close', start_date=start_date, end_date=end_date)

    if formula: #公式字段存在就按公式计算,不然就直接读取相应文件
        dv.add_formula(signal_name, formula)
        signal = dv.get_ts(signal_name, start_date=start_date, end_date=end_date)
    else:
        signal = pd.read_csv(os.path.join(work_dir, signal_name+'.csv'), index_col=[0], engine='python', encoding='gbk')
        #要对signal格式进行检验,行是时间,列是股票,时间有可能是DatetimeIndex格式,要转换成类似20200202这样的int型
        #另外就是(股票)列需要和基础数据一致对齐
        signal = _check_and_align_idx_col(price, signal)

    dv.add_formula('not_index_member', '!index_member') #不是指数成员都为1(真)
    dv.add_formula('limit_reached', 'Abs((open - Delay(close, 1)) / Delay(close, 1)) > 0.095')
    trade_status = dv.get_ts('trade_status', start_date=start_date, end_date=end_date)
    mask_sus = (trade_status == 0) #停牌的都为真
    mask_index_member = dv.get_ts('not_index_member', start_date=start_date, end_date=end_date)
    mask_limit_reached = dv.get_ts('limit_reached', start_date=start_date, end_date=end_date)
    mask_all = np.logical_or(mask_sus, np.logical_or(mask_index_member, mask_limit_reached)) #为真的格子都忽略
    price_bench = dv.benchmark['close']
    obj = SignalDigger(output_folder=os.path.join(work_dir, 'output', signal_name), output_format='pdf')
    obj.create_binary_event_report(signal, price, mask_all, price_bench, periods=periods, group_by=None)


def task_single_signal_report(researcher_id, task):
    """ 单标的CTA信号时序研究
    """
    work_dir = os.path.join("datahouse", researcher_id) #数据存放目录

    start_date = task["start_date"]     #开始时间
    end_date = task["end_date"]         #结束时间
    symbol = task["symbol"]             #研究标的符号
    quantiles = task["quantiles"]       #分为几组
    periods = task["periods"]           #计算收益周期
    signal_name = task["signal_name"]   #因子名称(如果下面的公式字段不存在就直接从文件中读取)
    formula = task.get("formula", None) #因子(信号)计算公式

    dv = DataView()
    dv.load_dataview(dataview_folder)
    price = dv.get_ts('hfq_close', symbol=symbol, start_date=start_date, end_date=end_date)

    if formula: #公式字段存在就按公式计算,不然就直接读取相应文件
        dv.add_formula(signal_name, formula)
        signal = dv.get_ts(signal_name, symbol=symbol, start_date=start_date, end_date=end_date)
    else:
        signal = pd.read_csv(os.path.join(work_dir, signal_name+'.csv'), index_col=[0], engine='python', encoding='gbk')
        #要对signal格式进行检验,行是时间,列是股票,时间有可能是DatetimeIndex格式,要转换成类似20200202这样的int型
        #另外就是(股票)列需要和基础数据一致对齐
        signal = _check_and_align_idx_col(price, signal)

    obj = SignalDigger(output_folder=os.path.join(work_dir, 'output', signal_name), output_format='pdf')
    obj.create_single_signal_report(signal, price, periods, quantiles, mask=None, trade_condition=None)


def main():
    if len(sys.argv) != 3: #只能接受两个参数
        return
    dbname = str(sys.argv[1])   #数据库名(即研究者ID)
    oid = ObjectId(sys.argv[2]) #task表中某条记录ID
    uri = "mongodb://{username}:{password}@{host}:{port}/{dbname}".format(username=quote_plus("root"),
                                                                          password=quote_plus("123456"),
                                                                          host=quote_plus("localhost"),
                                                                          port=27017,
                                                                          dbname="admin")
    client = pymongo.MongoClient(uri)
    db = client[dbname]
    try:
        r = db.task.find_one({"_id":oid})
        task = r['task']
        if task['type'] == "factor_evaluate_report":
            task_factor_evaluate_report(dbname, task) #因子评估任务
        elif task['type'] == "binary_event_report":
            task_binary_event_report(dbname, task) #事件研究任务
        elif task['type'] == "single_signal_report":
            task_single_signal_report(dbname, task) #单标的CTA信号时序研究
        else:
            raise Exception('type {} is unrecognisable'.format(task['type']))
    except Exception as err:
        db.task.update_one({"_id":oid}, {"$set":{"state":"error","errtxt":str(err)}}) #更新任务状态
    else:
        db.task.update_one({"_id":oid}, {"$set":{"state":"finish"}}) #更新任务状态


if __name__ == "__main__":
    main()
