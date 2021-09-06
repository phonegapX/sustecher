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
import researcher_api as ra


if __name__ == "__main__":
    #读取样例信号数据
    factor_df = pd.read_csv(os.path.join('sample.csv'), index_col=[0], engine='python', encoding='gbk')
    #上传因子数据
    ra.write_to_datahouse(dat_val=factor_df, dat_name="requests3")

    requests1 =  {
        "task": {
            "type": "factor_evaluate_report",  #任务类型:因子检验
            "start_date": 20150101,            #开始时间
            "end_date": 20181231,              #结束时间
            "universe": '000300.SH',           #选股空间
            "benchmark": '000300.SH',          #对比基准
            "signal_name": "requests1",        #因子名称(如果下面的公式字段不存在就直接从文件中读取)
            "formula": 'Abs((open - Delay(close, 1)) / Delay(close, 1))', #因子(信号)计算公式
            "quantiles": 5,                    #分为几组
            "period": 22                       #计算收益周期
        }
    }

    requests2 =  {
        "task": {
            "type": "binary_event_report", #任务类型:事件研究
            "start_date": 20150101,        #开始时间
            "end_date": 20181231,          #结束时间
            "universe": '000300.SH',       #选股空间
            "benchmark": '000300.SH',      #对比基准
            "signal_name": "requests2",    #事件名称(如果下面的公式字段不存在就直接从文件中读取)
            "formula": '(Delay(index_member, 1) == 0) && (index_member > 0)', #因子(信号)计算公式
            "periods": [20, 40, 60, 120]   #计算收益周期
        }
    }

    requests3 =  {
        "task": {
            "type": "single_signal_report", #任务类型:时序信号研究
            "start_date": 20150101,        #开始时间
            "end_date": 20181231,          #结束时间
            "universe": None,              #选股空间,必须为空
            "benchmark": None,             #对比基准,必须为空
            "symbol": "000001.SZ",         #研究标的符号
            "signal_name": "requests3",    #信号名称(如果下面的公式字段不存在就直接从文件中读取)
            "formula": 'Abs((open - Delay(close, 1)) / Delay(close, 1))', #因子(信号)计算公式
            "quantiles": 5,                #分为几组
            "periods": [20, 40, 60, 120]   #计算收益周期
        }
    }

    ra.submit_task(requests3) #提交任务

    ra.cleanup() #释放资源
