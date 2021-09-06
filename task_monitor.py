# encoding: utf-8
"""
阿尔法研究平台

Project: sustecher
Author: Moses
E-mail: 8342537@qq.com
"""
import os
import time
import numpy as np
import pandas as pd
import pymongo
from retrying import retry
from urllib.parse import quote_plus


TASK_INST = os.path.join(os.path.dirname(__file__), "task_inst.py")


#连接mongodb数据库
uri = "mongodb://{username}:{password}@{host}:{port}/{dbname}".format(username=quote_plus("root"),
                                                                      password=quote_plus("123456"),
                                                                      host=quote_plus("localhost"),
                                                                      port=27017,
                                                                      dbname="admin")
client = pymongo.MongoClient(uri)
researcher_dbs = {}
for i in range(20):
    name = "researcher{:03d}".format(i+1)
    researcher_dbs[name] = client[name] #每一个研究员对应一个数据库


def launch_task(dbname, oid):
    """ 运行实际跑研究工作的进程
    """
    cmd = 'start python ' + TASK_INST + ' ' + dbname + ' ' + oid
    print(cmd)
    val = os.system(cmd)
    print(val)


def retry_if_auto_reconnect_error(exception):
    """Return True if we should retry (in this case when it's an AutoReconnect), False otherwise"""
    return isinstance(exception, pymongo.errors.AutoReconnect)


@retry(retry_on_exception=retry_if_auto_reconnect_error, stop_max_attempt_number=10)
def monitor_db(name, db):
    """ 监控单个数据库
    一个研究员对应一个数据库,每个数据库里面包含一个task表,这个表里面每一条记录代表相应研究员提交的研究任务
    """
    print(name)
    records = [r for r in db.task.find()] #读取task表里面所有记录
    if len(records) == 0:
        return
    df = pd.DataFrame(records)
    workcount = 0
    if 'state' in df.columns:
        workcount = (df['state']=='working').sum()
    for r in records: #遍历某个研究员提交的所有任务
        if workcount > 2:
            break #一个研究员同时最多并发执行三个研究任务
        print(r)
        oid = r["_id"]
        state = r.get("state", None) #任务状态
        '''
        state:
        字段不存在: 新添加任务待执行
        'working': 任务正在进行中
        'error': 任务发生错误,停止
        'finish': 任务已经正常完成
        '''
        if state is None: #如果是待执行的任务就执行它
            db.task.update_one({"_id":oid}, {"$set":{"state":"working"}}) #更新任务状态
            launch_task(name, str(oid)) #创建执行研究工作的任务进程
            workcount = workcount+1 #并发任务数加一


def monitor_all():
    """ 监控所有数据库
    一个研究员对应一个数据库,每个数据库里面包含一个task表,这个表里面每一条记录代表相应研究员提交的研究任务
    """
    for (name, db) in researcher_dbs.items():
        monitor_db(name, db)


if __name__ == "__main__":
    while True:
        monitor_all()
        time.sleep(1) #每秒执行一次