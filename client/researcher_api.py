# encoding: utf-8
"""
阿尔法研究平台

Project: sustecher
Author: Moses
E-mail: 8342537@qq.com
"""
import configparser
import pyarrow
import pyarrow.flight
import pymongo
from retrying import retry


#读取研究员预设配置
config = configparser.ConfigParser()
config.read("./researcher_config.ini")
researcher_cfg = config["researcher_cfg"]

#建立中台数据库连接
client = pymongo.MongoClient(host=researcher_cfg["host"], port=researcher_cfg.getint("port"))
researcher_db = client[researcher_cfg["researcher_id"]]


def retry_if_auto_reconnect_error(exception):
    """Return True if we should retry (in this case when it's an AutoReconnect), False otherwise"""
    return isinstance(exception, pymongo.errors.AutoReconnect)


@retry(retry_on_exception=retry_if_auto_reconnect_error, stop_max_attempt_number=10)
def auth():
    """ 远程数据库登录
    """
    researcher_db.authenticate(name=researcher_cfg["researcher_id"], password=researcher_cfg["password"], source=researcher_cfg["researcher_id"])


auth() #远程数据库登录


def write_to_datahouse(dat_val, dat_name):
    """ 因子数据上传
    """
    uri = "grpc+tcp://{}:5005".format(researcher_cfg["host"])
    client = pyarrow.flight.FlightClient(uri)
    token_pair = client.authenticate_basic_token(researcher_cfg["researcher_id"], researcher_cfg["password"])
    while True:
        try:
            action = pyarrow.flight.Action("healthcheck", b"")
            options = pyarrow.flight.FlightCallOptions(headers=[token_pair], timeout=3)
            list(client.do_action(action, options=options))
            break
        except pyarrow.ArrowIOError as e:
            if "Deadline" in str(e):
                print("Server is not ready, waiting...")
    my_table = pyarrow.Table.from_pandas(dat_val)
    print('Table rows=', str(len(my_table)))
    options = pyarrow.flight.FlightCallOptions(headers=[token_pair])
    writer, _ = client.do_put(pyarrow.flight.FlightDescriptor.for_path(dat_name), my_table.schema, options)
    writer.write_table(my_table)
    writer.close()


@retry(retry_on_exception=retry_if_auto_reconnect_error, stop_max_attempt_number=10)
def submit_task(request):
    """ 任务提交
    """
    researcher_db.task.insert_one(request)


def cleanup():
    """ 资源释放
    """
    client.close()


__all__ = ['write_to_datahouse', 'submit_task', 'cleanup']