# -*- coding: utf-8 -*-
import logging
import inspect
import os
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from apscheduler.schedulers.background import BackgroundScheduler
import cx_Oracle
import pymssql
import pandas as pd
import numpy as np
import re
import traceback
from sqlalchemy import create_engine
import calendar

# import matplotlib.pyplot as plt

pd.set_option('display.max_columns', None)


# # 全局变量
# GLOBAL_Logger = None
# GLOBAL_WellNameReference = {}


# 日志函数
# 返回日志文件
def setlog():
    global GLOBAL_Logger

    logger = logging.getLogger('logger')
    logger.setLevel(logging.INFO)

    this_file = inspect.getfile(inspect.currentframe())
    dirpath = os.path.abspath(os.path.dirname(this_file))

    # create a file handlerINFO
    handler = logging.FileHandler(os.path.join(dirpath, 'dataserver.log'))
    handler.setLevel(logging.INFO)
    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)

    GLOBAL_Logger = logger


# 数据库函数
# 数据库连接初始化
def initdatacon():
    global GlOBAL_oracle_engine
    global GlOBAL_mssql_read_engine
    global GlOBAL_mssql_write_engine

    # 采油厂生产数据关系库
    SqlserverDataServre = {"host": '10.16.192.40',
                           "user": 'zhangdkl',
                           "password": '4687607',
                           "database": 'cyec',
                           "charset": 'utf8',
                           "servertype": 'sqlserver'}
    # 采油厂生产数据实时关系库
    dsn_tns = cx_Oracle.makedsn('10.16.192.49', 1521, 'ORCL')
    sensor_oracle_data_servre = {"user": 'sssjzc',
                                 "password": 'sssjzc',
                                 "dsn_tns": dsn_tns,
                                 "servertype": 'oracle'}
    # 分公司生产数据池发布数据库
    dsn_tns1 = cx_Oracle.makedsn('10.16.3.34', 1521, service_name='kfdb')
    daily_oracle_data_servre = {"user": 'YJYJY',
                                "password": 'JYadmin#$2021',
                                "dsn_tns": dsn_tns1,
                                "servertype": 'oracle'}

    # 采油厂实时生产数据库连接
    GlOBAL_oracle_engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (
        sensor_oracle_data_servre["user"], sensor_oracle_data_servre["password"], sensor_oracle_data_servre["dsn_tns"]))
    # 采油厂动态生产数据库连接
    GlOBAL_mssql_read_engine = create_engine('mssql+pymssql://%s:%s@%s/%s' % (
        SqlserverDataServre["user"], SqlserverDataServre["password"], SqlserverDataServre["host"],
        SqlserverDataServre["database"]), connect_args={'charset': 'GBK'})
    # 采油厂动态生产数据库连接
    GlOBAL_mssql_write_engine = create_engine('mssql+pymssql://%s:%s@%s/%s' % (
        SqlserverDataServre["user"], SqlserverDataServre["password"], SqlserverDataServre["host"],
        SqlserverDataServre["database"]))


# 获取数据库链接
def getConnection(server):
    type = server["servertype"]
    if type == "sqlserver":
        host = server["host"]
        user = server["user"]
        password = server["password"]
        database = server["database"]
        charset = server["charset"]
        connection = pymssql.connect(host, user, password, database, charset)
    else:
        user = server["user"]
        password = server["password"]
        dsn_tns = server["dsn_tns"]
        connection = cx_Oracle.connect(user, password, dsn_tns)

    return connection


# 从数据库查询数据函数
def getdatasql(server, sql):
    conn = getConnection(server)
    cursor = conn.cursor()
    cursor.execute(sql)
    data = list(cursor.fetchall())
    cursor.close()
    conn.close()
    return data


# 执行SQL语句函数
def executesql(server, sql):
    conn = getConnection(server)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(sql)
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())
    cursor.close()
    conn.close()


# 往数据库插入数据函数
def insertdata(server, tablename, fields, values):
    tablename = tablename
    StrFieds = ""
    StrValues = ""

    for field in fields:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field

    for value in values:
        if StrValues == "":
            if str(value) == "NULL":
                StrValues = str(value)
            else:
                StrValues = "'" + str(value) + "'"
        else:
            if str(value) == "NULL":
                StrValues = StrValues + ", " + str(value)
            else:
                StrValues = StrValues + ", '" + str(value) + "'"

    insert_data_sql = "INSERT INTO " + tablename + " (" + StrFieds + ") values(" + StrValues + ")"

    executesql(server, insert_data_sql)


# 日期转化函数
# 返回目前日期
def getnowtime():
    NowDate = time.strftime("%Y-%m-%d %H:%M:%S", datetime.fromtimestamp(time.time()).timetuple())
    return NowDate


# 返回间隔前时间
def getbegintime(durtime, unit="days"):
    NowTime = time.time()

    if unit == "days":
        DateBeginDay = datetime.fromtimestamp(NowTime) - relativedelta(days=durtime)
    elif unit == "minutes":
        DateBeginDay = datetime.fromtimestamp(NowTime) - relativedelta(minutes=durtime)
    else:
        DateBeginDay = getnowtime()

    StrBeginDay = time.strftime("%Y-%m-%d %H:%M:%S", DateBeginDay.timetuple())
    return StrBeginDay


# 返回间隔前日期
def getstartdate(durtime):
    NowTime = time.time()
    DateBeginDay = datetime.fromtimestamp(NowTime) - relativedelta(days=durtime)
    StrBeginDay = time.strftime("%Y-%m-%d 00:00:00", DateBeginDay.timetuple())
    return StrBeginDay


# 获取正式井号
def getofficialwellnames():
    global GLOBAL_OfficiaWellNames

    tablename = "ecsjb"
    date = time.strftime("%Y-%m-%d", (datetime.fromtimestamp(time.time()) - relativedelta(days=3)).timetuple())
    welldata_sql = "SELECT DISTINCT JH FROM %s WHERE RQ>='%s'" % (tablename, date)
    wellname = pd.read_sql(welldata_sql, GlOBAL_mssql_read_engine)
    # wellname = [x[0].encode('latin1').decode('gbk') for x in wellname.values]
    wellname = [x[0] for x in wellname.values]

    GLOBAL_OfficiaWellNames = wellname


# 获取井号对照表
def getwellnamereference():
    global GLOBAL_WellNameReference

    begintime = getbegintime(5, "minutes")
    tablename = "ssc_yj_ss"

    def check_contain_chinese(check_str):
        if u'\u4e00' <= check_str[0] <= u'\u9fff':
            return True
        else:
            return False

    get_data_sql = "SELECT %s FROM %s WHERE %s>TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" \
                   % ("JH", tablename, "CJSJ", begintime)
    realtimedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine)
    aliasname = [x[0] for x in list(realtimedata.values)]
    aliasname = list(set(aliasname))

    namereference = {}

    for wellname in GLOBAL_OfficiaWellNames:
        if check_contain_chinese(wellname):
            namereference[wellname] = wellname
        else:
            m = re.match(r'[A-Za-z]+\d+-*\d*', wellname)
            tempwellname = m.group()
            matchstr = r"(" + tempwellname + ")$|(" + tempwellname + ")[A-Za-z]|(" + tempwellname + ")[\u4e00-\u9fa5]"
            alias = ""
            for name in aliasname:
                if re.match(matchstr, name):
                    alias = name
                    aliasname.remove(name)
                    break
            if alias:
                namereference[wellname] = alias
            else:
                namereference[wellname] = ""

    GLOBAL_WellNameReference = namereference


# 初始化基础参数
def initconfig():
    initdatacon()

    setlog()

    getofficialwellnames()
    # getwellnamereference()


# 提取二厂单井曲线数据
def extract_erchang_welldynicurve_data():
    global GLOBAL_Logger

    # GLOBAL_Logger.info("开始提取二厂单井曲线数据！")
    print("......................................................")
    print(getnowtime(), "开始提取二厂单井曲线数据!")

    extractdays = 3
    try:
        for exday in range(1, extractdays):
            # if True:
            #     exday = 4216
            extracterchangwelldynicurvedata(exday)
            # print("前%s日二厂单井曲线数据提取完毕！" % str(exday + 1))
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 开始提取二厂单井曲线数据
def extracterchangwelldynicurvedata(exday):
    begintime = getstartdate(exday)

    print(begintime)

    SqlserverDataServre = {"host": '10.16.192.40',
                           "user": 'zhangdkl',
                           "password": '4687607',
                           "database": 'cyec',
                           "charset": 'utf8',
                           "servertype": 'sqlserver'}


    # print(begintime)

    # 生产日报
    StrFieds = ""
    tablename = "ecsjb"
    GetField = ["RQ", "JH", "DWMC", "CYQ", "SCSJ", "YZ", "PL1", "BJ", "BJ1", "BS", "PL", "CC", "CC1", "YY",
                "TY", "HY", "JKWD", "HHYL", "RCYL1", "RCYL", "RCSL", "RCXL", "HHHS", "HS", "DY", "DL",
                "SXDL", "XXDL", "RZSL", "RZQL", "RBSL", "BZ", "DYFS", "CXFS", "SCFS", "ISCX", "JB", "SCZT", "ZYLX"]
    FieldName = ["RQ", "JH", "DWMC", "CYQ", "SCSJ", "YZ", "PL1", "BJ", "BJ1", "BS", "PL", "CC", "CC1", "YY",
                 "TY", "HY", "JKWD", "HHYL", "RCYL1", "RCYL", "RCSL", "RCXL", "HHHS", "HS", "DY", "DL",
                 "SXDL", "XXDL", "RZSL", "RZQL", "RBSL", "BZ", "DYFS", "CXFS", "SCFS", "ISCX", "JB", "SCZT", "ZYLX"]

    # GetField = ["RQ", "JH", "DWMC", "CYQ"]
    # FieldName = ["RQ", "JH", "DWMC", "CYQ"]

    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = '%s'" % (StrFieds, tablename, "RQ", begintime)
    # dynamicdata = getdatasql(SqlserverDataServre, get_data_sql)
    dynamicdata = pd.read_sql(get_data_sql, GlOBAL_mssql_read_engine)
    dynamicdata = pd.DataFrame(dynamicdata, columns=FieldName)
    dynamicdata = dynamicdata.set_index(["JH"])
    dynamicdata["XCB"] = dynamicdata["RCXL"] / dynamicdata["RCYL"]
    dynamicdata["BX"] = dynamicdata["HHYL"] / dynamicdata["PL"] * 100
    # print(dynamicdata.loc[:, ["DYFS", "CXFS", "SCFS", "ISCX", "JB", "SCZT", "ZYLX"]])

    # 液面压力
    StrFieds = ""
    tablename = "EcWellTest"
    GetField = ["wellname", "date", "dynamicfluidlevel", "staticfluidlevel", "dynamicpressure", "staticpressure"]
    FieldName = ["JH", "RQ", "DYM", "JYM", "DTYL", "JTYL"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = '%s'" % (StrFieds, tablename, "date", begintime)
    # enleveldata = getdatasql(SqlserverDataServre, get_data_sql)
    enleveldata = pd.read_sql(get_data_sql, GlOBAL_mssql_read_engine)
    enleveldata = pd.DataFrame(enleveldata, columns=FieldName)
    enleveldata.drop_duplicates(subset=["JH"], keep='last', inplace=True)
    enleveldata = enleveldata.set_index(["JH"])
    # print(enleveldata)

    # 计量
    StrFieds = ""
    tablename = "EcWellMeteringReport"
    GetField = ["wellname", "date", "value", "method"]
    FieldName = ["JH", "RQ", "JL", "JLFS"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = '%s'" % (StrFieds, tablename, "date", begintime)
    # meteringdata = getdatasql(SqlserverDataServre, get_data_sql)
    meteringdata = pd.read_sql(get_data_sql, GlOBAL_mssql_read_engine)

    meteringdata = pd.DataFrame(meteringdata, columns=FieldName)
    meteringdata.sort_values(by="JL", inplace=True)
    meteringdata.drop_duplicates(subset=["JH"], keep='last', inplace=True)
    meteringdata = meteringdata.set_index(["JH"])
    # print(meteringdata)

    # 粘温
    StrFieds = ""
    tablename = "EcWellViscosity"
    GetField = ["wellname", "testdate", "viscositywell"]
    FieldName = ["JH", "RQ", "NV"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = '%s'" % (StrFieds, tablename, "testdate", begintime)
    # viscositydata = getdatasql(SqlserverDataServre, get_data_sql)
    viscositydata = pd.read_sql(get_data_sql, GlOBAL_mssql_read_engine)
    viscositydata = pd.DataFrame(viscositydata, columns=FieldName)
    # print(viscositydata)
    viscositydata.sort_values(by="NV", inplace=True)
    viscositydata.drop_duplicates(subset=["JH"], keep='last', inplace=True)
    viscositydata = viscositydata.set_index(["JH"])
    # print(viscositydata)
    # print(viscositydata)

    # 化验含水
    StrFieds = ""
    tablename = "EcWellMoisture"
    GetField = ["wellname", "testdate", "moistureavg"]
    FieldName = ["JH", "RQ", "HYHS"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = '%s'" % (StrFieds, tablename, "testdate", begintime)
    # moisturedata = getdatasql(SqlserverDataServre, get_data_sql)
    moisturedata = pd.read_sql(get_data_sql, GlOBAL_mssql_read_engine)
    moisturedata = pd.DataFrame(moisturedata, columns=FieldName)
    moisturedata.sort_values(by="HYHS", inplace=True)
    moisturedata.drop_duplicates(subset=["JH"], keep='last', inplace=True)
    moisturedata = moisturedata.set_index(["JH"])
    # print(moisturedata)

    # 密度
    StrFieds = ""
    tablename = "EcWellDensity"
    GetField = ["wellname", "testdate", "standarddensity"]
    FieldName = ["JH", "RQ", "MD"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = '%s'" % (StrFieds, tablename, "testdate", begintime)
    # densitydata = getdatasql(SqlserverDataServre, get_data_sql)
    densitydata = pd.DataFrame(moisturedata, columns=FieldName)
    densitydata = pd.DataFrame(densitydata, columns=FieldName)
    densitydata.sort_values(by="MD", inplace=True)
    densitydata.drop_duplicates(subset=["JH"], keep='last', inplace=True)
    densitydata = densitydata.set_index(["JH"])
    # print(densitydata)

    filenames = ["DYM", "JYM", "DTYL", "JTYL"]
    for col in filenames:
        dynamicdata[col] = enleveldata[col]

    filenames = ["JL", "JLFS"]
    for col in filenames:
        dynamicdata[col] = meteringdata[col]

    dynamicdata["NV"] = viscositydata["NV"]
    dynamicdata["HYHS"] = moisturedata["HYHS"]
    dynamicdata["MD"] = densitydata["MD"]

    dynamicdata.dropna(subset=["RQ"], inplace=True)
    dynamicdata.replace([np.inf, -np.inf], np.nan, inplace=True)
    # print(dynamicdata.loc[:, ["RQ", "MD"]])

    # 删除已有数据
    tablename = "EcWellHisData"
    del_data_sql = "DELETE FROM %s WHERE %s = '%s'" % (tablename, "RQ", begintime)
    executesql(SqlserverDataServre, del_data_sql)

    # 删除已有数据
    dynamicdata.to_sql(tablename, GlOBAL_mssql_write_engine, if_exists='append')

    print("%s日%s口井单井曲线数据更新完毕！" % (begintime, len(dynamicdata)))


if __name__ == '__main__':
    print("开始初始化数据")
    initconfig()

    extract_erchang_welldynicurve_data()

    # test()
    # # 提取单井参数数据

    # extract_tanker_shipment_data()
    # extract_well_realtime_data()
    # extract_level_date()

    # # print("开始迭代更新数据")
    # scheduler = BackgroundScheduler()
    # # 提取单井参数数据
    # job8 = scheduler.add_job(extract_well_parameter_data, 'cron', hour=20)
    # scheduler.start()
    #
    # while True:
    #     time.sleep(0.1)
