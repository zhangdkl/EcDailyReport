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
import requests
import json
import locale

locale.setlocale(locale.LC_CTYPE, 'chinese')

# import matplotlib.pyplot as plt

pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
# pd.set_option('display.max_colwidth', 5000)
pd.set_option('display.width', 1000)


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
    global GlOBAL_oracle_engine_ecpro
    global GlOBAL_oracle_engine_ecpro_pradayly
    global GlOBAL_oracle_engine_bupro
    global GlOBAL_mssql_engine_GBK
    global GlOBAL_mssql_engine_UTF8
    global SqlserverDataServre
    global GLOBLE_gaswaterrob_url
    global GLOBLE_headers
    global GLOBLE_corp_id
    global GLOBLE_corp_secret

    GLOBLE_gaswaterrob_url = "https://oapi.dingtalk.com/robot/send?access_token=6f7a56d15c8f7f362a7f7d0c5896919133fe3238d0c8bca53e6532e30488c6a5"
    GLOBLE_headers = {"Content-Type": "application/json; charset=utf-8"}
    GLOBLE_corp_id = "ding4acb78f9e8af3f0635c2f4657eb6378f"
    GLOBLE_corp_secret = "zUm-_VjM_s_O4zNHBqfdrf1SE9ejfQVoXviIFRK9Y02dgG-t1ZI5PGvhPsoLblEe"

    # 采油厂生产数据关系库
    SqlserverDataServre = {"host": '10.16.192.40',
                           "user": 'zhangdkl',
                           "password": '4687607',
                           "database": 'cyec',
                           "charset": 'utf8',
                           "servertype": 'sqlserver'}
    # 采油厂生产数据关系库（PCS）
    dsn_tns = cx_Oracle.makedsn('10.16.192.49', 1521, 'ORCL')
    sensor_oracle_data_servre = {"user": 'sssjzc',
                                 "password": 'sssjzc',
                                 "dsn_tns": dsn_tns,
                                 "servertype": 'oracle'}
    # 采油厂生产数据关系库（PCS内日度数据表）
    dsn_tns1 = cx_Oracle.makedsn('10.16.192.49', 1521, 'ORCL')
    sensor_oracle_data_servre1 = {"user": 'pcsxt20',
                                  "password": 'pcsxt_20',
                                  "dsn_tns": dsn_tns1,
                                  "servertype": 'oracle'}
    # 分公司生产数据池发布数据库
    dsn_tns1 = cx_Oracle.makedsn('10.16.17.81', 1521, service_name='kfdb')
    daily_oracle_data_servre = {"user": 'yjywj',
                                "password": 'WjAdmin#$2021003',
                                "dsn_tns": dsn_tns1,
                                "servertype": 'oracle'}

    # 采油厂实时生产数据库连接
    GlOBAL_oracle_engine_ecpro = create_engine('oracle+cx_oracle://%s:%s@%s' % (
        sensor_oracle_data_servre["user"], sensor_oracle_data_servre["password"], sensor_oracle_data_servre["dsn_tns"]))
    # 采油厂实时生产数据库连接(油井日度数据)
    GlOBAL_oracle_engine_ecpro_pradayly = create_engine('oracle+cx_oracle://%s:%s@%s' % (
        sensor_oracle_data_servre1["user"], sensor_oracle_data_servre1["password"], sensor_oracle_data_servre1["dsn_tns"]))
    # 分公司实时生产数据库连接
    GlOBAL_oracle_engine_bupro = create_engine('oracle+cx_oracle://%s:%s@%s' % (
        daily_oracle_data_servre["user"], daily_oracle_data_servre["password"], daily_oracle_data_servre["dsn_tns"]))
    # 采油厂动态生产数据库连接（GBK）
    GlOBAL_mssql_engine_GBK = create_engine('mssql+pymssql://%s:%s@%s/%s' % (
        SqlserverDataServre["user"], SqlserverDataServre["password"], SqlserverDataServre["host"],
        SqlserverDataServre["database"]), connect_args={'charset': 'GBK'})
    # 采油厂动态生产数据库连接（UTF8）
    GlOBAL_mssql_engine_UTF8 = create_engine('mssql+pymssql://%s:%s@%s/%s' % (
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

    StrBeginDay = time.strftime("%Y-%m-%d %H:00:00", DateBeginDay.timetuple())
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
    wellname = pd.read_sql(welldata_sql, GlOBAL_mssql_engine_GBK)
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
    realtimedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine_ecpro)
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


# 转储数据
# 转储二厂单井日度化验数据
def extract_erchang_well_assay_data():
    # GLOBAL_Logger.info("开始转储二厂油井化验数据！")
    print("......................................................")
    print(getnowtime(), "开始转储二厂油井化验数据!")

    extractdays = 6
    try:
        for exday in range(1, extractdays):
            # if True:
            #     exday = 4216
            ExtractErchangWellTestData(exday)
            ExtractErchangWellManualIndicatorData(exday)
            print(getstartdate(exday), " 二厂油井化验数据转储完毕！")
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 转储二厂单井动静压力、液面数据
def ExtractErchangWellTestData(exday):
    begintime = getstartdate(exday)

    # 液面数据
    StrFieds = ""
    tablename = "CJXT.YS_DCA03"
    GetField = ["JH", "CSRQ", "DYM", "JYM"]
    LevelFieldName = ["wellname", "date", "dynamicfluidlevel", "staticfluidlevel"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" \
                   % (StrFieds, tablename, "CSRQ", begintime)
    yemiandata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine_bupro)
    yemiandata.columns = LevelFieldName
    yemiandata = yemiandata.set_index(["wellname", "date"])
    # print(yemiandata)

    # 动态压力数据
    StrFieds = ""
    tablename = "CJXT.YS_DCA05"
    GetField = ["JH", "CSRQ", "YQCZBLY", "YQCZBJY"]
    PresureFieldName = ["wellname", "date", "dynamicpressure", "staticpressure"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" \
                   % (StrFieds, tablename, "CSRQ", begintime)
    yalidata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine_bupro)
    yalidata.columns = PresureFieldName
    yalidata = yalidata.set_index(["wellname", "date"])

    testdata = pd.concat([yemiandata, yalidata], sort=True)
    testdata = testdata.reset_index(level=["date"])

    # 删除已有数据
    tablename = "EcWellTest"
    del_data_sql = "DELETE FROM %s WHERE %s = '%s'" % (tablename, "date", begintime)
    executesql(SqlserverDataServre, del_data_sql)

    # # 写入测试数据
    testdata.to_sql(tablename, GlOBAL_mssql_engine_UTF8, if_exists='append')


# 转储二厂单井手测功图数据
def ExtractErchangWellManualIndicatorData(exday):
    begintime = getstartdate(exday)

    # 手动功图数据
    StrFieds = ""
    tablename = "CJXT.YS_DCA01"
    GetField = ["JH", "CSRQ", "CC", "CC1", "SXZDFH", "XXZXFH", "DBMS_LOB.SUBSTR(SGT,2000,1)"]
    ShouceFieldName = ["wellname", "date", "stroke", "speed", "maxload", "minload", "gongtu"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" \
                   % (StrFieds, tablename, "CSRQ", begintime)
    shoucedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine_bupro)
    shoucedata.columns = ShouceFieldName
    shoucedata = shoucedata.set_index(["wellname"])
    shoucedata = shoucedata.dropna()

    displacement = []
    load = []
    for i in range(len(shoucedata)):
        gongtu = shoucedata.iloc[i]["gongtu"]
        gongtu = [x.split(",") for x in gongtu.split(";")]
        weiyi = [x[0] for x in gongtu][:-1]
        zaihe = [x[-1] for x in gongtu][:-1]
        weiyi = ",".join(weiyi)
        zaihe = ",".join(zaihe)
        displacement.append(weiyi)
        load.append(zaihe)
    shoucedata["displacement"] = displacement
    shoucedata["load"] = load
    shoucedata = shoucedata.drop(columns=['gongtu'])

    # print(shoucedata)
    # 删除已有数据
    tablename = "EcWellIndicator"
    del_data_sql = "DELETE FROM %s WHERE %s = '%s'" % (tablename, "date", begintime)
    executesql(SqlserverDataServre, del_data_sql)

    # # 写入测试数据
    shoucedata.to_sql(tablename, GlOBAL_mssql_engine_UTF8, if_exists='append')


# 转储二厂单井小时监测数据
def extract_erchang_well_hour_monitor_data():
    print("......................................................")
    print(getnowtime(), "开始转储二厂单井小时监测数据!")

    extracthours = 5
    try:
        for exhour in range(1, extracthours):
            # 单井功图数据
            ExtractEcWellAutoIndicatorDataHour(exhour)
            # 单井掺稀数据
            ExtractEcWellChanxiDataHour(exhour)
            # 计转站外输数据
            ExtractEcStationExTransmissionDataHour(exhour)
            # 计转站掺稀数据
            ExtractEcStationChanxiInputDataHour(exhour)
            # 单井工况参数
            ExtractEcWellParameterDataHour(exhour)
            print("%s 二厂单井小时监测转储完毕！" % getbegintime((exhour - 1) * 60, "minutes"))
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 转储二厂单井自动功图数据
def ExtractEcWellAutoIndicatorDataHour(exhour):
    begintime = getbegintime(exhour * 60, "minutes")
    endtime = getbegintime((exhour - 1) * 60, "minutes")

    # print(begintime, endtime)

    # 自动功图数据
    StrFieds = ""
    tablename = "ssc_sgt_gtcj"
    GetField = ["JH", "CJSJ", "cc", "cc1", "WY", "ZH"]
    ShishiFieldName = ["wellname", "date", "stroke", "speed", "displacement", "load"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s >= TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') and %s <= TO_DATE('%s', " \
                   "'yyyy-mm-dd,hh24:mi:ss')" \
                   % (StrFieds, tablename, "CJSJ", begintime, "CJSJ", endtime)
    shicedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine_ecpro)
    shicedata.columns = ShishiFieldName
    shicedata = shicedata[shicedata["stroke"] > 0]

    shicedata.drop_duplicates(subset=["wellname"], keep='last', inplace=True)
    shicedata = shicedata.set_index(["wellname"])
    shicedata.dropna(how="any", inplace=True)

    # 去除为零异常功图，计算最大载荷
    datalen = len(shicedata)
    if datalen > 0:
        iswork = []
        maxload = []
        minload = []
        for i in range(datalen):
            displacement = shicedata.iloc[i]["displacement"]
            displacement = displacement.split(",")
            displacement = list(map(float, displacement))
            load = shicedata.iloc[i]["load"]
            load = load.split(",")
            load = list(map(float, load))
            if sum(displacement) > 0 and sum(load) > 0:
                iswork.append(True)
                maxload.append(round(max(load), 2))
                minload.append(round(min(load), 2))
            else:
                iswork.append(False)
                maxload.append(round(max(load), 2))
                minload.append(round(min(load), 2))

        shicedata["iswork"] = iswork
        shicedata["maxload"] = maxload
        shicedata["minload"] = minload
        shicedata["date"] = endtime
        shicedata = shicedata[shicedata["iswork"]]
        shicedata = shicedata.drop(columns=['iswork'])

        # 删除已有数据
        tablename = "EcWellIndicatorAutoHour"
        del_data_sql = "DELETE FROM %s WHERE %s = '%s'" % (tablename, "date", endtime)
        executesql(SqlserverDataServre, del_data_sql)

        # # 写入测试数据
        shicedata.to_sql(tablename, GlOBAL_mssql_engine_UTF8, if_exists='append')


# 转储二厂单井掺稀小时数据
def ExtractEcWellChanxiDataHour(exhour):
    # 流量计位置检查
    def checklocation(x):
        if pd.isnull(x):
            status = "未知"
        else:
            if ("港西" in x) or ("单井" in x):
                status = "站外"
            elif ("不掺稀" in x) or ("否" in x):
                status = "不掺稀"
            else:
                status = "站内"
        return status

    # 计转站归属整理
    def sortstation(x):
        if pd.isnull(x):
            status = "未知"
        else:
            if x.find("计") > 0:
                status = x[:x.find("计") + 1]
            elif x.find("罐") > 0:
                status = x[:x.find("罐") + 2]
            else:
                status = x
        return status

    # 流量计状态检查
    def checkstatus(x):
        prostatus = x["prostatus"]
        rio = x["stacheck"]
        baserio = x["setcheck"]
        if prostatus != "不掺稀":
            if prostatus == "开井":
                if pd.isnull(rio):
                    status = "无数据"
                elif rio < 10:
                    if baserio < 30:
                        status = "正常"
                    else:
                        status = "设定异常"
                else:
                    if baserio < 30:
                        status = "底数异常"
                    else:
                        status = "底数及设定异常"
            else:
                status = "未开井"
        else:
            status = "不掺稀"

        return status

    def gethourdata(x):
        avr = x["avrhourchanxi"]
        diff = x["diffhourchanxi"]
        rio = x["stacheck"]

        if rio < 10:
            hourdata = diff
        else:
            hourdata = avr

        return hourdata

    exbegintime = getbegintime((exhour + 1) * 60, "minutes")
    begintime = getbegintime(exhour * 60, "minutes")
    endtime = getbegintime((exhour - 1) * 60, "minutes")

    # 掺稀数据统计
    tablename = "ssc_yj_ss"
    fields = ["JH", "CJSJ", "CX_SDLL", "CX_SSLL", "CX_LJLL"]
    FieldName = ["wellname", "time", "basedata", "hourchanxi", "sumchanxi"]
    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field

    get_data_sql = "SELECT %s FROM %s WHERE cjsj>TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') AND cjsj <= TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" % \
                   (str_fieds, tablename, begintime, endtime)
    realtimedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine_ecpro)
    realtimedata.columns = FieldName
    realtimedata = realtimedata.sort_values("time")

    get_data_sql = "SELECT %s FROM %s WHERE cjsj>TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') AND cjsj <= TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" % \
                   (str_fieds, tablename, exbegintime, begintime)
    exrealtimedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine_ecpro)
    exrealtimedata.columns = FieldName
    exrealtimedata = exrealtimedata.sort_values("time")

    # 统计掺稀设定值、掺稀底数
    sumrealtimedata = realtimedata.drop_duplicates(subset=["wellname"], keep='last')
    sumrealtimedata.set_index(['wellname'], inplace=True)

    exsumrealtimedata = exrealtimedata.drop_duplicates(subset=["wellname"], keep='last')
    exsumrealtimedata.set_index(['wellname'], inplace=True)
    exsumrealtimedata = exsumrealtimedata.drop(columns=['time', 'basedata', 'hourchanxi'])
    exsumrealtimedata.rename(columns={'sumchanxi': 'exsumchanxi'}, inplace=True)

    sumrealtimedata = pd.concat([sumrealtimedata, exsumrealtimedata], axis=1, join='inner')
    sumrealtimedata["diffhourchanxi"] = sumrealtimedata["sumchanxi"] - sumrealtimedata["exsumchanxi"]
    sumrealtimedata = sumrealtimedata.drop(columns=['time', 'hourchanxi', 'exsumchanxi'])

    # 统计小时掺稀量，平均值
    realtimedata = realtimedata.groupby("wellname").mean()
    realtimedata = realtimedata.drop(columns=['basedata', 'sumchanxi'])
    realtimedata = pd.concat([realtimedata, sumrealtimedata], axis=1, join='inner')

    # 归属关系统计
    tablename = "EcWellStaticData"
    fields = ["JH", "DWMC", "DYFS", "CXFS", "CYQ", "SCZT", "ISCX"]
    FieldName = ["wellname", "compname", "exportstation", "chanxistation", "area", "prostatus", "ischanxi"]
    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field

    get_data_sql = "SELECT %s FROM %s " % (str_fieds, tablename)
    staticdata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_GBK)
    staticdata.columns = FieldName
    staticdata = staticdata.set_index("wellname")

    realtimedata = pd.concat([staticdata, realtimedata], axis=1, join='outer')

    realtimedata["check"] = realtimedata.index
    realtimedata = realtimedata[~realtimedata["check"].str.contains("-JRL")]
    realtimedata.drop(columns=["check"], inplace=True)
    realtimedata.dropna(how="all", inplace=True)
    realtimedata["time"] = endtime

    realtimedata.rename(columns={'hourchanxi': 'avrhourchanxi'}, inplace=True)

    realtimedata["stacheck"] = abs((realtimedata["avrhourchanxi"] - realtimedata["diffhourchanxi"]) / realtimedata["avrhourchanxi"] * 100)
    realtimedata.replace([np.inf, -np.inf], np.nan, inplace=True)
    realtimedata["devicelocation"] = realtimedata["chanxistation"].map(checklocation)
    realtimedata["hourchanxi"] = realtimedata.apply(gethourdata, axis=1)
    realtimedata["setcheck"] = abs((realtimedata["basedata"] - realtimedata["hourchanxi"]) / realtimedata["hourchanxi"] * 100)
    realtimedata["devicestatus"] = realtimedata.apply(checkstatus, axis=1)
    realtimedata["station"] = realtimedata["chanxistation"].map(sortstation)

    realtimedata.replace([np.inf, -np.inf], np.nan, inplace=True)

    # 删除已有数据
    tablename = "EcWellChanxiRealtimeAutoHour"
    del_data_sql = "DELETE FROM %s WHERE %s = '%s'" % (tablename, "time", endtime)
    executesql(SqlserverDataServre, del_data_sql)

    # # 写入测试数据
    realtimedata.to_sql(tablename, GlOBAL_mssql_engine_UTF8, if_exists='append')


# 转储二厂站库外输小时数据
def ExtractEcStationExTransmissionDataHour(exhour):
    # 流量计状态检查
    def checkstatus(x):
        if pd.isnull(x):
            status = "无数据"
        elif x < 10:
            status = "正常"
        else:
            status = "底数异常"
        return status

    def gethourdata(x):
        avr = x["evrhourdata"]
        diff = x["diffhourdata"]
        rio = x["stacheck"]

        if rio < 10:
            hourdata = diff
        else:
            hourdata = avr

        return hourdata

    exbegintime = getbegintime((exhour + 1) * 60, "minutes")
    begintime = getbegintime(exhour * 60, "minutes")
    endtime = getbegintime((exhour - 1) * 60, "minutes")

    tablename = "SSC_LLJ_SS"

    # 当前小时数据
    fields = ["CJSJ", "SBBM", "SSLL", "LJLL", "YL", "WD"]
    FieldName = ["time", "divcode", "hourdata", "sumdata", "pressure", "temperature"]
    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field

    get_data_sql = "SELECT %s FROM %s WHERE cjsj>=TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') and cjsj<TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') and " \
                   "instr(sbbm, '%s')>0" % (str_fieds, tablename, begintime, endtime, "_YYWSHG")
    realtimedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine_ecpro)
    realtimedata.columns = FieldName
    realtimedata = realtimedata.sort_values("time")
    realtimedata["station"] = realtimedata["divcode"].map(lambda x: x[:x.find("_") - 3] + "计")

    # 当前小时累计数据
    sumrealtimedata = realtimedata.drop_duplicates(subset=["divcode"], keep='last')
    sumrealtimedata.set_index(['divcode'], inplace=True)

    # 上一小时数据
    fields = ["CJSJ", "SBBM", "SSLL", "LJLL", "YL", "WD"]
    FieldName = ["time", "divcode", "hourdata", "sumdata", "pressure", "temperature"]
    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field

    get_data_sql = "SELECT %s FROM %s WHERE cjsj>=TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') and cjsj<TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') and " \
                   "instr(sbbm, '%s')>0" % (str_fieds, tablename, exbegintime, begintime, "_YYWSHG")
    exrealtimedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine_ecpro)
    exrealtimedata.columns = FieldName
    exrealtimedata = exrealtimedata.sort_values("time")
    # exrealtimedata["station"] = exrealtimedata["divcode"].map(lambda x: x[:x.find("_")-3] + "计")
    # 上小时累计数据
    exsumrealtimedata = exrealtimedata.drop_duplicates(subset=["divcode"], keep='last')
    exsumrealtimedata.set_index(['divcode'], inplace=True)
    exsumrealtimedata = exsumrealtimedata.drop(columns=['time', 'hourdata', 'pressure', 'temperature'])
    exsumrealtimedata.rename(columns={'sumdata': 'exsumdata'}, inplace=True)

    # 计算外输底数差值
    sumrealtimedata = pd.concat([sumrealtimedata, exsumrealtimedata], axis=1, join='inner')
    sumrealtimedata["diffhourdata"] = sumrealtimedata["sumdata"] - sumrealtimedata["exsumdata"]
    sumrealtimedata = sumrealtimedata.drop(columns=['time', 'hourdata', 'temperature', 'pressure', 'exsumdata'])

    # 计算小时数据
    realtimedata = realtimedata.groupby("divcode").mean()
    realtimedata.rename(columns={'hourdata': 'evrhourdata'}, inplace=True)
    realtimedata = realtimedata.drop(columns=['sumdata'])
    realtimedata = pd.concat([realtimedata, sumrealtimedata], axis=1, join='inner')
    realtimedata.set_index(['station'], inplace=True)
    realtimedata["time"] = endtime
    realtimedata["devicetype"] = "原油外输"
    realtimedata["stacheck"] = abs((realtimedata["evrhourdata"] - realtimedata["diffhourdata"]) / realtimedata["evrhourdata"] * 100)
    realtimedata.replace([np.inf, -np.inf], np.nan, inplace=True)
    realtimedata["devicestatus"] = realtimedata["stacheck"].map(checkstatus)
    realtimedata["hourdata"] = realtimedata.apply(gethourdata, axis=1)

    # print(realtimedata)

    # 删除已有数据
    tablename = "EcStationExtransmissionAutoHour"
    del_data_sql = "DELETE FROM %s WHERE %s = '%s'" % (tablename, "time", endtime)
    executesql(SqlserverDataServre, del_data_sql)

    # # 写入测试数据
    realtimedata.to_sql(tablename, GlOBAL_mssql_engine_UTF8, if_exists='append')


# 转储二厂站库掺稀来油小时数据
def ExtractEcStationChanxiInputDataHour(exhour):
    # 流量计状态检查
    def checkstatus(x):
        if pd.isnull(x):
            status = "无数据"
        elif x < 10:
            status = "正常"
        else:
            status = "底数异常"
        return status

    def gethourdata(x):
        avr = x["avrhourchanxi"]
        diff = x["diffhourchanxi"]
        rio = x["stacheck"]

        if rio < 10:
            hourdata = diff
        else:
            hourdata = avr

        return hourdata

    exbegintime = getbegintime((exhour + 1) * 60, "minutes")
    begintime = getbegintime(exhour * 60, "minutes")
    endtime = getbegintime((exhour - 1) * 60, "minutes")

    tablename = "SSC_LLJ_SS"

    # 当前小时数据
    fields = ["CJSJ", "SBBM", "SSLL", "LJLL", "YL", "WD"]
    FieldName = ["time", "divcode", "hourchanxi", "sumchanxi", "pressure", "temperature"]
    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field

    get_data_sql = "SELECT %s FROM %s WHERE cjsj>=TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') and cjsj<TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') and " \
                   "instr(sbbm, '%s')>0" % (str_fieds, tablename, begintime, endtime, "_LXYGX")
    realtimedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine_ecpro)
    realtimedata.columns = FieldName
    realtimedata = realtimedata.sort_values("time")
    realtimedata["station"] = realtimedata["divcode"].map(lambda x: x[:x.find("_") - 3] + "计")

    # 当前小时累计数据
    sumrealtimedata = realtimedata.drop_duplicates(subset=["divcode"], keep='last')
    sumrealtimedata.set_index(['divcode'], inplace=True)
    # print(sumrealtimedata)

    # 上一小时数据
    fields = ["CJSJ", "SBBM", "SSLL", "LJLL", "YL", "WD"]
    FieldName = ["time", "divcode", "hourchanxi", "sumchanxi", "pressure", "temperature"]
    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field

    get_data_sql = "SELECT %s FROM %s WHERE cjsj>=TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') and cjsj<TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') and " \
                   "instr(sbbm, '%s')>0" % (str_fieds, tablename, exbegintime, begintime, "_LXYGX")
    exrealtimedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine_ecpro)
    exrealtimedata.columns = FieldName
    exrealtimedata = exrealtimedata.sort_values("time")
    # exrealtimedata["station"] = exrealtimedata["divcode"].map(lambda x: x[:x.find("_")-3] + "计")
    # 上小时累计数据
    exsumrealtimedata = exrealtimedata.drop_duplicates(subset=["divcode"], keep='last')
    exsumrealtimedata.set_index(['divcode'], inplace=True)
    exsumrealtimedata = exsumrealtimedata.drop(columns=['time', 'hourchanxi', 'pressure', 'temperature'])
    exsumrealtimedata.rename(columns={'sumchanxi': 'exsumchanxi'}, inplace=True)

    # 计算外输底数差值
    sumrealtimedata = pd.concat([sumrealtimedata, exsumrealtimedata], axis=1, join='inner')
    sumrealtimedata["diffhourchanxi"] = sumrealtimedata["sumchanxi"] - sumrealtimedata["exsumchanxi"]
    sumrealtimedata = sumrealtimedata.drop(columns=['time', 'hourchanxi', 'temperature', 'pressure', 'exsumchanxi'])

    # 计算小时数据
    realtimedata = realtimedata.groupby("divcode").mean()
    realtimedata.rename(columns={'hourchanxi': 'avrhourchanxi'}, inplace=True)
    realtimedata = realtimedata.drop(columns=['sumchanxi'])
    realtimedata = pd.concat([realtimedata, sumrealtimedata], axis=1, join='inner')
    realtimedata.set_index(['station'], inplace=True)
    realtimedata["time"] = endtime
    realtimedata["devicetype"] = "掺稀来油"
    realtimedata["stacheck"] = abs((realtimedata["avrhourchanxi"] - realtimedata["diffhourchanxi"]) / realtimedata["avrhourchanxi"] * 100)
    realtimedata.replace([np.inf, -np.inf], np.nan, inplace=True)
    realtimedata["devicestatus"] = realtimedata["stacheck"].map(checkstatus)
    realtimedata["hourchanxi"] = realtimedata.apply(gethourdata, axis=1)

    # 删除已有数据
    tablename = "EcStationChanxiInputAutoHour"
    del_data_sql = "DELETE FROM %s WHERE %s = '%s'" % (tablename, "time", endtime)
    executesql(SqlserverDataServre, del_data_sql)

    # # 写入测试数据
    realtimedata.to_sql(tablename, GlOBAL_mssql_engine_UTF8, if_exists='append')


# 转储二厂单井参数小时数据
def ExtractEcWellParameterDataHour(exhour):
    begintime = getbegintime(exhour * 60, "minutes")
    endtime = getbegintime((exhour - 1) * 60, "minutes")
    enddate = getstartdate((exhour - 1) / 24)

    # 掺稀数据统计
    tablename = "ssc_yj_ss"
    fields = ["JH", "CJSJ", "JK_YY", "JK_TY", "JK_HY", "JK_WD", "DY_A", "DY_B", "DY_C", "DL_A", "DL_B", "DL_C", "AXSXDLFZ", "BXSXDLFZ", "CXSXDLFZ",
              "AXXXDLFZ", "BXXXDLFZ", "CXXXDLFZ", "JRL_SYWD", "JRL_YW"]
    FieldName = ["wellname", "RQ", "YY", "TY", "HY", "JW", "DY_A", "DY_B", "DY_C", "DL_A", "DL_B", "DL_C", "AXSXDLFZ", "BXSXDLFZ", "CXSXDLFZ",
                 "AXXXDLFZ", "BXXXDLFZ", "CXXXDLFZ", "LW", "CW"]
    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field

    get_data_sql = "SELECT %s FROM %s WHERE cjsj>TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') AND cjsj <= TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" % \
                   (str_fieds, tablename, begintime, endtime)
    realtimedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine_ecpro)
    realtimedata.columns = FieldName
    realtimedata = realtimedata.sort_values("RQ")

    realtimedata = realtimedata.groupby("wellname").mean()
    realtimedata["DY"] = (realtimedata["DY_A"] + realtimedata["DY_B"] + realtimedata["DY_C"]) / 3
    realtimedata["DL"] = (realtimedata["DL_A"] + realtimedata["DL_B"] + realtimedata["DL_C"]) / 3
    realtimedata["SXDL"] = (realtimedata["AXSXDLFZ"] + realtimedata["BXSXDLFZ"] + realtimedata["CXSXDLFZ"]) / 3
    realtimedata["XXDL"] = (realtimedata["AXXXDLFZ"] + realtimedata["BXXXDLFZ"] + realtimedata["CXXXDLFZ"]) / 3
    realtimedata = realtimedata.drop(columns=['DY_A', 'DY_B', 'DY_C', 'DL_A', 'DL_B', 'DL_C', 'AXSXDLFZ', 'BXSXDLFZ', 'CXSXDLFZ', 'AXXXDLFZ',
                                              'BXXXDLFZ', 'CXXXDLFZ'])

    # 归属关系统计
    fields = ["JH", "DWMC", "DYFS", "CXFS", "CYQ", "SCZT", "ISCX", "SCFS", "JB"]
    FieldName = ["wellname", "compname", "exportstation", "chanxistation", "area", "prostatus", "ischanxi", "protype", "welltype"]

    tablename = "EcWellStaticDataHistory"
    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field

    get_data_sql = "SELECT %s FROM %s WHERE RQ = '%s'" % (str_fieds, tablename, enddate)
    staticdata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_GBK)

    if len(staticdata) == 0:
        tablename = "EcWellStaticData"
        str_fieds = ""
        for field in fields:
            if str_fieds == "":
                str_fieds = field
            else:
                str_fieds = str_fieds + ", " + field

        get_data_sql = "SELECT %s FROM %s " % (str_fieds, tablename)
        staticdata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_GBK)

    staticdata.columns = FieldName
    staticdata = staticdata.set_index("wellname")

    realtimedata = pd.concat([staticdata, realtimedata], axis=1, join='outer')
    realtimedata = realtimedata.round(2)
    realtimedata["time"] = endtime
    realtimedata["date"] = enddate

    # 删除已有数据
    tablename = "EcWellParameterRealtimeAutoHour"
    del_data_sql = "DELETE FROM %s WHERE %s = '%s'" % (tablename, "time", endtime)
    executesql(SqlserverDataServre, del_data_sql)

    # # 写入测试数据
    realtimedata.to_sql(tablename, GlOBAL_mssql_engine_UTF8, if_exists='append')


# 转储二厂单井日度监测数据
def extract_erchang_well_daily_monitor_data():
    print("......................................................")
    print(getnowtime(), "开始转储二厂单井日度监测数据!")

    extracdays = 6
    try:
        for exday in range(0, extracdays):
            # 单井功图数据
            ExtractEcWellAutoIndicatorDataDay(exday)
            # 单井工况参数
            ExtractEcWellParameterDataDay(exday)
            print("%s 二厂单井日度监测转储完毕！" % getbegintime(exday, "days"))
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 转储二厂单井日度监测数据
def extract_erchang_well_daily_monitor_chanxi_data():
    print("......................................................")
    print(getnowtime(), "开始转储二厂单井日度掺稀监测数据!")

    extracdays = 5
    try:
        for exday in range(0, extracdays):
            # 单井掺稀数据
            ExtractEcWellChanxiDataDay(exday)
            print("%s 二厂单井日度掺稀监测转储完毕！" % getbegintime(exday, "days"))
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 转储二厂单井自动功图数据
def ExtractEcWellAutoIndicatorDataDay(exday):
    begintime = getbegintime(exday + 1, "days")
    endtime = getbegintime(exday, "days")
    uptime = getstartdate(exday)
    # print(1, uptime)

    # 自动功图数据
    StrFieds = ""
    tablename = "EcWellIndicatorAutoHour"
    GetField = ["wellname", "date", "stroke", "speed", "minload", "maxload", "displacement", "load"]
    # ShishiFieldName = ["wellname", "date", "stroke", "speed", "minload", "maxload"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s >= '%s' and %s <= '%s'" % (StrFieds, tablename, "date", begintime, "date", endtime)
    shicedata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_GBK)
    # shicedata.columns = ShishiFieldName
    if len(shicedata) > 0:
        meanshicedata = shicedata.groupby("wellname").mean()
        meanshicedata = meanshicedata.round(1)
        meanshicedata["date"] = uptime

        lastshicedata = shicedata.groupby("wellname").last()
        lastshicedata = lastshicedata.loc[:, ["displacement", "load"]]

        shicedata = pd.concat([meanshicedata, lastshicedata], axis=1, join="outer")

        # 删除已有数据
        tablename = "EcWellIndicatorAutoDaily"
        del_data_sql = "DELETE FROM %s WHERE %s = '%s'" % (tablename, "date", uptime)
        executesql(SqlserverDataServre, del_data_sql)

        # 写入测试数据
        shicedata.to_sql(tablename, GlOBAL_mssql_engine_UTF8, if_exists='append')


# 转储二厂单井掺稀日度数据
def ExtractEcWellChanxiDataDay(exday):
    begintime = getbegintime(exday - 1, "days")
    endtime = getbegintime(exday, "days")
    uptime = getstartdate(exday)
    # print(begintime, endtime)

    # 小时掺稀数据
    StrFieds = ""
    tablename = "EcWellChanxiRealtimeCheckHour"
    GetField = ["wellname", "time", "basedata", "diffhourchanxi", "sumchanxi", "compname", "exportstation", "chanxistation", "area", "prostatus",
                "ischanxi", "remark"]
    FieldName = ["wellname", "date", "basedata", "dailychanxi", "sumchanxi", "compname", "exportstation", "chanxistation", "area", "prostatus",
                 "ischanxi", "remark"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s >= '%s' and %s <= '%s'" % (StrFieds, tablename, "time", begintime, "time", endtime)
    dailydata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_GBK)
    dailydata.columns = FieldName
    dailydata = dailydata.sort_values("date")

    # 统计掺稀设定值、掺稀底数
    sumdailydata = dailydata.drop_duplicates(subset=["wellname"], keep='last')
    sumdailydata.set_index(['wellname'], inplace=True)
    sumdailydata = sumdailydata.drop(columns=['dailychanxi', 'date', 'remark'])

    sumdailydata["dailychanxi"] = dailydata.groupby("wellname")["dailychanxi"].sum()
    sumdailydata["remark"] = dailydata.groupby("wellname")["remark"].apply(lambda x: x.str.cat(sep=';'))
    sumdailydata["date"] = uptime
    # print(sumdailydata)
    # print(len(sumdailydata))

    # 删除已有数据
    tablename = "EcWellChanxiRealtimeAutoDaily"
    del_data_sql = "DELETE FROM %s WHERE %s = '%s'" % (tablename, "date", uptime)
    executesql(SqlserverDataServre, del_data_sql)

    # 写入测试数据
    sumdailydata.to_sql(tablename, GlOBAL_mssql_engine_UTF8, if_exists='append')


# 转储二厂单井工况参数日度数据
def ExtractEcWellParameterDataDay(exday):
    uptime = getstartdate(exday)

    # print(2, uptime)

    def replacedata(x, be_row, to_row):
        bedata = x[be_row]
        todata = x[to_row]
        if pd.isnull(bedata):
            return todata
        else:
            return bedata
        pass

    def comparedata(x, maxrow, minrow):
        maxdata = x[maxrow]
        mindata = x[minrow]
        if pd.isnull(maxdata):
            return mindata
        elif maxdata >= mindata:
            return mindata
        elif mindata - maxdata > 1:
            return mindata
        else:
            return maxdata - 0.01
        pass

    # （1）提取PCS日度数据
    tablename = "SCZH.SSC_YJ_RD"
    fields = ["JH", "YY", "JKWD", "CC", "CC1", "DY", "DL", "SXDL", "XXDL"]
    FieldName = ["wellname", "YY", "JW", "CC", "CC1", "DY", "DL", "SXDL", "XXDL"]
    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE RQ=TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') " % (str_fieds, tablename, uptime)
    pcsdata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine_ecpro_pradayly)
    pcsdata.columns = FieldName
    pcsdata.set_index(['wellname'], inplace=True)

    # 检查PCS数据，若不存在，连续取数据！
    checklen = len(pcsdata)
    checktimes = 12
    checktime = 0
    if checklen == 0:
        waitetime = 5 * 60
        time.sleep(waitetime)
        for checktime in range(checktimes):
            pcsdata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine_ecpro_pradayly)
            checklen = len(pcsdata)
            print("重新提取数据%s次" % (checktime + 1))
            if checklen > 0:
                break
            time.sleep(waitetime)
    # print(pcsdata)

    # （2）提取远传小时数据
    begintime = getbegintime(exday + 1, "days")
    endtime = getbegintime(exday, "days")

    # 小时掺稀数据
    StrFieds = ""
    tablename = "EcWellParameterRealtimeAutoHour"
    GetField = ["wellname", "time", "yy", "ty", "hy", "jw", "dy", "dl", "sxdl", "xxdl", "lw", "cw"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s >= '%s' and %s < '%s'" % (StrFieds, tablename, "time", begintime, "time", endtime)
    dailydata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_GBK)
    dailydata = dailydata.sort_values("time")
    gbydailydata = dailydata.groupby("wellname")

    dailydatamean = gbydailydata.mean()
    dailydatamean = dailydatamean.drop(columns=["yy", "ty", "hy", "jw"])
    dailydatamean.columns = ["dymean", "dlmean", "sxdlmean", "xxdlmean", "lwmean", "cwmean"]

    dailydatamax = gbydailydata.max()
    dailydatamax = dailydatamax.drop(columns=["dy", "dl", "sxdl", "xxdl", "lw", "cw", "time"])
    dailydatamax.columns = ["yymax", "tymax", "hymax", "jwmax"]

    dailydatamin = gbydailydata.min()
    dailydatamin = dailydatamin.drop(columns=["dy", "dl", "sxdl", "xxdl", "lw", "cw", "time"])
    dailydatamin.columns = ["yymin", "tymin", "hymin", "jwmin"]
    #
    # print(dailydatamean)
    # print(dailydatamax)
    # print(dailydatamin)

    # （3）提取功图载荷数据
    StrFieds = ""
    tablename = "EcWellIndicatorAutoDaily"
    GetField = ["wellname", "stroke", "speed", "maxload"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = '%s'" % (StrFieds, tablename, "date", uptime)
    indicatedata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_GBK)
    indicatedata.set_index(["wellname"], inplace=True)
    # print(indicatedata)

    # （4）归属关系统计
    # 归属关系统计
    fields = ["JH", "DWMC", "DYFS", "CXFS", "CYQ", "SCZT", "ISCX", "SCFS", "JB"]
    FieldName = ["wellname", "compname", "exportstation", "chanxistation", "area", "prostatus", "ischanxi", "protype", "welltype"]

    tablename = "EcWellStaticData"
    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field

    get_data_sql = "SELECT %s FROM %s " % (str_fieds, tablename)
    staticdata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_GBK)
    staticdata.columns = FieldName
    staticdata = staticdata.set_index("wellname")

    # print(staticdata)

    # (4)数据整合
    returndata = pd.concat([dailydatamean, dailydatamax, dailydatamin], axis=1, join='outer')
    if len(indicatedata) > 0:
        returndata = pd.concat([returndata, indicatedata], axis=1, join='outer')
    returndata = pd.concat([staticdata, returndata], axis=1, join='outer')
    # print(returndata)

    # 若持续提取不到数据，放弃
    if checktime < checktimes - 1:
        returndata = pd.concat([returndata, pcsdata], axis=1, join='outer')
        returndata["yymax"] = returndata.apply(replacedata, axis=1, args=('YY', 'yymax'))
        returndata["jwmax"] = returndata.apply(replacedata, axis=1, args=('JW', 'jwmax'))
        returndata["stroke"] = returndata.apply(replacedata, axis=1, args=('CC', 'stroke'))
        returndata["speed"] = returndata.apply(replacedata, axis=1, args=('CC1', 'speed'))
        returndata["dymean"] = returndata.apply(replacedata, axis=1, args=('DY', 'dymean'))
        returndata["dlmean"] = returndata.apply(replacedata, axis=1, args=('DL', 'dlmean'))
        returndata["sxdlmean"] = returndata.apply(replacedata, axis=1, args=('SXDL', 'sxdlmean'))
        returndata["xxdlmean"] = returndata.apply(replacedata, axis=1, args=('XXDL', 'xxdlmean'))
        returndata = returndata.drop(columns=["YY", "JW", "CC", "CC1", "DY", "DL", "SXDL", "XXDL"])
        # returndata.columns = ["dy", "dl", "sxdl", "xxdl", "lw", "cw", "yymax", "tymax", "hymax", "jwmax", "yymin", "tymin", "hymin", "jwmin",
        #                       "stroke", "speed", "maxload", "date"]

    returndata["hymax"] = returndata.apply(comparedata, axis=1, args=('yymax', 'hymax'))
    returndata["yymin"] = returndata.apply(comparedata, axis=1, args=('yymax', 'yymin'))
    returndata["tymin"] = returndata.apply(comparedata, axis=1, args=('tymax', 'tymin'))
    returndata["hymin"] = returndata.apply(comparedata, axis=1, args=('hymax', 'hymin'))
    returndata["jwmin"] = returndata.apply(comparedata, axis=1, args=('jwmax', 'jwmin'))
    returndata.dropna(how="all", inplace=True)

    returndata["check"] = returndata.index
    returndata = returndata[~returndata["check"].str.contains("-JRL")]
    returndata.drop(columns=["check"], inplace=True)

    returndata = returndata.round(2)
    returndata["stroke"] = returndata["stroke"].round(1)
    returndata["speed"] = returndata["speed"].round(1)
    returndata["dymean"] = returndata["dymean"].round(0)
    returndata["sxdlmean"] = returndata["sxdlmean"].round(1)
    returndata["xxdlmean"] = returndata["xxdlmean"].round(1)
    returndata["jwmax"] = returndata["jwmax"].round(1)
    returndata["tymax"] = returndata["tymax"].round(1)
    returndata["date"] = uptime
    # print(returndata)

    # 删除已有数据
    tablename = "EcWellParameterRealtimeAutoDaily"
    del_data_sql = "DELETE FROM %s WHERE %s = '%s'" % (tablename, "date", uptime)
    executesql(SqlserverDataServre, del_data_sql)

    # 写入测试数据
    returndata.to_sql(tablename, GlOBAL_mssql_engine_UTF8, if_exists='append')


# 转储二厂油井归属数据
def extract_erchang_well_relegation_data():
    global GLOBAL_Logger

    # GLOBAL_Logger.info("开始转储二厂油井归属数据！")
    print("......................................................")
    print(getnowtime(), "开始转储二厂油井归属数据!")

    try:
        ExtractErchangWellRelegationData()
        print("二厂油井归属数据转储完毕！")
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 转储二厂油井归属数据
def ExtractErchangWellRelegationData():
    # 更新本日静态数据
    datenow = getstartdate(0)

    StrFieds = ""
    tablename = "ecsjb"
    GetField = ["RQ", "JH", "DWMC", "CYQ", "DYFS", "CXFS", "SCFS", "ISCX", "JB", "SCZT", "ZYLX"]
    FieldName = ["RQ", "JH", "DWMC", "CYQ", "DYFS", "CXFS", "SCFS", "ISCX", "JB", "SCZT", "ZYLX"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = '%s'" % (StrFieds, tablename, "RQ", datenow)
    staticdata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_GBK)
    staticdata.columns = FieldName
    staticdata = staticdata.set_index(["JH"])

    # 删除已有数据
    tablename = "EcWellStaticDataHistory"
    del_data_sql = "DELETE FROM %s WHERE RQ = '%s'" % (tablename, datenow)
    executesql(SqlserverDataServre, del_data_sql)

    # 更新数据
    staticdata.to_sql(tablename, GlOBAL_mssql_engine_UTF8, if_exists='append')

    # 更新今日静态数据
    datebefore = getstartdate(10)

    StrFieds = ""
    tablename = "ecsjb"
    GetField = ["RQ", "JH", "DWMC", "CYQ", "DYFS", "CXFS", "SCFS", "ISCX", "JB", "SCZT", "ZYLX"]
    FieldName = ["RQ", "JH", "DWMC", "CYQ", "DYFS", "CXFS", "SCFS", "ISCX", "JB", "SCZT", "ZYLX"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s >= '%s'" % (StrFieds, tablename, "RQ", datebefore)
    staticdata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_GBK)
    staticdata.columns = FieldName
    staticdata.sort_values(by="RQ", inplace=True)
    staticdata.drop_duplicates(subset=["JH", "DWMC", "CYQ", "DYFS", "CXFS", "ZYLX"], keep='last', inplace=True)
    staticdata.fillna(method="ffill", inplace=True)
    staticdata.drop_duplicates(subset=["JH"], keep='last', inplace=True)
    staticdata = staticdata.set_index(["JH"])

    # 删除已有数据
    tablename = "EcWellStaticData"
    del_data_sql = "DELETE FROM %s " % tablename
    executesql(SqlserverDataServre, del_data_sql)

    # 更新数据
    staticdata.to_sql(tablename, GlOBAL_mssql_engine_UTF8, if_exists='append')


# 集合数据
# 集合二厂动态数据
def extract_erchang_well_dynicurve_data():
    global GLOBAL_Logger

    # GLOBAL_Logger.info("开始提取二厂单井曲线数据！")
    print("......................................................")
    print(getnowtime(), "开始提取二厂单井曲线数据!")

    extractdays = 10
    try:
        for exday in range(0, extractdays):
            # if True:
            #     exday = 4216
            ExtractErchangWellDynicurveData(exday)
            # print("前%s日二厂单井曲线数据提取完毕！" % str(exday + 1))
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 集合二厂动态数据
def ExtractErchangWellDynicurveData(exday):
    begintime = getstartdate(exday)

    # 生产日报
    StrFieds = ""
    tablename = "ecsjb"
    GetField = ["RQ", "JH", "DWMC", "CYQ", "SCSJ", "YZ", "PL1", "BJ", "BJ1", "BS", "PL", "CC", "CC1", "YY",
                "TY", "HY", "JKWD", "HHYL", "RCYL1", "RCYL", "RCSL", "RCXL", "HHHS", "HS", "DY", "DL",
                "SXDL", "XXDL", "RZSL", "RZQL", "RBSL", "BZ", "DYFS", "CXFS", "SCFS", "ISCX", "JB", "SCZT", "ZYLX",
                "SCC", "SCC1", "KXY", "RJNL", "Maxload", "CQL"]
    FieldName = ["RQ", "JH", "DWMC", "CYQ", "SCSJ", "YZ", "PL1", "BJ", "BJ1", "BS", "PL", "CC", "CC1", "YY",
                 "TY", "HY", "JKWD", "HHYL", "RCYL1", "RCYL", "RCSL", "RCXL", "HHHS", "HS", "DY", "DL",
                 "SXDL", "XXDL", "RZSL", "RZQL", "RBSL", "BZ", "DYFS", "CXFS", "SCFS", "ISCX", "JB", "SCZT", "ZYLX",
                 "SCC", "SCC1", "KXY", "RJNL", "Maxload", "CQL"]

    # GetField = ["RQ", "JH", "DWMC", "CYQ"]
    # FieldName = ["RQ", "JH", "DWMC", "CYQ"]

    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = '%s'" % (StrFieds, tablename, "RQ", begintime)
    dynamicdata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_GBK)
    dynamicdata.columns = FieldName
    dynamicdata = dynamicdata.set_index(["JH"])
    dynamicdata["XCB"] = dynamicdata["RCXL"] / dynamicdata["RCYL"]
    dynamicdata["BX"] = dynamicdata["HHYL"] / dynamicdata["PL"] * 100

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
    enleveldata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_GBK)
    # enleveldata = pd.DataFrame(enleveldata, columns=FieldName)
    enleveldata.columns = FieldName
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
    meteringdata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_GBK)

    # meteringdata = pd.DataFrame(meteringdata, columns=FieldName)
    meteringdata.columns = FieldName
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
    viscositydata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_GBK)
    # viscositydata = pd.DataFrame(viscositydata, columns=FieldName)
    viscositydata.columns = FieldName
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
    moisturedata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_GBK)
    # moisturedata = pd.DataFrame(moisturedata, columns=FieldName)
    moisturedata.columns = FieldName
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
    densitydata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_GBK)
    # densitydata = pd.DataFrame(moisturedata, columns=FieldName)
    # densitydata = pd.DataFrame(densitydata, columns=FieldName)
    densitydata.columns = FieldName
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

    # 删除已有数据
    tablename = "EcWellHisData"
    del_data_sql = "DELETE FROM %s WHERE %s = '%s'" % (tablename, "RQ", begintime)
    executesql(SqlserverDataServre, del_data_sql)

    # 删除已有数据
    dynamicdata.to_sql(tablename, GlOBAL_mssql_engine_UTF8, if_exists='append')

    print("%s日%s口井单井曲线数据更新完毕！" % (begintime, len(dynamicdata)))


# 提取注水注气小时数据
def extract_gas_water_injection_realtime_data():
    global GLOBAL_Logger

    GLOBAL_Logger.info("开始更新注水注气小时远传数据！")
    print("......................................................")
    print("开始更新数据！", getnowtime())

    extracthours = 24
    try:
        extimes = 0
        for exhour in range(0, extracthours):
            # for exhour in range(290, extracthours):
            extractgaswaterinjectionrealtimedata(exhour)
            print("前%s小时数据提取成功！" % str(exhour))
            extimes = extimes + 1
        GLOBAL_Logger.info("共%s小时实时数据提取完毕!" % str(extimes))
        print("共%s小时实时数据提取完成!" % str(extimes))
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


def extractgaswaterinjectionrealtimedata(hour):
    global GLOBAL_Logger
    global GlOBAL_oracle_engine_ecpro_pradayly
    global GlOBAL_mssql_engine_GBK
    # global GlOBAL_mssql_engine_GBK

    print('................................')
    print(getbegintime(hour * 60, 'minutes'))

    gas_min_data_info = {
        'tablename': "PCSYWXT20.SSC_ZQSB_SS",
        'fields': ['jh', 'cjsj', 'yhbzsyl', 'yhbssll', 'yhbljll', 'ehbzsyl', 'ehbssll', 'ehbljll', 'zqwd', 'dqcd', 'zqyl', 'zqssll', 'zqljll',
                   'sygzl', 'ygyl', 'tgyl', 'gsmc', 'gsdm', 'txzt', 'ycsbbh', 'sjzrl', 'zqlx'],
        'time': "cjsj",
        'database': 'ora'
    }

    check_data_info = {
        'tablename': "dbo.EcGasInjectHourly",
        'fields': ['wellname', 'cotime'],
        'time': "cotime",
        'database': 'mssql'
    }
    sync_data_info = {
        'tablename': "dbo.EcGasInjectHourly",
        'fields': ['wellname', 'cotime', 'zsyl', 'zsssll', 'zsssll1', 'zsljll', 'zqwd', 'zqnd', 'zqyl', 'zqssll', 'zqssll1', 'zqljll', 'yy', 'ty',
                   'dwmc', 'sjzrl', 'zqlx', 'sbbh'],
        'time': "cotime",
        'database': 'mssql'
    }

    # （1）提取关系库动态数据内原始数据

    begin_time = getbegintime((hour + 1) * 60, 'minutes')
    end_time = getbegintime(hour * 60, 'minutes')
    min_gas_data = extract_ora_data(GlOBAL_oracle_engine_ecpro_pradayly, gas_min_data_info, btime=begin_time, etime=end_time)

    uptime = datetime.fromtimestamp(time.time()) - relativedelta(minutes=hour * 60)
    uptime = time.strftime("%Y-%m-%d %H:00:00", uptime.timetuple())
    hour_gas_data = extract_gas_hour_data(min_gas_data, uptime)

    begin_time = getbegintime((hour + 2) * 60, 'minutes')
    end_time = getbegintime(hour - 2 * 60, 'minutes')
    check_data = extract_ora_data(GlOBAL_mssql_engine_GBK, check_data_info, btime=begin_time, etime=end_time)

    synchronize_gas_hour_data(hour_gas_data, check_data, SqlserverDataServre, sync_data_info)


# 数据提取
def extract_ora_data(engine, info, btime='', etime=''):
    tablename = info["tablename"]
    fields = info["fields"]
    timefield = info["time"]
    database_type = info["database"]

    if len(fields) == 0:
        str_fieds = "*"
    else:
        str_fieds = ""
        for field in fields:
            if str_fieds == "":
                str_fieds = field
            else:
                str_fieds = str_fieds + ", " + field

    if database_type == 'ora':
        if len(btime) == 0:
            get_data_sql = "SELECT %s FROM %s " % (str_fieds, tablename)
        else:
            if len(etime) == 0:
                get_data_sql = "SELECT %s FROM %s WHERE %s>=TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') " % \
                               (str_fieds, tablename, timefield, btime)
            else:
                get_data_sql = "SELECT %s FROM %s WHERE %s >=TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') AND %s <TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') " % \
                               (str_fieds, tablename, timefield, btime, timefield, etime)
    else:
        if len(btime) == 0:
            get_data_sql = "SELECT %s FROM %s " % (str_fieds, tablename)
        else:
            if len(etime) == 0:
                get_data_sql = "SELECT %s FROM %s WHERE %s>='%s' " % \
                               (str_fieds, tablename, timefield, btime)
            else:
                get_data_sql = "SELECT %s FROM %s WHERE %s >= '%s' AND %s < '%s' " % \
                               (str_fieds, tablename, timefield, btime, timefield, etime)
    realtimedata = pd.read_sql(get_data_sql, engine)

    return realtimedata


def extract_water_data(engine, info, btime='', etime=''):
    tablename = info["tablename"]
    fields = info["fields"]
    timefield = info["time"]
    database_type = info["database"]

    if len(fields) == 0:
        str_fieds = "*"
    else:
        str_fieds = ""
        for field in fields:
            if str_fieds == "":
                str_fieds = field
            else:
                str_fieds = str_fieds + ", " + field

    if database_type == 'ora':
        if len(btime) == 0:
            get_data_sql = "SELECT %s FROM %s " % (str_fieds, tablename)
        else:
            if len(etime) == 0:
                get_data_sql = "SELECT %s FROM %s WHERE %s>=TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') " % \
                               (str_fieds, tablename, timefield, btime)
            else:
                get_data_sql = "SELECT %s FROM %s WHERE %s >=TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') AND %s <TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') " % \
                               (str_fieds, tablename, timefield, btime, timefield, etime)
    else:
        if len(btime) == 0:
            get_data_sql = "SELECT %s FROM %s " % (str_fieds, tablename)
        else:
            if len(etime) == 0:
                get_data_sql = "SELECT %s FROM %s WHERE %s>='%s' " % \
                               (str_fieds, tablename, timefield, btime)
            else:
                get_data_sql = "SELECT %s FROM %s WHERE %s >= '%s' AND %s < '%s' " % \
                               (str_fieds, tablename, timefield, btime, timefield, etime)
    realtimedata = pd.read_sql(get_data_sql, engine)

    return realtimedata


# 提取注气小时数据
def extract_gas_hour_data(ordata, uptime):
    ordata.fillna(np.nan, inplace=True)

    exdata_df = {}
    zsyl_list = []
    zsssll_list = []
    zsssll1_list = []
    zsljll_list = []
    zqwd_list = []
    zqnd_list = []
    zqyl_list = []
    zqssll_list = []
    zqssll1_list = []
    zqljll_list = []
    yy_list = []
    ty_list = []
    dwmc_list = []
    sjzrl_list = []
    zqlx_list = []
    sbbh_list = []

    # 井号列、时间列
    welllist = list(set(ordata['jh'].values))

    # print(welllist)

    exdata_df["wellname"] = welllist
    exdata_df["cotime"] = uptime

    ordata = ordata.drop(columns=['txzt'])

    for wellname in welllist:
        welldata = ordata[ordata["jh"] == wellname]
        well_max_data = welldata.groupby(["jh"]).max()
        well_mean_data = welldata.groupby(["jh"]).mean()
        well_min_data = welldata.groupby(["jh"]).min()
        zsyl_list.append(max([well_max_data["yhbzsyl"].values[0], well_max_data["ehbzsyl"].values[0]]))
        zsssll_list.append(well_mean_data["yhbssll"].values[0] + well_mean_data["ehbssll"].values[0])
        zsssll1 = sum([well_max_data["yhbljll"].values[0] - well_min_data["yhbljll"].values[0],
                       well_max_data["ehbljll"].values[0] - well_min_data["ehbljll"].values[0]])
        zsssll1_list.append(zsssll1)
        zsljll_list.append(sum([well_max_data["yhbljll"].values[0], well_max_data["ehbljll"].values[0]]))
        zqwd_list.append(well_mean_data["zqwd"].values[0])
        zqnd_list.append(well_mean_data["dqcd"].values[0])
        zqyl_list.append(well_mean_data["zqyl"].values[0])
        zqssll_list.append(well_mean_data["zqssll"].values[0])
        zqssll1_list.append((well_max_data["zqljll"].values[0] - well_min_data["zqljll"].values[0]) * 10000)
        zqljll_list.append(well_max_data["zqljll"].values[0])
        yy_list.append(well_mean_data["ygyl"].values[0])
        ty_list.append(well_mean_data["tgyl"].values[0])
        dwmc_list.append(welldata["gsmc"].values[0])
        sjzrl_list.append(welldata["sjzrl"].values[0])
        zqlx_list.append(welldata["zqlx"].values[0])
        sbbh_list.append(welldata["ycsbbh"].values[0])

    exdata_df["zsyl"] = zsyl_list
    exdata_df["zsssll"] = zsssll_list
    exdata_df["zsssll1"] = zsssll1_list
    exdata_df["zsljll"] = zsljll_list
    exdata_df["zqwd"] = zqwd_list
    exdata_df["zqnd"] = zqnd_list
    exdata_df["zqyl"] = zqyl_list
    exdata_df["zqssll"] = zqssll_list
    exdata_df["zqssll1"] = zqssll1_list
    exdata_df["zqljll"] = zqljll_list
    exdata_df["yy"] = yy_list
    exdata_df["ty"] = ty_list
    exdata_df["dwmc"] = dwmc_list
    exdata_df["sjzrl"] = sjzrl_list
    exdata_df["zqlx"] = zqlx_list
    exdata_df["sbbh"] = sbbh_list

    exdata = pd.DataFrame(exdata_df)

    return exdata
    # return 1


# 同步注气小时数据
def synchronize_gas_hour_data(data, checkdata, server, info):
    # print(data)
    data.fillna("NULL", inplace=True)
    # print(data)
    checkdata["cotime"] = checkdata["cotime"].astype('str')

    def check_data_is_exist(well_name, check_time):
        welllist = list(set(checkdata['wellname'].values))
        if len(welllist) > 0:
            if well_name in welllist:
                well_check_list = checkdata[checkdata["wellname"] == well_name]
                well_check_list = list(set(well_check_list['cotime'].values))
                if check_time in well_check_list:
                    return True
                else:
                    return False
            else:
                return False
        else:
            return False

    # 同步更新数据
    tablename = info["tablename"]
    columns = info["fields"]
    if len(data) > 0:
        for i in range(len(data)):
            values = []
            meta_data = data.iloc[i, :]
            for j in range(len(meta_data)):
                values.append(meta_data[columns[j]])

            wellname = meta_data['wellname']
            checktime = meta_data['cotime']
            if check_data_is_exist(wellname, checktime):
                updatedata(server, tablename, columns, values, funtype='update')
            else:
                updatedata(server, tablename, columns, values, funtype='insert')


# 数据库函数
# 往数据库插入数据函数
def updatedata(server, tablename, fields, values, funtype="insert"):
    if funtype == "insert":
        str_fieds = ""
        for field in fields:
            if str_fieds == "":
                str_fieds = field
            else:
                str_fieds = str_fieds + ", " + field

        str_values = ""
        for value in values:
            if str_values == "":
                if str(value) == "NULL" or str(value).find('To_Date') >= 0:
                    str_values = str(value)
                else:
                    str_values = "'" + str(value) + "'"
            else:
                if str(value) == "NULL" or str(value).find('To_Date') >= 0:
                    str_values = str_values + ", " + str(value)
                else:
                    str_values = str_values + ", '" + str(value) + "'"

        update_data_sql = "INSERT INTO " + tablename + " (" + str_fieds + ") values(" + str_values + ")"
    else:
        str_fieds = ""
        for i in range(len(fields)):
            if str_fieds == "":
                if str(values[i]) == "NULL" or str(values[i]).find('To_Date') >= 0:
                    str_fieds = fields[i] + " = " + str(values[i])
                else:
                    str_fieds = fields[i] + " = '" + str(values[i]) + "'"
            else:
                if str(values[i]) == "NULL" or str(values[i]).find('To_Date') >= 0:
                    str_fieds = str_fieds + ", " + fields[i] + " = " + str(values[i])
                else:
                    str_fieds = str_fieds + ", " + fields[i] + " = '" + str(values[i]) + "'"
        update_data_sql = "UPDATE %s SET %s WHERE %s = '%s' and %s = '%s'" % (tablename, str_fieds, "wellname", values[0], "cotime", values[1])

    # print(update_data_sql)
    executesql(server, update_data_sql)


# 检查注水注气数据质量
def inspect_gas_water_injection_farpass_status():
    global GLOBAL_Logger

    GLOBAL_Logger.info("开始检查注水注气远传数据质量！")
    print("......................................................")
    print("开始检查数据！", getnowtime())

    extractdays = 1
    try:
        extimes = 0
        for exday in range(0, extractdays):
            # if True:
            #     exday = 4216
            inspectgaswaterinjectionfarpassstatus(exday)
            # print("完成注水注气远传数据质量检查！")
        GLOBAL_Logger.info("注水注气远传数据质量检查完毕!")
        print("注水注气远传数据质量检查完成!")
        # if extractdays - extimes > 0:
        #     print("共%s天注水注气远传数据质量检查失败!" % str(extractdays - extimes))
        #     GLOBAL_Logger.info("共%s天注水注气远传数据质量检查失败!" % str(extractdays - extimes))
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


def inspectgaswaterinjectionfarpassstatus(days):
    global GLOBAL_Logger
    # global GlOBAL_oracle_engine_
    # global GlOBAL_mssql_engine_GBK
    global GLOBAL_WellNameReference

    onpasswellname = []
    offpasswellname = []
    expiredwellname = []

    exdate = getstartdate(days + 3)
    print("开始更新%s日日报原始参数" % exdate)

    # 日报动态数据，确定注气井号
    tablename = "cyec.dbo.ecsjb"
    fields = ["JH", "RQ", "ZSSJ", "RZSL", "ZQSJ", "RZQL", "RBSL"]
    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE RQ>'%s'" % \
                   (str_fieds, tablename, exdate)
    dynimicdata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_GBK)
    gasdynimicdata = dynimicdata[dynimicdata["ZQSJ"] >= 0]
    gaswellname = list(gasdynimicdata["JH"].values)
    gaswellname = list(set(gaswellname))
    # waterdynimicdata = dynimicdata[dynimicdata["ZSSJ"] >= 0]
    # waterdynimicwellname = list(waterdynimicdata["JH"].values)
    # waterdynimicwellname = list(set(waterdynimicwellname))
    # print(gaswellname)

    # 远传数据，确定注气远传井号
    exdate = getstartdate(days + 1)
    tablename = "PCSYWXT20.SSC_ZQSB_SS"
    fields = ["JH", "CJSJ", "ZQSSLL", "ZQLJLL", "YGYL", "TGYL"]
    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE CJSJ>TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') " % \
                   (str_fieds, tablename, exdate)
    realtimedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine_ecpro_pradayly)
    gasrealtimedata = realtimedata[realtimedata["zqssll"] >= 0]
    gasrealtimewellname = list(gasrealtimedata["jh"].values)
    gasrealtimewellname = list(set(gasrealtimewellname))
    # print(gasrealtimewellname)

    # tablename = "PCSYWXT20.SSC_ZSSB_SS"
    # fields = ["JH", "CJSJ", "SSLL", "LJLL", "YY", "TY"]
    # str_fieds = ""
    # for field in fields:
    #     if str_fieds == "":
    #         str_fieds = field
    #     else:
    #         str_fieds = str_fieds + ", " + field
    # get_data_sql = "SELECT %s FROM %s WHERE CJSJ>TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') " % \
    #                (str_fieds, tablename, exdate)
    # realtimedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine)
    # waterrealtimedata = realtimedata[realtimedata["ssll"] >= 0]
    # waterrealtimewellname = list(waterrealtimedata["jh"].values)
    # waterrealtimewellname = list(set(waterrealtimewellname))
    # # print(gasrealtimewellname)

    # 对比井号，确定远传情况
    for well in gaswellname:
        if well in gasrealtimewellname:
            onpasswellname.append(well)
        else:
            offpasswellname.append(well)
    for well in gasrealtimewellname:
        if well not in gaswellname:
            expiredwellname.append(well)

    # print(onpasswellname)
    # print(offpasswellname)
    # print(expiredwellname)

    gdate = time.strftime("%Y年%m月%d日%H点%M分", datetime.fromtimestamp(time.time()).timetuple())
    gonnum = len(onpasswellname)
    goffnum = len(offpasswellname)
    gofftext = ""
    if goffnum > 0:
        for tx in offpasswellname:
            if gofftext == "":
                gofftext = tx
            else:
                gofftext = gofftext + ", " + tx

    gexpirednum = len(expiredwellname)
    gexpiredtext = ""
    if gexpirednum > 0:
        for tx in expiredwellname:
            if gexpiredtext == "":
                gexpiredtext = tx
            else:
                gexpiredtext = gexpiredtext + ", " + tx

    adjusttext = "注气井远传情况公告：\n截至%s，共有注气井%s口，其中远传正常井%s口，远传掉线井%s口" % (gdate, (gonnum + goffnum), gonnum, goffnum)
    if goffnum == 0:
        adjusttext = adjusttext + "。"
    else:
        adjusttext = adjusttext + ",请尽快运维上线。\n掉线井井号为：%s" % gofftext
    if gexpirednum == 0:
        adjusttext = adjusttext + "!"
    else:
        adjusttext = adjusttext + "!\n已停注，未摘除设备井号为：%s!\n" % gexpiredtext

    print(adjusttext)

    post_data = {
        "msgtype": "text",
        "text": {"content": adjusttext},
        # "at": {"isAtAll": True}
    }
    postdata(GLOBLE_gaswaterrob_url, post_data)


# 推送消息
def postdata(url, post_data):
    global GLOBLE_headers

    isnotconnect = True
    num = 0

    while isnotconnect:
        num = num + 1
        if num > 10:
            break
        try:
            response = requests.post(url, headers=GLOBLE_headers, data=json.dumps(post_data))
            response_str = response.text
            response_dict = json.loads(response_str)
            if ("errcode" in response_dict) and response_dict["errcode"] == 0:
                isnotconnect = False
            else:
                isnotconnect = True
        except Exception as e:
            # print(response)
            # print(response_str)
            # print(response_dict)
            print(traceback.format_exc())
            GLOBAL_Logger.info(traceback.format_exc())
            time.sleep(5)
            continue


# 提取注水注气小时数据
def extract_water_injection_realtime_data():
    global GLOBAL_Logger

    GLOBAL_Logger.info("开始更新注水注气小时远传数据！")
    print("......................................................")
    print("开始更新数据！", getnowtime())

    extracthours = 6
    try:
        extimes = 0
        for exhour in range(0, extracthours):
            # for exhour in range(290, extracthours):
            extractwaterinjectionrealtimedata(exhour)
            print("前%s小时数据提取成功！" % str(exhour))
            extimes = extimes + 1
        GLOBAL_Logger.info("共%s小时实时数据提取完毕!" % str(extimes))
        print("共%s小时实时数据提取完成!" % str(extimes))
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


def extractwaterinjectionrealtimedata(hour):
    global GLOBAL_Logger
    # global GlOBAL_oracle_engine_ecpro_pradayly
    # global GlOBAL_mssql_engine_GBK
    # global GlOBAL_mssql_engine_GBK

    print('................................')
    print(getbegintime(hour * 60, 'minutes'))

    water_hour_data_info = {
        'tablename': "dbo.EcWaterInjectHourly_Manual",
        'fields': ['jh', 'sj', 'YY', 'TY', 'ZRL', 'LZL'],
        'time': "sj",
        'database': 'mssql'
    }

    water_static_data_info = {
        'tablename': "dbo.EcWaterStaticData_Manual",
        'fields': ['DWMC', 'ZRFS', 'ZSLX', 'GSFS', 'SY', 'ZSSB', 'SBLX', 'ZGYY', 'ZGTY', 'SJQD',
                   'SJSL', 'ZSFL', 'BZDJ', 'JLFS', 'BZ', 'ZSZT', 'WZSJ', 'JH'],
        'time': "ZSZT",
        'database': 'mssql'
    }

    check_data_info = {
        'tablename': "dbo.EcWaterInjectDaily_Manual",
        'fields': ['wellname', 'cotime'],
        'time': "cotime",
        'database': 'mssql'
    }

    sync_data_info = {
        'tablename': "dbo.EcWaterInjectDaily_Manual",
        'fields': ['wellname', 'cotime', 'DWMC', 'ZRFS', 'ZSLX', 'GSFS', 'SY', 'ZSSB', 'SBLX', 'ZGYY', 'ZGTY', 'SJQD',
                   'SJSL', 'ZSFL', 'BZDJ', 'JLFS', 'BZ', 'ZSZT', 'WZSJ', 'yy', 'ty', 'dayinj', 'suminj'],
        'time': "cotime",
        'database': 'mssql'
    }

    def extract_water_static_data(engine, info):
        tablename = info["tablename"]
        fields = info["fields"]

        if len(fields) == 0:
            str_fieds = "*"
        else:
            str_fieds = ""
            for field in fields:
                if str_fieds == "":
                    str_fieds = field
                else:
                    str_fieds = str_fieds + ", " + field

        get_data_sql = "SELECT %s FROM %s " % (str_fieds, tablename)
        staticdata = pd.read_sql(get_data_sql, engine)
        return staticdata

    def extract_water_check_data(engine, info, checktime):
        tablename = info["tablename"]
        fields = info["fields"]
        checkfield = info["time"]

        if len(fields) == 0:
            str_fieds = "*"
        else:
            str_fieds = ""
            for field in fields:
                if str_fieds == "":
                    str_fieds = field
                else:
                    str_fieds = str_fieds + ", " + field

        get_data_sql = "SELECT %s FROM %s WHERE %s = '%s'" % (str_fieds, tablename, checkfield, checktime)

        staticdata = pd.read_sql(get_data_sql, engine)
        return staticdata

    def extract_daily_water_data(staticydata, hourdata, time):

        def extract_water_daily_data(ordata, uptime):
            ordata.fillna(np.nan, inplace=True)

            exdata_df = {}
            yy_list = []
            ty_list = []
            zsl_list = []
            lzl_list = []

            # 井号列、时间列
            welllist = list(set(ordata['jh'].values))

            # print(welllist)

            exdata_df["JH"] = welllist
            # exdata_df["cotime"] = uptime

            # ordata = ordata.drop(columns=['zszt'])

            for wellname in welllist:
                welldata = ordata[ordata["jh"] == wellname]
                well_max_data = welldata.groupby(["jh"]).max()
                well_mean_data = welldata.groupby(["jh"]).mean()

                yy_list.append(well_mean_data["YY"].values[0])
                ty_list.append(well_mean_data["TY"].values[0])
                zsl_list.append(well_mean_data["ZRL"].values[0])
                lzl_list.append(well_max_data["LZL"].values[0])

            exdata_df["yy"] = yy_list
            exdata_df["ty"] = ty_list
            exdata_df["dayinj"] = zsl_list
            exdata_df["suminj"] = lzl_list

            exdata = pd.DataFrame(exdata_df)
            exdata.set_index(["JH"], inplace=True)

            return exdata

        staticydata = staticydata[staticydata['ZSZT'] == "在注"]
        staticydata.set_index(['JH'], inplace=True)

        daily_data = extract_water_daily_data(hourdata, time)
        daily_data = pd.concat([staticydata, daily_data], axis=1, join='outer')
        daily_data["wellname"] = daily_data.index
        daily_data["cotime"] = time
        return daily_data

    def synchronize_water_daily_data(data, checkdata, server, info):
        # print(data)
        data.fillna("NULL", inplace=True)
        # print(data)
        checkdata["cotime"] = checkdata["cotime"].astype('str')

        def check_data_is_exist(well_name, check_time):
            welllist = list(set(checkdata['wellname'].values))
            if len(welllist) > 0:
                if well_name in welllist:
                    well_check_list = checkdata[checkdata["wellname"] == well_name]
                    well_check_list = list(set(well_check_list['cotime'].values))
                    if check_time in well_check_list:
                        return True
                    else:
                        return False
                else:
                    return False
            else:
                return False

        # 同步更新数据
        tablename = info["tablename"]
        columns = info["fields"]
        if len(data) > 0:
            for i in range(len(data)):
                values = []
                meta_data = data.iloc[i, :]
                # print(meta_data)

                for j in range(len(meta_data)):
                    values.append(meta_data[columns[j]])

                # print(columns)
                # print(values)
                wellname = meta_data['wellname']
                checktime = meta_data['cotime']
                if check_data_is_exist(wellname, checktime):
                    updatedata(server, tablename, columns, values, funtype='update')
                else:
                    updatedata(server, tablename, columns, values, funtype='insert')

    begin_time = getbegintime((hour + 24) * 60, 'minutes')
    end_time = getbegintime(hour * 60, 'minutes')
    hour_water_data = extract_ora_data(GlOBAL_mssql_engine_GBK, water_hour_data_info, btime=begin_time, etime=end_time)
    # print(hour_water_data)

    staticy_water_data = extract_water_static_data(GlOBAL_mssql_engine_GBK, water_static_data_info)
    # print(staticy_water_data)

    uptime = datetime.fromtimestamp(time.time()) - relativedelta(minutes=hour * 60)
    uptime = time.strftime("%Y-%m-%d %H:00:00", uptime.timetuple())
    daily_water_data = extract_daily_water_data(staticy_water_data, hour_water_data, uptime)
    # print(daily_water_data)

    check_data = extract_water_check_data(GlOBAL_mssql_engine_GBK, check_data_info, uptime)
    # print(check_data)

    synchronize_water_daily_data(daily_water_data, check_data, SqlserverDataServre, sync_data_info)


if __name__ == '__main__':
    print("开始初始化数据")
    initconfig()

    # extract_erchang_well_dynicurve_data()
    # extract_erchang_well_relegation_data()
    # extract_erchang_well_assay_data()
    # extract_erchang_well_hour_monitor_data()
    # extract_erchang_well_daily_monitor_data()
    # extract_erchang_well_daily_monitor_chanxi_data()
    # inspect_gas_water_injection_farpass_status()
    # extract_water_injection_realtime_data()

    print("开始迭代更新数据")
    scheduler = BackgroundScheduler()
    # 提取单井参数数据
    # 集合数据，生成动态集合数据，可供单井曲线使用
    job1 = scheduler.add_job(extract_erchang_well_dynicurve_data, 'cron', minute=6)
    # 提取油井静态归属关系
    job2 = scheduler.add_job(extract_erchang_well_relegation_data, 'cron', minute=5)
    # 提取化验数据，包括动静压力、动静液面、手测功图数据
    job3 = scheduler.add_job(extract_erchang_well_assay_data, 'interval', hours=2)
    # 每小时归集远传数据，生成小时动态数据
    job4 = scheduler.add_job(extract_erchang_well_hour_monitor_data, 'cron', minute=4)
    # 每天归集小时动态数据，生成日度动态数据，包括功图数据及单井参数数据
    job5 = scheduler.add_job(extract_erchang_well_daily_monitor_data, 'cron', hour=11, minute=5)
    # 每天归集小时动态数据，生成日度动态数据，包括站库掺稀数据
    job6 = scheduler.add_job(extract_erchang_well_daily_monitor_chanxi_data, 'cron', hour=16, minute=5)
    # 每天推送注气井远传状态信息
    job7 = scheduler.add_job(inspect_gas_water_injection_farpass_status, 'cron', hour=18)
    # 每天小时归集注气24小时小时数据
    job8 = scheduler.add_job(extract_gas_water_injection_realtime_data, 'cron', minute=50)
    # 每天小时归集注气24小时小时数据
    job9 = scheduler.add_job(extract_water_injection_realtime_data, 'cron', minute=50)

    scheduler.start()

    while True:
        time.sleep(0.1)
