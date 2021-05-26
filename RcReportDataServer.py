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

pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 50)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


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
    global GlOBAL_oracle_engine_bupro
    global GlOBAL_mssql_engine_GBK
    global GlOBAL_mssql_engine_UTF8
    global SqlserverDataServre

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
    daily_oracle_data_servre = {"user": 'yjywj',
                                "password": 'WJadmin#$2021002',
                                "dsn_tns": dsn_tns1,
                                "servertype": 'oracle'}

    # 采油厂实时生产数据库连接
    GlOBAL_oracle_engine_ecpro = create_engine('oracle+cx_oracle://%s:%s@%s' % (
        sensor_oracle_data_servre["user"], sensor_oracle_data_servre["password"], sensor_oracle_data_servre["dsn_tns"]))
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

    extracthours = 3
    try:
        for exhour in range(1, extracthours):
            # if True:
            #     exday = 4216
            ExtractEcWellAutoIndicatorDataHour(exhour)
            ExtractEcWellChanxiDataHour(exhour)
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

    # print(shicedata)

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

        # print(shicedata)

        # 删除已有数据
        tablename = "EcWellIndicatorAutoHour"
        del_data_sql = "DELETE FROM %s WHERE %s = '%s'" % (tablename, "date", endtime)
        executesql(SqlserverDataServre, del_data_sql)

        # # 写入测试数据
        shicedata.to_sql(tablename, GlOBAL_mssql_engine_UTF8, if_exists='append')


# 转储二厂单井掺稀小时数据
def ExtractEcWellChanxiDataHour(exhour):
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
    realtimedata.sort_values("time")

    get_data_sql = "SELECT %s FROM %s WHERE cjsj>TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') AND cjsj <= TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" % \
                   (str_fieds, tablename, exbegintime, begintime)
    exrealtimedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine_ecpro)
    exrealtimedata.columns = FieldName
    exrealtimedata.sort_values("time")

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

    realtimedata["time"] = endtime

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

    realtimedata = pd.concat([staticdata, realtimedata], axis=1, join='inner')

    # 删除已有数据
    tablename = "EcWellChanxiRealtimeAutoHour"
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
        for exday in range(1, extracdays):
            # if True:
            #     exday = 4216
            ExtractEcWellAutoIndicatorDataDay(exday)
            ExtractEcWellChanxiDataDay(exday)
            print("%s 二厂单井日度监测转储完毕！" % getbegintime(exday, "days"))
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 转储二厂单井自动功图数据
def ExtractEcWellAutoIndicatorDataDay(exday):
    begintime = getbegintime(exday, "days")
    endtime = getbegintime(exday - 1, "days")
    uptime = getstartdate(exday - 1)
    # print(begintime, endtime)
    #
    # 自动功图数据
    StrFieds = ""
    tablename = "EcWellIndicatorAutoHour"
    GetField = ["wellname", "date", "stroke", "speed", "minload", "maxload"]
    # ShishiFieldName = ["wellname", "date", "stroke", "speed", "minload", "maxload"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s >= '%s' and %s <= '%s'" % (StrFieds, tablename, "date", begintime, "date", endtime)
    shicedata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_UTF8)
    # shicedata.columns = ShishiFieldName
    if len(shicedata) > 0:
        shicedata = shicedata.groupby("wellname").mean()
        shicedata = shicedata.round(1)
        shicedata["date"] = uptime
        # print(shicedata)

        # 删除已有数据
        tablename = "EcWellIndicatorAutoDaily"
        del_data_sql = "DELETE FROM %s WHERE %s = '%s'" % (tablename, "date", uptime)
        executesql(SqlserverDataServre, del_data_sql)

        # 写入测试数据
        shicedata.to_sql(tablename, GlOBAL_mssql_engine_UTF8, if_exists='append')


# 转储二厂单井掺稀日度数据
def ExtractEcWellChanxiDataDay(exday):
    begintime = getbegintime(exday, "days")
    endtime = getbegintime(exday - 1, "days")
    uptime = getstartdate(exday - 1)
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
    dailydata.sort_values("date")

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
    StrFieds = ""
    tablename = "ecsjb"
    GetField = ["RQ", "JH", "DWMC", "CYQ", "DYFS", "CXFS", "SCFS", "ISCX", "JB", "SCZT", "ZYLX"]
    FieldName = ["RQ", "JH", "DWMC", "CYQ", "DYFS", "CXFS", "SCFS", "ISCX", "JB", "SCZT", "ZYLX"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s >= '%s'" % (StrFieds, tablename, "RQ", getstartdate(10))
    staticdata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_GBK)
    staticdata.columns = FieldName
    # staticdata = pd.DataFrame(staticdata, columns=FieldName)
    staticdata.sort_values(by="RQ", inplace=True)
    staticdata.drop_duplicates(subset=["JH", "DWMC", "CYQ", "DYFS", "CXFS", "ZYLX"], keep='last', inplace=True)
    staticdata.fillna(method="ffill", inplace=True)
    staticdata.drop_duplicates(subset=["JH"], keep='last', inplace=True)
    # staticdata["JH"] = staticdata["JH"].str.encode('latin1').str.decode('gbk')
    staticdata = staticdata.set_index(["JH"])
    # print(staticdata)

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
        for exday in range(1, extractdays):
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
    dynamicdata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine_GBK)
    # dynamicdata = pd.DataFrame(dynamicdata, columns=FieldName)
    dynamicdata.columns = FieldName
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

    # print(dynamicdata)
    #
    # print(dynamicdata.loc[:, ["RQ", "MD"]])

    # 删除已有数据
    tablename = "EcWellHisData"
    del_data_sql = "DELETE FROM %s WHERE %s = '%s'" % (tablename, "RQ", begintime)
    executesql(SqlserverDataServre, del_data_sql)

    # 删除已有数据
    dynamicdata.to_sql(tablename, GlOBAL_mssql_engine_UTF8, if_exists='append')

    print("%s日%s口井单井曲线数据更新完毕！" % (begintime, len(dynamicdata)))


if __name__ == '__main__':
    print("开始初始化数据")
    initconfig()

    # extract_erchang_well_dynicurve_data()
    # extract_erchang_well_relegation_data()
    # extract_erchang_well_assay_data()
    # extract_erchang_well_hour_monitor_data()
    # extract_erchang_well_daily_monitor_data()

    # extract_tanker_shipment_data()
    # extract_well_realtime_data()
    # extract_level_date()

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
    # 每天归集小时动态数据，生成日度动态数据，包括功图数据及站库掺稀数据
    job5 = scheduler.add_job(extract_erchang_well_daily_monitor_data, 'cron', hour=16, minute=5)
    scheduler.start()

    while True:
        time.sleep(0.1)
