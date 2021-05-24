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

GLOBAL_Logger = None

GLOBAL_WellNameReference = {}

SqlserverDataServre = {"host": '10.16.192.40',
                       "user": 'zhangdkl',
                       "password": '4687607',
                       "database": 'cyec',
                       "charset": 'utf8',
                       "servertype": 'sqlserver'}

SqlserverDataServre_BRU = {"host": '10.16.35.197',
                           "user": 'kfdb',
                           "password": 'kfdb001',
                           "database": 'kfdb',
                           "charset": 'utf8',
                           "servertype": 'sqlserver'}

dsn_tns = cx_Oracle.makedsn('10.16.192.49', 1521, 'ORCL')
sensor_oracle_data_servre = {"user": 'sssjzc',
                             "password": 'sssjzc',
                             "dsn_tns": dsn_tns,
                             "servertype": 'oracle'}

dsn_tns1 = cx_Oracle.makedsn('10.16.3.34', 1521, service_name='kfdb')
daily_oracle_data_servre = {"user": 'YJYJY',
                            "password": 'JYadmin#$2021',
                            "dsn_tns": dsn_tns1,
                            "servertype": 'oracle'}

# oracle地面防腐数据库
dsn_tns2 = cx_Oracle.makedsn('10.16.128.13', 8057, 'cygcorc')
pipeline_oracle_data_servre = {"user": 'yqjscx',
                               "password": 'cyec_607',
                               "dsn_tns": dsn_tns2,
                               "servertype": 'oracle'}

# oracle智能泵房三倒数据库
dsn_tns3 = cx_Oracle.makedsn('10.16.128.18', 1521, service_name='njsjk')

tanker_shipment_oracle_data_servre = {"user": 'dyec',
                                      "password": 'dyec2018',
                                      "dsn_tns": dsn_tns3,
                                      "servertype": 'oracle'}


# 获取正式井号
def getofficialwellnames():
    tablename = "ecsjb"
    date = time.strftime("%Y-%m-%d", (datetime.fromtimestamp(time.time()) - relativedelta(days=3)).timetuple())
    welldata_sql = "SELECT DISTINCT JH FROM %s WHERE RQ>='%s'" % (tablename, date)
    wellname = getdatasql(SqlserverDataServre, welldata_sql)
    # wellname = [x[0].encode('latin1').decode('gbk') for x in wellname]
    wellname = [x[0] for x in wellname]
    return wellname


def initconfig():
    global GLOBAL_Logger
    global GLOBAL_OfficiaWellNames
    global GLOBAL_WellNameReference
    global GlOBAL_oracle_engine
    global GlOBAL_mssql_engine
    global GlOBAL_mssql_engine_brU
    global GlOBAL_pipeline_oracle_engine
    global GlOBAL_tanker_shipment_oracle_engine

    GLOBAL_Logger = setlog()

    GLOBAL_OfficiaWellNames = getofficialwellnames()

    GlOBAL_oracle_engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (
        sensor_oracle_data_servre["user"], sensor_oracle_data_servre["password"], sensor_oracle_data_servre["dsn_tns"]))
    GlOBAL_mssql_engine = create_engine('mssql+pymssql://%s:%s@%s/%s' % (
        SqlserverDataServre["user"], SqlserverDataServre["password"], SqlserverDataServre["host"],
        SqlserverDataServre["database"]), connect_args={'charset': 'GBK'})
    GlOBAL_mssql_engine_brU = create_engine('mssql+pymssql://%s:%s@%s/%s' % (
        SqlserverDataServre_BRU["user"], SqlserverDataServre_BRU["password"], SqlserverDataServre_BRU["host"],
        SqlserverDataServre_BRU["database"]))
    GlOBAL_pipeline_oracle_engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (
        pipeline_oracle_data_servre["user"], pipeline_oracle_data_servre["password"],
        pipeline_oracle_data_servre["dsn_tns"]))
    GlOBAL_tanker_shipment_oracle_engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (
        tanker_shipment_oracle_data_servre["user"], tanker_shipment_oracle_data_servre["password"],
        tanker_shipment_oracle_data_servre["dsn_tns"]))

    GLOBAL_WellNameReference = getwellnamereference()


# 日志函数
# 返回日志文件
def setlog():
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

    return logger


# 数据库函数
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


# 日期转化函数
# 返回目前日期
def getnowtime():
    NowDate = time.strftime("%Y-%m-%d %H:%M:%S", datetime.fromtimestamp(time.time()).timetuple())
    return NowDate


# 返回间隔前日期
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


# 获取日报静态数据
def getdailydata(server, tablename, GetField, GetFieldName, gaptime=0):
    begintime = getbegintime(gaptime, "minutes")

    StrFieds = ""

    # 取数据
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    if gaptime == 0:
        get_data_sql = "SELECT DISTINCT %s FROM %s" % (StrFieds, tablename)
    else:
        get_data_sql = "SELECT DISTINCT %s FROM %s WHERE %s>='%s' ORDER BY %s DESC " % (
            StrFieds, tablename, GetField[0], begintime, GetField[0])

    data = getdatasql(server, get_data_sql)

    if len(data) == 0:
        data = pd.DataFrame([], columns=GetFieldName)
    else:
        data = pd.DataFrame(data, columns=GetFieldName)
        if len(GetFieldName) > 1:
            data.set_index(GetFieldName[1], inplace=True)
            data = data.fillna(0)
    return data


# 获取实时远传数据
def getrealtimewellnamedata(server, tablename, GetField, GetFieldName, gaptime):
    begintime = getbegintime(gaptime, "minutes")

    StrFieds = ""

    # 取数据
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field

    if server["servertype"] == "oracle":
        get_data_sql = "SELECT %s FROM %s WHERE %s>TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" \
                       % (StrFieds, tablename, GetField[0], begintime)
    else:
        get_data_sql = "SELECT %s FROM %s WHERE %s>'%s'" % (
            StrFieds, tablename, GetField[0], begintime)

    data = getdatasql(server, get_data_sql)

    if len(data) == 0:
        data = pd.DataFrame([], columns=GetFieldName)
    else:
        data = pd.DataFrame(data, columns=GetFieldName)
        data.set_index(['time'], inplace=True)

    return data


# 获取井号对照表
def getwellnamereference():
    global GLOBAL_OfficiaWellNames
    global GlOBAL_oracle_engine

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

    return namereference


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


def generatedailydata():
    global GLOBAL_Logger
    global GLOBAL_WellNameReference

    # 数据提取日期
    date = time.strftime("%Y-%m-%d", datetime.fromtimestamp(time.time()).timetuple())
    # 三天日报所有正式井号
    wellnames = getofficialwellnames()

    # 三天静态日报数据
    tablename = "ecsjb"
    GetField = ["RQ", "JH", "SCZT", "CXFS", "DYFS", "SCFS", "DWMC"]
    GetFieldName = ["date", "wellname", "wellStatus", "chanxiStation", "productStation", "productStatus", "area"]
    dailystaticdata = getdailydata(SqlserverDataServre, tablename, GetField, GetFieldName, 3 * 24 * 60)

    # 三天动态日报数据
    tablename = "ecsjb"
    GetField = ["RQ", "JH", "RCYL", "RCSL", "RCXL", "SCSJ"]
    GetFieldName = ["date", "wellname", "chanyou", "chanshui", "chanxi", "chanshi"]
    dailyvaluedata = getdailydata(SqlserverDataServre, tablename, GetField, GetFieldName, 3 * 24 * 60)

    # 密度数据
    tablename = "EcWellDensity"
    GetField = ["testdate", "wellname", "standarddensity"]
    GetFieldName = ["densitydate", "wellname", "density"]
    densitydata = getdailydata(SqlserverDataServre, tablename, GetField, GetFieldName, 30 * 24 * 60)
    densitydata = densitydata[densitydata["density"] > 0]
    densitydata['densitydate'] = densitydata['densitydate'].astype('str')

    # 粘度数据
    tablename = "EcWellViscosity"
    GetField = ["testdate", "wellname", "viscositywell"]
    GetFieldName = ["viscositydate", "wellname", "viscosity"]
    viscositydata = getdailydata(SqlserverDataServre, tablename, GetField, GetFieldName, 30 * 24 * 60)
    viscositydata = viscositydata[viscositydata["viscosity"] > 0]
    viscositydata['viscositydate'] = viscositydata['viscositydate'].astype('str')

    # # 远传井号
    # tablename = "ssc_yj_ss"
    # GetField = ["CJSJ", "JH"]
    # GetFieldName = ["time", "wellname"]
    # realtimedata = getrealtimewellnamedata(sensor_oracle_data_servre, tablename, GetField, GetFieldName, 5)
    # aliasname = [x[0] for x in list(realtimedata.values)]
    # aliasname = list(set(aliasname))
    # wellnamereference = getwellnamereference(aliasname)

    # 静态数据库已有井号
    tablename = "WellParemeterStatic"
    GetField = ["wellname"]
    GetFieldName = ["wellname"]
    staticwellname = [x[0] for x in getdailydata(SqlserverDataServre, tablename, GetField, GetFieldName, 0).values]

    # 逐井计算
    for wellname in wellnames:
        fields = ["date", "wellname"]
        values = [date, wellname]

        # 取上报字段对应数据
        welldailystaticdata = dailystaticdata.loc[wellname]
        welldailyvaluedata = dailyvaluedata.loc[wellname]

        # 日报静态数据
        tempfields = ["chanxiStation", "productStation", "productStatus", "area"]
        for i in range(len(tempfields)):
            fields.append(tempfields[i])
            if len(welldailystaticdata.shape) == 2:
                values.append(welldailystaticdata[tempfields[i]].values[0])
            else:
                values.append(welldailystaticdata[tempfields[i]])

        # 日报动态数据
        fields.append("chanshi")
        chanshi = welldailyvaluedata["chanshi"].mean()
        values.append(chanshi)

        tempfields = ["chanyou", "chanshui", "chanxi"]
        for i in range(len(tempfields)):
            fields.append(tempfields[i])
            if chanshi > 0:
                values.append(welldailyvaluedata[tempfields[i]].mean() / chanshi * 24)
            else:
                values.append(0.0)
        fields.append("chanxiPhour")
        if chanshi > 0:
            values.append(welldailyvaluedata["chanxi"].mean() / chanshi)
        else:
            values.append(0.0)

        fields.append("wellStatus")
        if len(welldailystaticdata.shape) == 2:
            wellstatus = welldailystaticdata["wellStatus"].values[0]
        else:
            wellstatus = welldailystaticdata["wellStatus"]
        if chanshi == 0:
            if wellstatus == "开井":
                wellstatus = "关井"
        values.append(wellstatus)

        # 密度数据
        tempfields = ["densitydate", "density"]
        if wellname in densitydata.index:
            welldensitydata = densitydata.loc[wellname]
            for i in range(len(tempfields)):
                fields.append(tempfields[i])
                if len(welldensitydata.shape) == 2:
                    values.append(welldensitydata[tempfields[i]].values[0])
                else:
                    values.append(welldensitydata[tempfields[i]])

        # 粘温数据
        tempfields = ["viscositydate", "viscosity"]
        if wellname in viscositydata.index:
            wellviscositydata = viscositydata.loc[wellname]
            for i in range(len(tempfields)):
                fields.append(tempfields[i])
                if len(wellviscositydata.shape) == 2:
                    values.append(wellviscositydata[tempfields[i]].values[0])
                else:
                    values.append(wellviscositydata[tempfields[i]])

        # 远传井号
        if wellname in GLOBAL_WellNameReference:
            fields.append("alias")
            values.append(GLOBAL_WellNameReference[wellname])

        # 上报数据
        tablename = "WellParemeterStatic"
        if wellname in staticwellname:
            StrFieds = ""
            for i in range(len(fields)):
                if StrFieds == "":
                    StrFieds = fields[i] + " = '" + str(values[i]) + "'"
                else:
                    StrFieds = StrFieds + ", " + fields[i] + " = '" + str(values[i]) + "'"

            update_data_sql = "UPDATE %s SET %s WHERE %s = '%s'" % (tablename, StrFieds, "wellname", wellname)
            executesql(SqlserverDataServre, update_data_sql)
        else:
            insertdata(SqlserverDataServre, tablename, fields, values)


# 返回间隔前日期
def getstartdate(durtime):
    NowTime = time.time()
    DateBeginDay = datetime.fromtimestamp(NowTime) - relativedelta(days=durtime)
    StrBeginDay = time.strftime("%Y-%m-%d 00:00:00", DateBeginDay.timetuple())
    return StrBeginDay


# 迁移液面流压数据
def generateleveldate():
    for benum in range(0, 10):
        begintime = getstartdate(benum)

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
        yemiandata = getdatasql(daily_oracle_data_servre, get_data_sql)
        yemiandata = pd.DataFrame(yemiandata, columns=LevelFieldName)
        yemiandata = yemiandata.set_index(["wellname", "date"])

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
        yalidata = getdatasql(daily_oracle_data_servre, get_data_sql)
        yalidata = pd.DataFrame(yalidata, columns=PresureFieldName)
        yalidata = yalidata.set_index(["wellname", "date"])

        data = pd.concat([yemiandata, yalidata], sort=True)
        data = data.reset_index(level=['date', "wellname"])
        data = data.fillna("")

        # print(yalidata)

        # 上报数据
        tablename = "cyec.dbo.EcWellTest"
        fields = LevelFieldName
        fields.append("dynamicpressure")
        fields.append("staticpressure")
        datalen = len(data)
        for j in range(datalen):
            wellname = data.loc[j]["wellname"]
            datatime = data.loc[j]["date"]
            values = []
            for field in fields:
                # print("------------------------------------------")
                # print(field)
                value = data.loc[j][field]
                if value == "":
                    value = "NULL"
                # print(value)
                values.append(value)

            get_data_sql = "SELECT * FROM %s WHERE %s = '%s' AND %s = '%s'" % (
                tablename, "date", datatime, "wellname", wellname)
            checkdata = getdatasql(SqlserverDataServre, get_data_sql)
            if len(checkdata) > 0:
                StrFieds = ""
                for i in range(len(fields)):
                    if StrFieds == "":
                        if str(values[i]) == "NULL":
                            StrFieds = fields[i] + " = " + str(values[i])
                        else:
                            StrFieds = fields[i] + " = '" + str(values[i]) + "'"
                    else:
                        if str(values[i]) == "NULL":
                            StrFieds = StrFieds + ", " + fields[i] + " = " + str(values[i])
                        else:
                            StrFieds = StrFieds + ", " + fields[i] + " = '" + str(values[i]) + "'"

                update_data_sql = "UPDATE %s SET %s WHERE %s = '%s' and %s = '%s'" \
                                  % (tablename, StrFieds, "date", datatime, "wellname", wellname)
                # print(update_data_sql)
                try:
                    executesql(SqlserverDataServre, update_data_sql)
                except Exception as e:
                    print(traceback.format_exc())

                # print("%s井%s日数据更新完毕" % (wellname, datatime))
            else:
                try:
                    insertdata(SqlserverDataServre, tablename, fields, values)
                except Exception as e:
                    print(traceback.format_exc())
                # print("%s井%s日数据插入完毕" % (wellname, datatime))
        print("%s日%s口井液面数据更新完毕！" % (begintime, datalen))


# 迁移功图数据
def generateindicatordate():
    for benum in range(0, 10):
        begintime = getstartdate(benum)
        endtime = getstartdate(benum - 1)

        # 手动功图数据
        StrFieds = ""
        tablename = "XBYY.DCA01"
        GetField = ["JH", "CSRQ", "DBMS_LOB.SUBSTR(SGT,2000,1)"]
        ShouceFieldName = ["wellname", "date", "gongtu"]
        for field in GetField:
            if StrFieds == "":
                StrFieds = field
            else:
                StrFieds = StrFieds + ", " + field
        get_data_sql = "SELECT %s FROM %s WHERE %s = TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" \
                       % (StrFieds, tablename, "CSRQ", begintime)
        shoucedata = getdatasql(daily_oracle_data_servre, get_data_sql)
        shoucedata = pd.DataFrame(shoucedata, columns=ShouceFieldName)
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
        shicedata = getdatasql(sensor_oracle_data_servre, get_data_sql)
        shicedata = pd.DataFrame(shicedata, columns=ShishiFieldName)
        datalen = len(shicedata)
        if datalen > 0:
            iswork = []
            for i in range(datalen):
                displacement = shicedata.iloc[i]["displacement"]
                displacement = displacement.split(",")
                displacement = list(map(float, displacement))
                load = shicedata.iloc[i]["load"]
                load = load.split(",")
                load = list(map(float, load))
                if sum(displacement) > 0 and sum(load) > 0:
                    iswork.append(True)
                else:
                    iswork.append(False)

            shicedata["iswork"] = iswork
            shicedata = shicedata[shicedata["iswork"]]
            shicedata = shicedata.drop(columns=['iswork'])
            shicedata = shicedata.set_index(["wellname"])

        if len(shoucedata) > 0:
            if len(shicedata) > 0:
                data = pd.concat([shoucedata, shicedata], sort=True)
            else:
                data = shoucedata
        else:
            if len(shicedata) > 0:
                data = shicedata
            else:
                continue

        data = data.reset_index(level=["wellname"])
        data = data.fillna("")

        # 上报数据
        tablename = "cyec.dbo.EcWellIndicator"
        fields = []
        for x in data:
            fields.append(x)
        datalen = len(data)
        for j in range(datalen):
            wellname = data.loc[j]["wellname"]
            datatime = data.loc[j]["date"]
            values = []
            for field in data:
                # print("------------------------------------------")
                # print(field)
                value = data.loc[j][field]
                if value == "":
                    value = "NULL"
                # print(value)
                values.append(value)

            get_data_sql = "SELECT * FROM %s WHERE %s = '%s' AND %s = '%s'" % (
                tablename, "date", datatime, "wellname", wellname)
            checkdata = getdatasql(SqlserverDataServre, get_data_sql)
            if len(checkdata) > 0:
                StrFieds = ""
                for i in range(len(fields)):
                    if StrFieds == "":
                        if str(values[i]) == "NULL":
                            StrFieds = fields[i] + " = " + str(values[i])
                        else:
                            StrFieds = fields[i] + " = '" + str(values[i]) + "'"
                    else:
                        if str(values[i]) == "NULL":
                            StrFieds = StrFieds + ", " + fields[i] + " = " + str(values[i])
                        else:
                            StrFieds = StrFieds + ", " + fields[i] + " = '" + str(values[i]) + "'"

                update_data_sql = "UPDATE %s SET %s WHERE %s = '%s' and %s = '%s'" \
                                  % (tablename, StrFieds, "date", datatime, "wellname", wellname)
                # print(update_data_sql)
                try:
                    executesql(SqlserverDataServre, update_data_sql)
                except Exception as e:
                    print(traceback.format_exc())

                # print("%s井%s日数据更新完毕" % (wellname, datatime))
            else:
                try:
                    insertdata(SqlserverDataServre, tablename, fields, values)
                except Exception as e:
                    print(traceback.format_exc())
                # print("%s井%s日数据插入完毕" % (wellname, datatime))
            # print("更新第%s个井%s......" % (j, wellname))
        print("%s日%s口井功图数据更新完毕！" % (begintime, datalen))


# 生产动态能力水平数据
def generatedynamicleveldata():
    for beday in range(0, 5):
        days = [10, 30]
        for benum in days:
            # print("....................................................")
            begintime = getstartdate(benum + beday)
            endtime = getstartdate(beday)
            # print(begintime)

            StrFieds = ""
            tablename = "cyec.dbo.ecsjb"
            GetField = ["JH", "RQ", "SCSJ", "RCYL1", "RCYL", "RCSL", "RCXL"]
            LevelFieldName = ["wellname", "date", "chanshi", "chanye", "chanyou", "chanshui", "chanxi"]
            for field in GetField:
                if StrFieds == "":
                    StrFieds = field
                else:
                    StrFieds = StrFieds + ", " + field
            get_data_sql = "SELECT %s FROM %s WHERE %s >= '%s' and %s < '%s' ORDER BY RQ DESC" \
                           % (StrFieds, tablename, "RQ", begintime, "RQ", endtime)
            dynamicdata = getdatasql(SqlserverDataServre, get_data_sql)
            dynamicdata = pd.DataFrame(dynamicdata, columns=LevelFieldName)
            dynamicdata = dynamicdata.set_index(["wellname"])

            if len(dynamicdata) == 0:
                break

            dynamicdata = dynamicdata.groupby(dynamicdata.index).sum()

            dynamicleveldata = pd.DataFrame([])
            newfieldname = "chanshi" + str(benum)
            dynamicleveldata[newfieldname] = dynamicdata["chanshi"] / benum

            FieldNames = ["chanye", "chanyou", "chanshui", "chanxi"]
            for fieldname in FieldNames:
                newfieldname = fieldname + "level" + str(benum)
                dynamicleveldata[newfieldname] = dynamicdata[fieldname] / benum
                newfieldname = fieldname + "ability" + str(benum)
                dynamicleveldata[newfieldname] = dynamicdata[fieldname] / dynamicdata["chanshi"] * 24
            dynamicleveldata.dropna(inplace=True)
            dynamicleveldata["date"] = getstartdate(beday + 1)
            dynamicleveldata = dynamicleveldata.reset_index(level=["wellname"])

            # 上报数据
            tablename = "cyec.dbo.EcDynamiclevel"
            fields = []
            for x in dynamicleveldata:
                fields.append(x)
            datalen = len(dynamicleveldata)
            for j in range(datalen):
                wellname = dynamicleveldata.loc[j]["wellname"]
                datatime = dynamicleveldata.loc[j]["date"]
                values = []
                for field in fields:
                    # print("------------------------------------------")
                    # print(field)
                    value = dynamicleveldata.loc[j][field]
                    if value == "":
                        value = "NULL"
                    # print(value)
                    values.append(value)

                get_data_sql = "SELECT * FROM %s WHERE %s = '%s' AND %s = '%s'" % (
                    tablename, "date", datatime, "wellname", wellname)
                checkdata = getdatasql(SqlserverDataServre, get_data_sql)
                if len(checkdata) > 0:
                    StrFieds = ""
                    for i in range(len(fields)):
                        if StrFieds == "":
                            if str(values[i]) == "NULL":
                                StrFieds = fields[i] + " = " + str(values[i])
                            else:
                                StrFieds = fields[i] + " = '" + str(values[i]) + "'"
                        else:
                            if str(values[i]) == "NULL":
                                StrFieds = StrFieds + ", " + fields[i] + " = " + str(values[i])
                            else:
                                StrFieds = StrFieds + ", " + fields[i] + " = '" + str(values[i]) + "'"

                    update_data_sql = "UPDATE %s SET %s WHERE %s = '%s' and %s = '%s'" \
                                      % (tablename, StrFieds, "date", datatime, "wellname", wellname)
                    # print(update_data_sql)
                    try:
                        executesql(SqlserverDataServre, update_data_sql)
                    except Exception as e:
                        print(traceback.format_exc())

                    # print("%s井%s日数据更新完毕" % (wellname, datatime))
                else:
                    try:
                        insertdata(SqlserverDataServre, tablename, fields, values)
                    except Exception as e:
                        print(traceback.format_exc())
                    # print("%s井%s日数据插入完毕" % (wellname, datatime))
                # print("更新第%s个井%s......" % (j, wellname))
            print("%s日%s口井%s天水平能力数据更新完毕！" % (getstartdate(beday + 1), datalen, str(benum)))


# 提取单井掺稀数据
def extract_well_chanxi_data(wellname):
    global GLOBAL_WellNameReference
    global GLOBAL_Logger
    # global GlOBAL_oracle_engine

    now_time = time.strftime("%Y-%m-%d %H:00:00", datetime.fromtimestamp(time.time()).timetuple())

    wellalias = GLOBAL_WellNameReference[wellname]
    if len(wellalias) > 0:
        # print(wellname + "-" + wellalias)
        tablename = "ssc_yj_ss"

        fields = ["CJSJ", "JH", "CX_SDLL", "CX_SSLL", "CX_LJLL"]
        str_fieds = ""
        for field in fields:
            if str_fieds == "":
                str_fieds = field
            else:
                str_fieds = str_fieds + ", " + field
        starttimedaily = getbegintime(60 * 24, "minutes")
        starttimehour = getbegintime(60, "minutes")

        get_data_sql = "SELECT %s FROM %s WHERE cjsj>TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') and JH ='%s' " % \
                       (str_fieds, tablename, starttimedaily, wellalias)
        realtimedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine)
        # realtimedata.set_index(['cjsj'], inplace=True)
        realtimedata = realtimedata.dropna()
        realtimedata.sort_values("cjsj")
        # 有数据
        if len(realtimedata) > 0:
            checkflag = realtimedata["cx_sdll"].sum() / 60
            # 设定量大于零
            if checkflag > 0:
                upname = wellname
                uptime = now_time
                upbasedata = realtimedata.iloc[-1, 2]
                updailychanxi1 = realtimedata["cx_ssll"].sum() / 60
                updailychanxi2 = realtimedata.iloc[-1, 4] - realtimedata.iloc[0, 4]

                checkflag2 = abs(updailychanxi1 - updailychanxi2)
                checkflag2 = checkflag2 / checkflag

                # 累计数据准确
                if checkflag2 < 0.5:
                    updailychanxi = (updailychanxi1 + updailychanxi2) / 2
                    updailychanxi = round(updailychanxi, 2)

                    realtimedata = realtimedata[realtimedata["cjsj"] >= starttimehour]
                    # 最近一小时有数据
                    if len(realtimedata) > 0:
                        uphourchanxi1 = realtimedata["cx_ssll"].sum() / 60
                        uphourchanxi2 = realtimedata.iloc[-1, 4] - realtimedata.iloc[0, 4]
                        uphourchanxi = (uphourchanxi1 + uphourchanxi2) / 2
                        uphourchanxi = round(uphourchanxi, 2)

                        # print(realtimedata)
                        # 上报数据
                        tablename = "cyec.dbo.EcWellChanxiRealtime"
                        fields = ["wellname", "time", "basedata", "hourchanxi", "dailychanxi"]
                        values = [upname, uptime, upbasedata, uphourchanxi, updailychanxi]

                        get_data_sql = "SELECT * FROM %s WHERE %s = '%s' AND %s = '%s'" % (
                            tablename, "time", uptime, "wellname", upname)
                        checkdata = getdatasql(SqlserverDataServre, get_data_sql)
                        if len(checkdata) > 0:
                            str_fieds = ""
                            for i in range(len(fields)):
                                if str_fieds == "":
                                    if str(values[i]) == "NULL":
                                        str_fieds = fields[i] + " = " + str(values[i])
                                    else:
                                        str_fieds = fields[i] + " = '" + str(values[i]) + "'"
                                else:
                                    if str(values[i]) == "NULL":
                                        str_fieds = str_fieds + ", " + fields[i] + " = " + str(values[i])
                                    else:
                                        str_fieds = str_fieds + ", " + fields[i] + " = '" + str(values[i]) + "'"

                            update_data_sql = "UPDATE %s SET %s WHERE %s = '%s' and %s = '%s'" \
                                              % (tablename, str_fieds, "time", uptime, "wellname", upname)
                            try:
                                executesql(SqlserverDataServre, update_data_sql)
                            except Exception as e:
                                print(traceback.format_exc())
                        else:
                            try:
                                insertdata(SqlserverDataServre, tablename, fields, values)
                            except Exception as e:
                                print(traceback.format_exc())


# 提取单井实时掺稀数据
def extract_well_realtime_data():
    global GLOBAL_OfficiaWellNames
    global GLOBAL_Logger

    GLOBAL_Logger.info("开始更新单井掺稀数据！")
    print("......................................................")
    print(getnowtime())
    print("开始更新单井掺稀数据！")

    try:
        num = 1
        for wellname in GLOBAL_OfficiaWellNames:
            # print(".......................................................")
            # print("初始化第%s口井：%s" % (str(num), wellname))
            extract_well_chanxi_data(wellname)
            num = num + 1

        GLOBAL_Logger.info("%s口井单井掺稀数据提取完毕" % str(num))
        print("%s口井单井掺稀数据提取完毕" % str(num))
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 提取计转站实时流量数据
def extract_station_turnover_data():
    global GLOBAL_WellNameReference
    global GLOBAL_Logger
    global GlOBAL_oracle_engine

    print("开始更新计转站来油外输数据")

    now_time = time.strftime("%Y-%m-%d %H:00:00", datetime.fromtimestamp(time.time()).timetuple())
    tablename = "SSC_LLJ_SS"

    fields = ["CJSJ", "SBBM", "SSLL", "LJLL"]
    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field
    starttimedaily = getbegintime(60 * 24, "minutes")
    starttimehour = getbegintime(60, "minutes")

    get_data_sql = "SELECT %s FROM %s WHERE cjsj>TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') and " \
                   "(instr(sbbm, '%s') + instr(sbbm, '%s') + instr(sbbm, '%s'))>0" % \
                   (str_fieds, tablename, starttimedaily, "_CXLY", "_CXFZ", "_YYWS")
    realtimedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine)
    # realtimedata.set_index(['sbbm'], inplace=True)
    realtimedata = realtimedata.dropna()

    stationnames = set(realtimedata["sbbm"])
    # print(len(stationnames))
    # print(realtimedata)

    stationcount = 0
    for name in stationnames:
        # print("...................")
        stationname = name[5:name.find("_")] + "计"
        # print(stationname)

        if "_CXLY" in name:
            devicename = "掺稀来油"
            devicetype = "掺稀来油"
        elif "_YYWS" in name:
            devicename = "原油外输"
            devicetype = "原油外输"
        else:
            namestr = name[name.find("_") + 1:]
            namestr = namestr[namestr.find("_") + 1:]
            if namestr == "BZ":
                namestr = "本站"

            devicename = namestr + "阀组"
            devicetype = "掺稀阀组"

        # print(devicename)
        # print(devicetype)

        stationrealtimedata = realtimedata[realtimedata["sbbm"] == name]
        stationrealtimedata = stationrealtimedata.set_index("cjsj")
        stationrealtimedata.sort_index(inplace=True)
        # print(stationrealtimedata)

        # 有数据
        if len(stationrealtimedata) > 0:
            updailyflowmean = stationrealtimedata["ssll"].mean() * 24
            updailyflowdiff = stationrealtimedata.iloc[-1, 2] - stationrealtimedata.iloc[0, 2]

            checkflag = abs(updailyflowmean - updailyflowdiff)
            if updailyflowmean == 0:
                devicestatus = "无流量"
                updailyflow = 0
            else:
                checkflag = checkflag / updailyflowmean

                if checkflag < 0.2:
                    devicestatus = "正常"
                    updailyflow = (updailyflowmean + updailyflowdiff) / 2
                else:
                    devicestatus = "底数异常"
                    updailyflow = updailyflowmean

            updailyflow = round(updailyflow, 2)
            basedata = stationrealtimedata.iloc[-1, 2]
            # print(updailyflowmean)
            # print(updailyflowdiff)
            # print(updailyflow)
            # print(devicestatus)

            stationrealtimedata = stationrealtimedata[stationrealtimedata.index >= starttimehour]

            if len(stationrealtimedata) > 0:
                uphourflowmean = stationrealtimedata["ssll"].mean()
                uphourflowdiff = stationrealtimedata.iloc[-1, 2] - stationrealtimedata.iloc[0, 2]

                checkflag = abs(uphourflowmean - uphourflowdiff)
                if uphourflowmean == 0:
                    devicestatus = "无流量"
                    uphourflow = 0
                else:
                    checkflag = checkflag / uphourflowmean

                    if checkflag < 0.2:
                        devicestatus = "正常"
                        uphourflow = (uphourflowmean + uphourflowdiff) / 2
                    else:
                        devicestatus = "底数异常"
                        uphourflow = uphourflowmean

                uphourflow = round(uphourflow, 2)

                # print(uphourflowmean)
                # print(uphourflowdiff)
                # print(uphourflow)
                # print(devicestatus)
            else:
                uphourflow = 0
                devicestatus = "无数据"
        else:
            devicestatus = "无数据"
            basedata = 0
            uphourflow = 0
            updailyflow = 0

        # 上报数据
        tablename = "cyec.dbo.EcStationTurnoverRealtime"
        fields = ["station", "time", "devicename", "devicetype", "devicestatus", "basedata", "updailyflow",
                  "uphourflow", "devicecode"]
        values = [stationname, now_time, devicename, devicetype, devicestatus, basedata, updailyflow, uphourflow, name]

        get_data_sql = "SELECT * FROM %s WHERE %s = '%s' AND %s = '%s'" % \
                       (tablename, "time", now_time, "devicecode", name)
        checkdata = getdatasql(SqlserverDataServre, get_data_sql)
        if len(checkdata) > 0:
            str_fieds = ""
            for i in range(len(fields)):
                if str_fieds == "":
                    if str(values[i]) == "NULL":
                        str_fieds = fields[i] + " = " + str(values[i])
                    else:
                        str_fieds = fields[i] + " = '" + str(values[i]) + "'"
                else:
                    if str(values[i]) == "NULL":
                        str_fieds = str_fieds + ", " + fields[i] + " = " + str(values[i])
                    else:
                        str_fieds = str_fieds + ", " + fields[i] + " = '" + str(values[i]) + "'"

            update_data_sql = "UPDATE %s SET %s WHERE %s = '%s' and %s = '%s'" \
                              % (tablename, str_fieds, "time", now_time, "devicecode", name)
            try:
                executesql(SqlserverDataServre, update_data_sql)
            except Exception as e:
                print(traceback.format_exc())
        else:
            try:
                insertdata(SqlserverDataServre, tablename, fields, values)
            except Exception as e:
                print(traceback.format_exc())

        stationcount = stationcount + 1

    print("%s个计转站实时数据更新完毕！" % stationcount)


# 提取计转站实时流量数据
def extract_station_realtime_data():
    global GLOBAL_Logger

    GLOBAL_Logger.info("开始更新计转站来油外输数据！")
    print("......................................................")
    print(getnowtime())
    print("开始更新计转站来油外输数据!")

    try:
        extract_station_turnover_data()
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 提取生产动态能力水平数据
def extract_dynamic_level_data():
    global GLOBAL_Logger

    GLOBAL_Logger.info("开始更新生产动态能力水平数据！")
    print("......................................................")
    print(getnowtime())
    print("开始更新生产动态能力水平数据!")

    try:
        generatedynamicleveldata()
        print("动态能力水平数据跟新完毕！")
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 提取在线功图数据
def extract_indicator_date():
    global GLOBAL_Logger

    GLOBAL_Logger.info("开始提取在线功图数据！")
    print("......................................................")
    print(getnowtime())
    print("开始提取在线功图数据!")

    try:
        generateindicatordate()
        print("提取在线功图数据跟新完毕！")
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 提取液面流静压数据
def extract_level_date():
    global GLOBAL_Logger

    GLOBAL_Logger.info("开始提取液面流静压数据！")
    print("......................................................")
    print(getnowtime())
    print("开始提取液面流静压数据!")

    try:
        generateleveldate()
        print("提取液面流静压数据跟新完毕！")
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 提取动静态数据
def extract_daily_date():
    global GLOBAL_Logger

    GLOBAL_Logger.info("开始提取动静态数据！")
    print("......................................................")
    print(getnowtime())
    print("开始提取动静态数据!")

    try:
        generatedailydata()
        print("提取动静态数据完毕！")
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 整理计转站来油对比数据
def sortstationinputcomparereportdata():
    for durtime in range(12):
        sortdate = datetime.fromtimestamp(time.time()) - relativedelta(hours=durtime)
        sortdate = time.strftime("%Y-%m-%d %H:00:00", sortdate.timetuple())
        # print(sortdate)

        # 单井掺稀数据
        StrFieds = ""
        tablename = "EcWellChanxiReport"
        # GetField = ["wellname", "time", "area", "station", "hourchanxi", "devicetype", "devicecode"]
        GetField = ["wellname", "time", "area", "station", "hourchanxi"]
        ShouceFieldName = ["wellname", "time", "area", "station", "hourchanxi"]
        for field in GetField:
            if StrFieds == "":
                StrFieds = field
            else:
                StrFieds = StrFieds + ", " + field
        get_data_sql = "SELECT %s FROM %s WHERE %s = '%s'" \
                       % (StrFieds, tablename, "time", sortdate)
        wellchanxidata = getdatasql(SqlserverDataServre, get_data_sql)
        wellchanxidata = pd.DataFrame(wellchanxidata, columns=ShouceFieldName)
        # print(wellchanxidata)
        welloilsum = wellchanxidata["hourchanxi"].sum()

        # 稀油储罐数据
        StrFieds = ""
        tablename = "EcStationStorageReport"
        GetField = ["wellname", "time", "area", "station", "inventory", "inventorydiff"]
        ShouceFieldName = ["wellname", "time", "area", "station", "inventory", "inventorydiff"]
        for field in GetField:
            if StrFieds == "":
                StrFieds = field
            else:
                StrFieds = StrFieds + ", " + field
        get_data_sql = "SELECT %s FROM %s WHERE %s = '%s'" % (StrFieds, tablename, "time", sortdate)
        storagedata = getdatasql(SqlserverDataServre, get_data_sql)
        storagedata = pd.DataFrame(storagedata, columns=ShouceFieldName)
        # print(storagedata)
        xiyouguandiffsum = storagedata[storagedata["wellname"] == "稀油罐"]
        xiyouguandiffsum = xiyouguandiffsum["inventorydiff"].sum()
        huanchongguandiffsum = storagedata[storagedata["wellname"] == "稀油缓冲罐"]
        huanchongguandiffsum = huanchongguandiffsum["inventorydiff"].sum()
        inventorydiffsum = round(xiyouguandiffsum + huanchongguandiffsum, 2)

        # 计转站井站关系数据
        StrFieds = ""
        tablename = "EcStationWellRelationship"
        GetField = ["station", "oilsource"]
        ShouceFieldName = ["station", "oilsource"]
        for field in GetField:
            if StrFieds == "":
                StrFieds = field
            else:
                StrFieds = StrFieds + ", " + field
        get_data_sql = "SELECT %s FROM %s " % (StrFieds, tablename)
        relationshipdata = getdatasql(SqlserverDataServre, get_data_sql)
        relationshipdata = pd.DataFrame(relationshipdata, columns=ShouceFieldName)
        relationshipdata = relationshipdata.drop_duplicates()

        # 计转站来油外输数据
        StrFieds = ""
        tablename = "EcStationTurnoverReport"
        # GetField = ["wellname", "time", "area", "station", "hourchanxi", "devicetype", "devicecode"]
        GetField = ["wellname", "area", "station", "hourchanxi", "devicetype"]
        ShouceFieldName = ["wellname", "area", "station", "hourchanxi", "devicetype"]
        for field in GetField:
            if StrFieds == "":
                StrFieds = field
            else:
                StrFieds = StrFieds + ", " + field
        get_data_sql = "SELECT %s FROM %s WHERE %s = '%s'" \
                       % (StrFieds, tablename, "time", sortdate)
        turnoverdata = getdatasql(SqlserverDataServre, get_data_sql)
        turnoverdata = pd.DataFrame(turnoverdata, columns=ShouceFieldName)

        # print(turnoverdata)
        if len(turnoverdata) > 0:
            stations = set(relationshipdata["station"])
            for station in stations:
                checkdata = turnoverdata[turnoverdata["station"] == station]

                area = checkdata["area"][0]

                chanxilaiyou = checkdata[checkdata["devicetype"] == "掺稀来油"]
                if len(chanxilaiyou) > 0:
                    chanxilaiyou = chanxilaiyou["hourchanxi"].values[0]
                else:
                    chanxilaiyou = 0

                lianluoxianlaiyou = checkdata[checkdata["devicetype"] == "联络线来油"]
                if len(lianluoxianlaiyou) > 0:
                    lianluoxianlaiyou = lianluoxianlaiyou["hourchanxi"].values[0]
                else:
                    lianluoxianlaiyou = 0

                inputvalveoilsum = checkdata[checkdata["devicetype"] == "本站阀组"]
                inputvalveoilsum = inputvalveoilsum["hourchanxi"].sum()
                if inputvalveoilsum > 0:
                    benzhanyongyou = round(inputvalveoilsum, 2)
                else:
                    benzhanyongyou = round(welloilsum, 2)

                outputvalveoilsum = checkdata[checkdata["devicetype"] == "外输阀组"]
                outputvalveoilsum = round(outputvalveoilsum["hourchanxi"].sum(), 2)

                relationshipdata = relationshipdata[relationshipdata["oilsource"] == station]

                outputstations = set(relationshipdata["station"])
                if len(outputstations) > 0:
                    outputlaiyousum = 0
                    for outputstation in outputstations:
                        outputlaiyoudata = turnoverdata[turnoverdata["station"] == outputstation]
                        outputlaiyoudata = outputlaiyoudata[outputlaiyoudata["devicetype"] == "掺稀来油"]
                        if len(outputlaiyoudata) > 0:
                            outputlaiyousum = outputlaiyousum + outputlaiyoudata["hourchanxi"].values[0]
                else:
                    outputlaiyousum = 0
                outputlaiyousum = round(outputlaiyousum, 2)

                laiyoudiff = chanxilaiyou + lianluoxianlaiyou - benzhanyongyou - outputvalveoilsum - outputlaiyousum - inventorydiffsum

                # 上报数据
                tablename = "cyec.dbo.EcStationIncomeCompare"
                fields = ["time", "station", "area", "chanxilaiyou", "lianluoxianlaiyou", "inventorydiffsum",
                          "benzhanyongyou", "outputvalveoilsum", "outputlaiyousum", "laiyoudiff"]
                values = [sortdate, station, area, chanxilaiyou, lianluoxianlaiyou, inventorydiffsum, benzhanyongyou,
                          outputvalveoilsum, outputlaiyousum, laiyoudiff]

                get_data_sql = "SELECT * FROM %s WHERE %s = '%s' AND %s = '%s'" % \
                               (tablename, "time", sortdate, "station", station)
                checkdata = getdatasql(SqlserverDataServre, get_data_sql)
                if len(checkdata) > 0:
                    str_fieds = ""
                    for i in range(len(fields)):
                        if str_fieds == "":
                            if str(values[i]) == "NULL":
                                str_fieds = fields[i] + " = " + str(values[i])
                            else:
                                str_fieds = fields[i] + " = '" + str(values[i]) + "'"
                        else:
                            if str(values[i]) == "NULL":
                                str_fieds = str_fieds + ", " + fields[i] + " = " + str(values[i])
                            else:
                                str_fieds = str_fieds + ", " + fields[i] + " = '" + str(values[i]) + "'"

                    update_data_sql = "UPDATE %s SET %s WHERE %s = '%s' and %s = '%s'" \
                                      % (tablename, str_fieds, "time", sortdate, "station", station)
                    try:
                        executesql(SqlserverDataServre, update_data_sql)
                    except Exception as e:
                        print(traceback.format_exc())
                else:
                    try:
                        insertdata(SqlserverDataServre, tablename, fields, values)
                    except Exception as e:
                        print(traceback.format_exc())


# 整理计转站来油对比数据
def sort_station_input_compare_report_data():
    global GLOBAL_Logger

    GLOBAL_Logger.info("开始整理计转站来油对比数据！")
    print("......................................................")
    print(getnowtime())
    print("开始整理计转站来油对比数据!")

    try:
        sortstationinputcomparereportdata()
        print("整理计转站来油对比数据完毕！")
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 提取单井参数数据
def extractwellparameterdata(extractday=0):
    global GLOBAL_WellNameReference
    global GlOBAL_mssql_engine

    # 油井计转站对照表
    tablename = "cyec.dbo.ecsjb"
    fields = ["JH", "RQ", "DWMC"]
    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE RQ>'%s'" % (str_fieds, tablename, getbegintime(5))
    # print(get_data_sql)
    realtimedata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine)
    realtimedata.sort_values("RQ", ascending=False, inplace=True)
    realtimedata.drop(['RQ'], axis=1, inplace=True)
    realtimedata.drop_duplicates(inplace=True)
    areadict = {}
    for i in range(len(realtimedata)):
        wellname = realtimedata.iloc[i, 0]
        area = realtimedata.iloc[i, 1]
        areadict[wellname] = area

    # # 取数据，整理数据，上报
    starttimedaily = getbegintime(60 * 24 * (extractday + 1), "minutes")
    endtimedaily = getbegintime(60 * 24 * extractday, "minutes")
    uptime = time.strftime("%Y-%m-%d 00:00:00",
                           (datetime.fromtimestamp(time.time()) - relativedelta(days=extractday)).timetuple())
    for wellname in GLOBAL_WellNameReference:
        alias = GLOBAL_WellNameReference[wellname]
        area = areadict.get(wellname, "")
        if len(alias) > 0:
            # 取数据
            tablename = "ssc_yj_ss"
            fields = ["CJSJ", "JK_YY", "JK_TY", "JK_HY", "JK_WD", "BXSXDLFZ", "BXXXDLFZ", "DL_B", "DY_B", "JRL_SYWD",
                      "JRL_YW"]
            str_fieds = ""
            for field in fields:
                if str_fieds == "":
                    str_fieds = field
                else:
                    str_fieds = str_fieds + ", " + field

            get_data_sql = "SELECT %s FROM %s WHERE cjsj>TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') and " \
                           "cjsj<TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') and JH ='%s' " % \
                           (str_fieds, tablename, starttimedaily, endtimedaily, alias)
            realtimedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine)
            realtimedata.set_index(['cjsj'], inplace=True)
            realtimedata.columns = ["yy", "ty", "hy", "jw", "sxdl", "xxdl", "dl", "dy", "lw", "cw"]
            # realtimedata = realtimedata.dropna()
            realtimedata.sort_values("cjsj")

            fields = ["date", "wellname", "area"]
            values = [uptime, wellname, area]
            isnormal = 3
            if len(realtimedata) > 0:
                for field in realtimedata.columns:
                    fields.append(field + "top")
                    fields.append(field + "bot")
                    fields.append(field)

                    maxvalue = realtimedata[field].quantile(0.85)
                    minvalue = realtimedata[field].quantile(0.15)
                    meanvalue = realtimedata[field].mean()

                    if not (field == "jw" or field == "lw" or field == "cw"):
                        if maxvalue <= 0:
                            maxvalue = 0.001
                        if minvalue <= 0:
                            minvalue = 0.001
                        if meanvalue <= 0:
                            meanvalue = 0.001

                    maxvalue = round(maxvalue, 1)
                    minvalue = round(minvalue, 1)
                    meanvalue = round(meanvalue, 1)

                    if field == "jw":
                        isnormal = 1
                    else:
                        if meanvalue == 0:
                            if isnormal:
                                isnormal = 1
                        else:
                            abnormalrate = abs(maxvalue - meanvalue) / meanvalue * 100
                            if abnormalrate > 100:
                                if maxvalue - meanvalue > 0.5:
                                    isnormal = 0
                                else:
                                    isnormal = 2
                            else:
                                if isnormal:
                                    isnormal = 1
                            abnormalrate = abs(minvalue - meanvalue) / meanvalue * 100
                            if abnormalrate > 100:
                                if maxvalue - meanvalue > 0.5:
                                    isnormal = 0
                                else:
                                    isnormal = 2
                            else:
                                if isnormal:
                                    isnormal = 1

                    if np.isnan(maxvalue):
                        maxvalue = "NULL"
                    values.append(maxvalue)
                    if np.isnan(minvalue):
                        minvalue = "NULL"
                    values.append(minvalue)
                    if np.isnan(meanvalue):
                        meanvalue = "NULL"
                    values.append(meanvalue)

                fields.append("isnormal")
                values.append(isnormal)

                # 上报数据
                tablename = "cyec.dbo.EcWellParameter"

                deldate = getbegintime(10)
                del_data_sql = "DELETE FROM %s WHERE %s < '%s'" % (tablename, "date", deldate)
                executesql(SqlserverDataServre, del_data_sql)

                get_data_sql = "SELECT * FROM %s WHERE %s = '%s' AND %s = '%s'" % (
                    tablename, "date", uptime, "wellname", wellname)
                checkdata = getdatasql(SqlserverDataServre, get_data_sql)
                if len(checkdata) > 0:
                    str_fieds = ""
                    for i in range(len(fields)):
                        if str_fieds == "":
                            if str(values[i]) == "NULL":
                                str_fieds = fields[i] + " = " + str(values[i])
                            else:
                                str_fieds = fields[i] + " = '" + str(values[i]) + "'"
                        else:
                            if str(values[i]) == "NULL":
                                str_fieds = str_fieds + ", " + fields[i] + " = " + str(values[i])
                            else:
                                str_fieds = str_fieds + ", " + fields[i] + " = '" + str(values[i]) + "'"

                    update_data_sql = "UPDATE %s SET %s WHERE %s = '%s' and %s = '%s'" \
                                      % (tablename, str_fieds, "date", uptime, "wellname", wellname)
                    try:
                        executesql(SqlserverDataServre, update_data_sql)
                    except Exception as e:
                        print(traceback.format_exc())
                else:
                    try:
                        insertdata(SqlserverDataServre, tablename, fields, values)
                    except Exception as e:
                        print(traceback.format_exc())


# 提取单井参数数据
def extract_well_parameter_data():
    global GLOBAL_Logger

    GLOBAL_Logger.info("开始提取单井参数数据！")
    print("......................................................")
    print(getnowtime())
    print("开始提取单井参数数据!")

    extractdays = 2
    try:
        for exday in range(extractdays):
            extractwellparameterdata(exday)
            uptime = time.strftime("%m/%d",
                                   (datetime.fromtimestamp(time.time()) - relativedelta(days=exday)).timetuple())
            print("%s日单井参数数据提取完毕！" % uptime)
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 计算单井掺稀基值数据
def sortwellbasechanxi():
    global GLOBAL_Logger
    global GlOBAL_mssql_engine
    global GLOBAL_WellNameReference

    # print("开始取日报数据")
    # 取30天日报动态数据
    startdate = getstartdate(30)
    fields = ["RQ", "JH", "DWMC", "CYQ", "SCSJ", "RCXL", "PL", "YZ"]
    fieldstr = fields[0]
    for field in fields[1:]:
        fieldstr = fieldstr + ", " + field
    tablename = "ecsjb"
    getstr = "SELECT %s  FROM %s WHERE %s >= '%s'" % (fieldstr, tablename, "RQ", startdate)
    daily_data = pd.read_sql(getstr, GlOBAL_mssql_engine, index_col=["JH"])
    daily_data = daily_data.fillna(0)
    daily_data = daily_data[daily_data["SCSJ"] == 24.0]
    daily_data = daily_data[daily_data["RCXL"] > 0.0]
    daily_data["JH"] = daily_data.index
    daily_data.sort_values("RQ", inplace=True)

    # 对单井循环计算掺稀基值
    wellnamedata = daily_data[["JH"]]
    wellnamedata = wellnamedata.drop_duplicates()

    for rownum in range(len(wellnamedata)):
        # for rownum in range(1):
        # for rownum in range(5):
        # print("........................")
        wellinfo = wellnamedata.iloc[rownum]
        wellname = wellinfo["JH"]
        # wellname = "TH12558H"
        welldata = daily_data.loc[wellname]

        # print(welldata)

        # 对开井时间间隔4天以上油井，认为为间开井，放弃间开前数据
        dateshape = welldata.shape
        if len(dateshape) == 2:
            datanum = dateshape[0]

            begindate = time.mktime(time.strptime(welldata.iloc[0]["RQ"], "%Y-%m-%d"))
            enddate = time.mktime(time.strptime(welldata.iloc[-1]["RQ"], "%Y-%m-%d"))
            checkdatenum = (enddate - begindate) / (60 * 60 * 24) + 1
            datadiff = checkdatenum - datanum

            if datadiff > 4:
                checkrows = datanum - 1
                checkwelldata = welldata
                for i in range(checkrows):
                    checkwelldata = welldata.iloc[i + 1:, ]
                    datanum = checkwelldata.shape[0]
                    begindate = time.mktime(time.strptime(checkwelldata.iloc[0]["RQ"], "%Y-%m-%d"))
                    enddate = time.mktime(time.strptime(checkwelldata.iloc[-1]["RQ"], "%Y-%m-%d"))
                    checkdatenum = (enddate - begindate) / (60 * 60 * 24) + 1
                    datadiff = checkdatenum - datanum
                    if datadiff == 0:
                        break
                welldata = checkwelldata

        # print(welldata)

        # 对于转生产方式油井，放弃目前生产方式前数据
        dateshape = welldata.shape
        if len(dateshape) == 2:
            begindata = welldata.iloc[0]
            enddata = welldata.iloc[-1]
            plb = begindata["PL"]
            ple = enddata["PL"]
            if ple == 0:
                if plb > 0:
                    welldata = welldata[welldata["PL"] == 0]
            else:
                if plb == 0:
                    welldata = welldata[welldata["PL"] > 0]

        # 对于工作制度调整油井，进行排量、油嘴调整
        dateshape = welldata.shape
        if len(dateshape) == 2:
            plb = welldata.iloc[0]["PL"]
            ple = welldata.iloc[-1]["PL"]
            if ple > 0:
                if abs(ple - plb) > 0.1:
                    welldata.loc[:, ["RCXL"]] = welldata["RCXL"] / welldata["PL"] * ple

            yzb = welldata.iloc[0]["YZ"]
            yze = welldata.iloc[-1]["YZ"]
            if abs(yze - yzb) > 0.1:
                if yze == 0:
                    yze = welldata["YZ"].max() + 0.5
                welldata.loc[welldata["YZ"] == 0, ["YZ"]] = welldata["YZ"].max() + 0.5
                welldata.loc[:, ["RCXL"]] = welldata["RCXL"] / pow(welldata["YZ"], 0.5) * pow(yze, 0.5)

        # print(welldata)

        # 取掺稀基值
        dateshape = welldata.shape

        if len(dateshape) == 1:
            area = welldata["DWMC"]
            block = welldata["CYQ"]
            chanxi = welldata["RCXL"]
            cxp50 = chanxi
            cxp25 = chanxi
            cxp75 = chanxi

        elif dateshape[0] == 1:
            area = welldata.iloc[0, 1]
            block = welldata.iloc[0, 2]
            chanxi = welldata.iloc[0, 4]
            cxp50 = chanxi
            cxp25 = chanxi
            cxp75 = chanxi
        else:
            welldata = welldata.loc[:, ["RQ", "DWMC", "CYQ", "RCXL"]]
            area = welldata.iloc[-1]["DWMC"]
            block = welldata.iloc[-1]["CYQ"]

            cxmean = welldata["RCXL"].mean()
            cxstd = welldata["RCXL"].std()
            welldata['STD'] = (welldata['RCXL'] - cxmean) / cxstd

            cxlstd = welldata.iloc[-1]["STD"]
            welldata = welldata[(welldata["STD"] <= 1.5) & (welldata["STD"] >= -1.5)]
            cxp25 = welldata["RCXL"].quantile(0.2)
            cxp50 = welldata["RCXL"].quantile(0.5)
            cxp75 = welldata["RCXL"].quantile(0.8)
            cxmean = welldata["RCXL"].mean()

            if cxlstd > 1:
                chanxi = (cxp75 * abs(cxlstd) + cxmean) / (abs(cxlstd) + 1)
            elif cxlstd < -1:
                chanxi = (cxp25 * abs(cxlstd) + cxmean) / (abs(cxlstd) + 1)
            else:
                chanxi = (cxmean + cxp50) / 2

        if np.isnan(chanxi):
            chanxi = "NULL"
        if np.isnan(cxp25):
            cxp25 = "NULL"
        if np.isnan(cxp50):
            cxp50 = "NULL"
        if np.isnan(cxp75):
            cxp75 = "NULL"

        # 上报数据
        tablename = "cyec.dbo.EcWellChanxiBasedata"
        fields = ["JH", "DWMC", "CYQ", "chanxi", "date", "p25", "p50", "p75"]
        values = [wellname, area, block, chanxi, getnowtime(), cxp25, cxp50, cxp75]

        get_data_sql = "SELECT * FROM %s WHERE %s = '%s'" % (tablename, "JH", wellname)
        checkdata = getdatasql(SqlserverDataServre, get_data_sql)
        if len(checkdata) > 0:
            str_fieds = ""
            for i in range(len(fields)):
                if str_fieds == "":
                    if str(values[i]) == "NULL":
                        str_fieds = fields[i] + " = " + str(values[i])
                    else:
                        str_fieds = fields[i] + " = '" + str(values[i]) + "'"
                else:
                    if str(values[i]) == "NULL":
                        str_fieds = str_fieds + ", " + fields[i] + " = " + str(values[i])
                    else:
                        str_fieds = str_fieds + ", " + fields[i] + " = '" + str(values[i]) + "'"

            update_data_sql = "UPDATE %s SET %s WHERE %s = '%s'" % (tablename, str_fieds, "JH", wellname)
            try:
                executesql(SqlserverDataServre, update_data_sql)
            except Exception as e:
                print(traceback.format_exc())
        else:
            try:
                insertdata(SqlserverDataServre, tablename, fields, values)
            except Exception as e:
                print(traceback.format_exc())


# 计算单井掺稀基值数据
def sort_well_base_chanxi():
    global GLOBAL_Logger

    # GLOBAL_Logger.info("开始计算单井掺稀基值数据！")
    print("......................................................")
    print(getnowtime())
    print("开始计算单井掺稀基值数据!")

    try:
        sortwellbasechanxi()
        print("计算单井掺稀基值数据完毕！")
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 检查数据是否上报
def checkdataupdate():
    global GlOBAL_mssql_engine

    tablename = "ecsjb"
    startdate = time.strftime("%Y-%m-%d", (datetime.fromtimestamp(time.time()) - relativedelta(days=1)).timetuple())
    fields = ["JH", "DWMC", "CYQ", "SCSJ", "RCXL"]
    fieldstr = fields[0]
    for field in fields[1:]:
        fieldstr = fieldstr + ", " + field
    getstr = "SELECT %s  FROM %s WHERE %s = '%s'" % (fieldstr, tablename, "RQ", startdate)
    daily_data = pd.read_sql(getstr, GlOBAL_mssql_engine)
    checkdata = daily_data.groupby("DWMC").count()

    if len(checkdata) < 3:
        return False
    else:
        # 取管理区井数
        startdate = getstartdate(10)
        fields = ["DWMC", "CYQ", "JH"]
        fieldstr = fields[0]
        for field in fields[1:]:
            fieldstr = fieldstr + ", " + field
        getstr = "SELECT %s  FROM %s WHERE %s >= '%s'" % (fieldstr, tablename, "RQ", startdate)
        welldata = pd.read_sql(getstr, GlOBAL_mssql_engine)
        welldata = welldata.drop_duplicates()
        welldata = welldata.groupby("DWMC").count()

        yiquwellnum = welldata.loc["管理一区", "JH"]
        erquwellnum = welldata.loc["管理二区", "JH"]
        sanquwellnum = welldata.loc["管理三区", "JH"]

        if checkdata.loc["管理一区", "JH"] > yiquwellnum * 0.95:
            yiqucheck = True
        else:
            yiqucheck = False

        if checkdata.loc["管理二区", "JH"] > erquwellnum * 0.95:
            erqucheck = True
        else:
            erqucheck = False

        if checkdata.loc["管理三区", "JH"] > sanquwellnum * 0.95:
            sanqucheck = True
        else:
            sanqucheck = False

        if yiqucheck and erqucheck and sanqucheck:
            return daily_data
        else:
            return False


# 计算单井掺稀上调跟踪数据
def sortwelldailychanxiupadjusttail():
    global GLOBAL_Logger
    global GlOBAL_mssql_engine

    # 检查是否上报完成
    dailydata = checkdataupdate()

    # 若未完成，等待10分钟，继续检查
    while dailydata is False:
        time.sleep(60 * 10)
        dailydata = checkdataupdate()

    # 计算掺稀调整量
    # 获取日报数据
    dailydata = dailydata.rename(columns={'JH': 'wellname', 'DWMC': 'area', 'RCXL': 'dailychanxi', 'CYQ': 'block'})
    dailydata = dailydata.set_index("wellname")
    dailydata = dailydata[dailydata["SCSJ"] == 24]
    dailydata = dailydata.drop("SCSJ", axis=1)

    # 取基准数据
    tablename = "EcWellChanxiBasedata"
    basedata = pd.read_sql(tablename, GlOBAL_mssql_engine, index_col=["JH"], columns=["chanxi"])
    basedata = basedata.rename(columns={'JH': 'wellname'})
    dailydata["basechanxi"] = basedata["chanxi"]
    dailydata["dailydiff"] = dailydata["dailychanxi"] - dailydata["basechanxi"]

    # 入库掺稀上调数据
    # （1）提取数据库已有掺稀上调井号
    tablename = "EcWellChanxiupTail"
    databaselist = pd.read_sql(tablename, GlOBAL_mssql_engine, columns=["wellname"])
    # print(databaselist)
    if len(databaselist) == 0:
        databaselist = []
    else:
        databaselist = list(x[0] for x in databaselist.values)

    # （2）整理最新掺稀上调井号数据
    dailyuptail = dailydata[dailydata["dailydiff"] >= 3]
    uptaillist = list(dailyuptail.index)

    # （3）删除已回调井号
    tablename = "EcWellChanxiupTail"
    for wellname in databaselist:
        if wellname not in uptaillist:
            del_data_sql = "DELETE FROM %s WHERE wellname = '%s' " % (tablename, wellname)
            executesql(SqlserverDataServre, del_data_sql)

    # （4）更新上调跟踪表本日上调数据
    fields = ["wellname", "area", "block", "basechanxi", "dailychanxi", "dailyupdiff"]
    for wellname in uptaillist:
        strfieds = ""
        values = [wellname, dailyuptail.loc[wellname, "area"], dailyuptail.loc[wellname, "block"],
                  dailyuptail.loc[wellname, "basechanxi"], dailyuptail.loc[wellname, "dailychanxi"],
                  dailyuptail.loc[wellname, "dailydiff"]]
        for i in range(len(fields)):
            if strfieds == "":
                strfieds = fields[i] + " = '" + str(values[i]) + "'"
            else:
                strfieds = strfieds + ", " + fields[i] + " = '" + str(values[i]) + "'"
        update_data_sql = "UPDATE %s SET %s WHERE %s = '%s'" % (tablename, strfieds, "wellname", wellname)

        if wellname in databaselist:
            executesql(SqlserverDataServre, update_data_sql)
        else:
            insertdata(SqlserverDataServre, tablename, fields, values)

            date_start_day = datetime.fromtimestamp(time.time()) - relativedelta(days=1)
            startdate = time.strftime("%Y-%m-%d", date_start_day.timetuple())
            update_data_sql = "UPDATE %s SET updatetime = '%s' WHERE %s = '%s'" % \
                              (tablename, startdate, "wellname", wellname)
            executesql(SqlserverDataServre, update_data_sql)

    # 入库掺稀调整数据记录
    # 整理上调数据
    dailyadjustrecord = dailydata[(dailydata["dailydiff"] >= 3) | (dailydata["dailydiff"] <= -3)]
    dailyadjustrecord = dailyadjustrecord.rename(columns={'dailyupdiff': 'dailyupdiff'})
    date_start_day = datetime.fromtimestamp(time.time()) - relativedelta(days=1)
    startdate = time.strftime("%Y-%m-%d", date_start_day.timetuple())
    dailyadjustrecord["updatetime"] = startdate
    dailyadjustrecord["adjusttype"] = dailyadjustrecord["dailydiff"].apply(lambda x: "上调" if x > 0 else "优化")

    # 删除已有数据
    tablename = "EcWellChanxiAdjustRecord"
    del_data_sql = "DELETE FROM %s WHERE updatetime = '%s'" % (tablename, startdate)
    executesql(SqlserverDataServre, del_data_sql)

    # 更新数据
    dailyadjustrecord.to_sql(tablename, GlOBAL_mssql_engine, if_exists='append')


# 计算单井掺稀上调跟踪数据
def sort_well_daily_chanxi_upadjust_tail():
    global GLOBAL_Logger

    GLOBAL_Logger.info("开始计算单井掺稀上调跟踪数据！")
    print("......................................................")
    print(getnowtime())
    print("开始计算单井掺稀上调跟踪数据!")

    try:
        sortwelldailychanxiupadjusttail()
        print("计算单井掺稀上调跟踪数据完毕！")
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 提取注水站注水实时数据
def extractwatterstationrealtimedata():
    # global GLOBAL_WellNameReference
    global GLOBAL_Logger
    global GlOBAL_oracle_engine

    now_time = time.strftime("%Y-%m-%d %H:00:00", datetime.fromtimestamp(time.time()).timetuple())
    tablename = "SSC_ZYZ_SS"
    fields = ["ZKMC", "CJSJ", "LSYL", "ZSB1_JKYL", "ZSB1_CKYL", "ZSB2_JKYL", "ZSB2_CKYL", "CSG1_YW", "CSG2_YW",
              "WSC_YW", "CKHG_SSLL", "CKHG_LJLL", "CKHG_YL", "ZSB1_JK_SSLL", "ZSB1_JK_LJLL", "ZSB2_JK_SSLL",
              "ZSB2_JK_LJLL"]
    colnames = ["stationname", "time", "inletpressure", "pump1inletpressure", "pump1outletpressure",
                "pump2inletpressure", "pump2outletpressure", "tank1level", "tank2level", "overflowtanklevel",
                "outlethourflow", "outletcumflow", "outletpressure", "pump1inlethourflow", "pump1inletcumflow",
                "pump2inlethourflow", "pump2inletcumflow"]

    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field
    # starttimedaily = getbegintime(60 * 24, "minutes")
    starttimehour = getbegintime(60, "minutes")

    get_data_sql = "SELECT %s FROM %s WHERE cjsj>TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') " % \
                   (str_fieds, tablename, starttimehour)
    realtimedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine)
    realtimedata.columns = colnames
    stationnames = list(set(realtimedata["stationname"]))

    stationcount = 0
    for name in stationnames:
        upfields = ["time"]
        upvalues = [now_time]

        # name = name.strip()
        if "ZYD" in name:
            stationname = name[:-3]
        elif "ZYZ" in name:
            stationname = name[:-3]
        elif "ZSZ" in name:
            stationname = name[:-3]
        else:
            stationname = name[:-2]

        upfields.append("stationname")
        upvalues.append(stationname)

        stationrealtimedata = realtimedata[realtimedata["stationname"] == name]
        stationrealtimedata = stationrealtimedata.set_index("time")
        stationrealtimedata.drop(["stationname"], axis=1, inplace=True)
        stationrealtimedata.sort_index(inplace=True)

        # 有数据
        if len(stationrealtimedata) > 0:
            # print(len(stationrealtimedata))
            for cols in ["outlethourflow", "pump1inlethourflow", "pump2inlethourflow"]:
                # print("...")
                # print(cols)
                upfields.append(cols)
                flowsum = stationrealtimedata[cols].sum()
                countnum = len(stationrealtimedata)
                colvalue = flowsum / countnum
                colvalue = round(colvalue, 2)
                if np.isnan(colvalue):
                    colvalue = "NULL"
                upvalues.append(colvalue)

            for cols in ["tank1level", "tank2level", "overflowtanklevel", "outletcumflow", "pump1inletcumflow",
                         "pump2inletcumflow", "inletpressure", "pump1inletpressure", "pump1outletpressure",
                         "pump2inletpressure", "pump2outletpressure", "outletpressure"]:
                upfields.append(cols)
                if len(stationrealtimedata.dropna()) > 0:
                    colvalue = stationrealtimedata.iloc[-1][cols]
                    colvalue = round(colvalue, 2)
                    if np.isnan(colvalue):
                        colvalue = "NULL"
                else:
                    colvalue = "NULL"
                upvalues.append(colvalue)
        else:
            for cols in ["outlethourflow", "pump1inlethourflow", "pump2inlethourflow", "tank1level", "tank2level",
                         "overflowtanklevel", "outletcumflow", "pump1inletcumflow", "pump2inletcumflow",
                         "inletpressure", "pump1inletpressure", "pump1outletpressure", "pump2inletpressure",
                         "pump2outletpressure", "outletpressure"]:
                upfields.append(cols)
                colvalue = "NULL"
                upvalues.append(colvalue)

        # 上报数据
        tablename = "cyec.dbo.EcWaterStationRealtime"
        get_data_sql = "SELECT * FROM %s WHERE %s = '%s' AND %s = '%s'" % \
                       (tablename, "time", now_time, "stationname", stationname)
        checkdata = getdatasql(SqlserverDataServre, get_data_sql)
        if len(checkdata) > 0:
            str_fieds = ""
            for i in range(len(upfields)):
                if str_fieds == "":
                    if str(upvalues[i]) == "NULL":
                        str_fieds = upfields[i] + " = " + str(upvalues[i])
                    else:
                        str_fieds = upfields[i] + " = '" + str(upvalues[i]) + "'"
                else:
                    if str(upvalues[i]) == "NULL":
                        str_fieds = str_fieds + ", " + upfields[i] + " = " + str(upvalues[i])
                    else:
                        str_fieds = str_fieds + ", " + upfields[i] + " = '" + str(upvalues[i]) + "'"

            update_data_sql = "UPDATE %s SET %s WHERE %s = '%s' and %s = '%s'" \
                              % (tablename, str_fieds, "time", now_time, "stationname", stationname)
            try:
                executesql(SqlserverDataServre, update_data_sql)
            except Exception as e:
                print(traceback.format_exc())
        else:
            try:
                insertdata(SqlserverDataServre, tablename, upfields, upvalues)
            except Exception as e:
                print(traceback.format_exc())

        stationcount = stationcount + 1

    print("%s个注水站实时数据更新完毕！" % stationcount)


# 提取注水站注水实时数据
def extract_watter_station_realtime_data():
    global GLOBAL_Logger

    # GLOBAL_Logger.info("开始更新注水站注水实时数据！")
    print("......................................................")
    print(getnowtime(), "开始更新注水站注水实时数据!")

    try:
        extractwatterstationrealtimedata()
    except Exception as e:
        GLOBAL_Logger.info("更新注水站注水实时数据！")
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 提取注水井注水量实时数据
def extractwatterwellrealtimedata():
    global GLOBAL_WellNameReference
    global GLOBAL_Logger
    global GlOBAL_oracle_engine

    print("提取注水井注水量实时数据")

    now_time = time.strftime("%Y-%m-%d %H:00:00", datetime.fromtimestamp(time.time()).timetuple())
    tablename = "SSC_ZS_SS"
    fields = ["JH", "CJSJ", "SDLL", "SSLL", "LJLL"]
    colnames = ["wellname", "time", "setflow", "hourflow", "cumflow"]

    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field
    starttimehour = getbegintime(60, "minutes")

    get_data_sql = "SELECT %s FROM %s WHERE cjsj>TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') " % \
                   (str_fieds, tablename, starttimehour)
    realtimedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine)
    realtimedata.columns = colnames

    wellnames = list(set(realtimedata["wellname"]))

    wellcount = 0
    for name in wellnames:
        # print("............................")
        upfields = ["wellname", "time"]
        upvalues = [name, now_time]

        wellrealtimedata = realtimedata[realtimedata["wellname"] == name]
        wellrealtimedata = wellrealtimedata.set_index("time")
        wellrealtimedata.drop(["wellname"], axis=1, inplace=True)
        wellrealtimedata.sort_index(inplace=True)

        # 有数据
        if len(wellrealtimedata) > 0:
            for cols in ["hourflow"]:
                upfields.append(cols)
                flowsum = wellrealtimedata["hourflow"].sum()
                countnum = len(wellrealtimedata)
                colvalue = flowsum / countnum
                colvalue = round(colvalue, 2)
                if np.isnan(colvalue):
                    colvalue = "NULL"
                upvalues.append(colvalue)

            for cols in ["setflow", "cumflow"]:
                upfields.append(cols)
                if len(wellrealtimedata.dropna()) > 0:
                    colvalue = wellrealtimedata.dropna().iloc[-1][cols]
                    colvalue = round(colvalue, 2)
                    if np.isnan(colvalue):
                        colvalue = "NULL"
                else:
                    colvalue = "NULL"
                upvalues.append(colvalue)
        else:
            for cols in ["setflow", "hourflow", "cumflow"]:
                upfields.append(cols)
                colvalue = "NULL"
                upvalues.append(colvalue)

        # 上报数据
        tablename = "cyec.dbo.EcWaterWellRealtime"
        get_data_sql = "SELECT * FROM %s WHERE %s = '%s' AND %s = '%s'" % \
                       (tablename, "time", now_time, "wellname", name)
        checkdata = getdatasql(SqlserverDataServre, get_data_sql)
        if len(checkdata) > 0:
            str_fieds = ""
            for i in range(len(upfields)):
                if str_fieds == "":
                    if str(upvalues[i]) == "NULL":
                        str_fieds = upfields[i] + " = " + str(upvalues[i])
                    else:
                        str_fieds = upfields[i] + " = '" + str(upvalues[i]) + "'"
                else:
                    if str(upvalues[i]) == "NULL":
                        str_fieds = str_fieds + ", " + upfields[i] + " = " + str(upvalues[i])
                    else:
                        str_fieds = str_fieds + ", " + upfields[i] + " = '" + str(upvalues[i]) + "'"

            update_data_sql = "UPDATE %s SET %s WHERE %s = '%s' and %s = '%s'" \
                              % (tablename, str_fieds, "time", now_time, "wellname", name)
            try:
                executesql(SqlserverDataServre, update_data_sql)
            except Exception as e:
                print(traceback.format_exc())
        else:
            try:
                insertdata(SqlserverDataServre, tablename, upfields, upvalues)
            except Exception as e:
                print(traceback.format_exc())

        wellcount = wellcount + 1

    print("%s口注水井实时数据更新完毕！" % wellcount)


# 提取注水井注水量实时数据
def extract_watter_well_realtime_data():
    global GLOBAL_Logger

    # GLOBAL_Logger.info("提取注水井注水量实时数据！")
    print("......................................................")
    print(getnowtime(), "提取注水井注水量实时数据!")

    try:
        extractwatterwellrealtimedata()
    except Exception as e:
        GLOBAL_Logger.info("提取注水井注水量实时数据！")
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 提取单井参数数据
def extractwellhourparameterdata(extracthour=0):
    global GLOBAL_WellNameReference
    global GlOBAL_mssql_engine

    # 油井计转站对照表
    tablename = "cyec.dbo.ecsjb"
    fields = ["JH", "RQ", "DWMC"]
    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE RQ>'%s'" % (str_fieds, tablename, getbegintime(5))
    # print(get_data_sql)
    realtimedata = pd.read_sql(get_data_sql, GlOBAL_mssql_engine)
    realtimedata.sort_values("RQ", ascending=False, inplace=True)
    realtimedata.drop(['RQ'], axis=1, inplace=True)
    realtimedata.drop_duplicates(inplace=True)
    areadict = {}
    for i in range(len(realtimedata)):
        wellname = realtimedata.iloc[i, 0]
        area = realtimedata.iloc[i, 1]
        areadict[wellname] = area

    # # 取数据，整理数据，上报
    starttimedaily = getbegintime(60 * (extracthour + 1), "minutes")
    endtimedaily = getbegintime(60 * extracthour, "minutes")
    uptime = time.strftime("%Y-%m-%d %H:00:00",
                           (datetime.fromtimestamp(time.time()) - relativedelta(days=extracthour)).timetuple())

    for wellname in GLOBAL_WellNameReference:
        alias = GLOBAL_WellNameReference[wellname]
        area = areadict.get(wellname, "")
        if len(alias) > 0:
            # 取数据
            tablename = "ssc_yj_ss"
            fields = ["CJSJ", "JK_YY", "JK_TY", "JK_HY", "JK_WD", "BXSXDLFZ", "BXXXDLFZ", "DL_B", "DY_B", "JRL_SYWD",
                      "JRL_YW"]
            str_fieds = ""
            for field in fields:
                if str_fieds == "":
                    str_fieds = field
                else:
                    str_fieds = str_fieds + ", " + field

            get_data_sql = "SELECT %s FROM %s WHERE cjsj>TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') and " \
                           "cjsj<TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') and JH ='%s' " % \
                           (str_fieds, tablename, starttimedaily, endtimedaily, alias)
            realtimedata = pd.read_sql(get_data_sql, GlOBAL_oracle_engine)
            realtimedata.set_index(['cjsj'], inplace=True)
            realtimedata.columns = ["yy", "ty", "hy", "jw", "sxdl", "xxdl", "dl", "dy", "lw", "cw"]
            # realtimedata = realtimedata.dropna()
            realtimedata.sort_values("cjsj")

            fields = ["date", "wellname", "area"]
            values = [uptime, wellname, area]
            isnormal = 3
            if len(realtimedata) > 0:
                for field in realtimedata.columns:
                    fields.append(field + "top")
                    fields.append(field + "bot")
                    fields.append(field)

                    maxvalue = realtimedata[field].quantile(0.85)
                    minvalue = realtimedata[field].quantile(0.15)
                    meanvalue = realtimedata[field].mean()

                    if not (field == "jw" or field == "lw" or field == "cw"):
                        if maxvalue <= 0:
                            maxvalue = 0.001
                        if minvalue <= 0:
                            minvalue = 0.001
                        if meanvalue <= 0:
                            meanvalue = 0.001

                    maxvalue = round(maxvalue, 1)
                    minvalue = round(minvalue, 1)
                    meanvalue = round(meanvalue, 1)

                    if field == "jw":
                        isnormal = 1
                    else:
                        if meanvalue == 0:
                            if isnormal:
                                isnormal = 1
                        else:
                            abnormalrate = abs(maxvalue - meanvalue) / meanvalue * 100
                            if abnormalrate > 100:
                                if maxvalue - meanvalue > 0.5:
                                    isnormal = 0
                                else:
                                    isnormal = 2
                            else:
                                if isnormal:
                                    isnormal = 1
                            abnormalrate = abs(minvalue - meanvalue) / meanvalue * 100
                            if abnormalrate > 100:
                                if maxvalue - meanvalue > 0.5:
                                    isnormal = 0
                                else:
                                    isnormal = 2
                            else:
                                if isnormal:
                                    isnormal = 1

                    if np.isnan(maxvalue):
                        maxvalue = "NULL"
                    values.append(maxvalue)
                    if np.isnan(minvalue):
                        minvalue = "NULL"
                    values.append(minvalue)
                    if np.isnan(meanvalue):
                        meanvalue = "NULL"
                    values.append(meanvalue)

                fields.append("isnormal")
                values.append(isnormal)

                # 上报数据
                tablename = "cyec.dbo.EcWellHourParameter"

                deldate = getbegintime(3)
                del_data_sql = "DELETE FROM %s WHERE %s < '%s'" % (tablename, "date", deldate)
                executesql(SqlserverDataServre, del_data_sql)

                get_data_sql = "SELECT * FROM %s WHERE %s = '%s' AND %s = '%s'" % (
                    tablename, "date", uptime, "wellname", wellname)
                checkdata = getdatasql(SqlserverDataServre, get_data_sql)
                if len(checkdata) > 0:
                    str_fieds = ""
                    for i in range(len(fields)):
                        if str_fieds == "":
                            if str(values[i]) == "NULL":
                                str_fieds = fields[i] + " = " + str(values[i])
                            else:
                                str_fieds = fields[i] + " = '" + str(values[i]) + "'"
                        else:
                            if str(values[i]) == "NULL":
                                str_fieds = str_fieds + ", " + fields[i] + " = " + str(values[i])
                            else:
                                str_fieds = str_fieds + ", " + fields[i] + " = '" + str(values[i]) + "'"

                    update_data_sql = "UPDATE %s SET %s WHERE %s = '%s' and %s = '%s'" \
                                      % (tablename, str_fieds, "date", uptime, "wellname", wellname)
                    try:
                        executesql(SqlserverDataServre, update_data_sql)
                    except Exception as e:
                        print(traceback.format_exc())
                else:
                    try:
                        insertdata(SqlserverDataServre, tablename, fields, values)
                    except Exception as e:
                        print(traceback.format_exc())


# 提取单井参数数据
def extract_well_hour_parameter_data():
    global GLOBAL_Logger

    # GLOBAL_Logger.info("开始提取单井小时参数数据！")
    print("......................................................")
    print(getnowtime(), "开始提取小时单井参数数据!")

    extracthour = 2
    try:
        for exhour in range(extracthour):
            extractwellhourparameterdata(exhour)
            # uptime = time.strftime("%m/%d",
            #                        (datetime.fromtimestamp(time.time()) - relativedelta(days=exday)).timetuple())
            print("%s小时单井参数数据提取完毕！" % extracthour)
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 开始提取局库数据
def extractxibeibureaudatabasedata(exday):
    begintime = getstartdate(exday)

    # print(begintime)

    # 生产日报
    StrFieds = ""
    tablename = "CJXT.YS_DBA01"
    GetField = ["JH", "RQ", "DWMC", "SCCW", "SCSJ", "BJ", "BJ1", "BS", "PL", "YZ", "CC", "CC1", "YY", "YY1", "TY",
                "TY1", "HY", "HY1", "JKWD", "JKWD1", "RCYL1", "RCQL", "QYB", "HS", "DY", "DL", "SXDL", "XXDL", "HDL",
                "CRFS", "CRYLX", "HHYL", "HHYHS", "HHYMD", "RCYL2", "ZCB", "BZ", "PL1"]
    FieldName = ["JH", "RQ", "DWMC", "SCCW", "SCSJ", "BJ", "BJ1", "BS", "PL", "YZ", "CC", "CC1", "YY", "YY1", "TY",
                 "TY1", "HY", "HY1", "JKWD", "JKWD1", "RCYL1", "RCQL", "QYB", "HS", "DY", "DL", "SXDL", "XXDL", "HDL",
                 "CRFS", "CRYLX", "HHYL", "HHYHS", "HHYMD", "RCYL2", "ZCB", "BZ", "PL1"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" \
                   % (StrFieds, tablename, "RQ", begintime)
    dynamicdata = getdatasql(daily_oracle_data_servre, get_data_sql)
    dynamicdata = pd.DataFrame(dynamicdata, columns=FieldName)
    dynamicdata = dynamicdata.set_index(["JH"])
    # print(dynamicdata)

    # 注水日报
    StrFieds = ""
    tablename = "CJXT.YS_DBA02"
    GetField = ["JH", "RQ", "ZSCW", "SCSJ", "ZSFS", "RZSL"]
    FieldName = ["JH", "RQ", "ZSCW", "ZSSJ", "ZSFS", "RZSL"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" \
                   % (StrFieds, tablename, "RQ", begintime)
    injectdata = getdatasql(daily_oracle_data_servre, get_data_sql)
    injectdata = pd.DataFrame(injectdata, columns=FieldName)
    injectdata = injectdata.set_index(["JH"])
    # print(injectdata)

    # 注气日报
    StrFieds = ""
    tablename = "CJXT.YS_DBC04"
    GetField = ["JH", "RQ", "ZQJLB", "ZRJZ", "ZRFS", "LJZQLC", "ZRSJ", "ZRYL", "RZQL", "RZYL", "RZSL"]
    FieldName = ["JH", "RQ", "ZQJLB", "ZRJZ", "ZRFS", "LJZQLC", "ZRSJ", "ZRYL", "RZQL", "RZYL", "RZSL"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" \
                   % (StrFieds, tablename, "RQ", begintime)
    gasinjectdata = getdatasql(daily_oracle_data_servre, get_data_sql)
    gasinjectdata = pd.DataFrame(gasinjectdata, columns=FieldName)
    gasinjectdata = gasinjectdata.set_index(["JH"])
    # print(gasinjectdata)

    # 液面数据
    StrFieds = ""
    tablename = "CJXT.YS_DCA03"
    GetField = ["JH", "CSRQ", "DYM", "JYM"]
    LevelFieldName = ["JH", "RQ", "DYM", "JYM"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" \
                   % (StrFieds, tablename, "CSRQ", begintime)
    yemiandata = getdatasql(daily_oracle_data_servre, get_data_sql)
    yemiandata = pd.DataFrame(yemiandata, columns=LevelFieldName)
    yemiandata = yemiandata.set_index(["JH"])
    # print(yemiandata)

    colnames = ["JH", "RQ"]
    get_data_sql = "SELECT DISTINCT JH, RQ FROM" \
                   "(SELECT JH, RQ  FROM CJXT.YS_DBA01 UNION SELECT JH, RQ FROM CJXT.YS_DBA02 )CYJ " \
                   "WHERE %s = TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" \
                   % ("RQ", begintime)
    data = getdatasql(daily_oracle_data_servre, get_data_sql)
    data = pd.DataFrame(data, columns=colnames)
    data = data.set_index(["JH"])
    # print(data)

    filenames = ["DWMC", "SCCW", "SCSJ", "BJ", "BJ1", "BS", "PL", "YZ", "CC", "CC1", "YY", "YY1", "TY",
                 "TY1", "HY", "HY1", "JKWD", "JKWD1", "RCYL1", "RCQL", "QYB", "HS", "DY", "DL", "SXDL", "XXDL", "HDL",
                 "CRFS", "CRYLX", "HHYL", "HHYHS", "HHYMD", "RCYL2", "ZCB", "BZ"]
    for col in filenames:
        data[col] = dynamicdata[col]
    # print(data)

    data["ZSSJ"] = injectdata["ZSSJ"]
    filenames = ["ZSCW", "ZSFS", "RZSL"]
    for col in filenames:
        data[col] = injectdata[col]
    # print(data)

    data["ZQLB"] = gasinjectdata["ZQJLB"]
    data["ZQFS"] = gasinjectdata["ZRFS"]
    data["ZQLC"] = gasinjectdata["LJZQLC"]
    data["ZQSJ"] = gasinjectdata["ZRSJ"]
    data["ZQBS"] = gasinjectdata["RZSL"]
    filenames = ["ZRJZ", "ZRYL", "RZQL", "RZYL"]
    for col in filenames:
        data[col] = gasinjectdata[col]
    # print(data)

    filenames = ["DYM", "JYM"]
    for col in filenames:
        data[col] = yemiandata[col]

    data.dropna(subset=["RQ"], inplace=True)
    # print(data)

    # 删除已有数据
    tablename = "XBYTDailyReport_B"
    del_data_sql = "DELETE FROM %s WHERE %s = '%s'" % (tablename, "RQ", begintime)
    executesql(SqlserverDataServre, del_data_sql)

    # 更新数据
    data.to_sql(tablename, GlOBAL_mssql_engine, if_exists='append')

    # print(data)

    print("%s日%s口井局库动态数据更新完毕！" % (begintime, len(data)))


# 提取局库数据
def extract_xibei_bureau_database_data():
    global GLOBAL_Logger

    # GLOBAL_Logger.info("开始提取局库动态日数据！")
    print("......................................................")
    print(getnowtime(), "开始提取局库动态日数据!")

    extractdays = 10
    try:
        for exday in range(0, extractdays):
            # if True:
            #     exday = 4216
            extractxibeibureaudatabasedata(exday + 1)
            # print("前%s日局库动态日数据提取完毕！" % str(exday + 1))
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 提取局库数据
def extract_new_xibei_bureau_database_data():
    global GLOBAL_Logger

    # GLOBAL_Logger.info("开始提取局库动态日数据！")
    print("......................................................")
    print(getnowtime(), "开始提取局库动态日数据!")

    extractdays = 10
    try:
        for exday in range(0, extractdays):
            # if True:
            #     exday = 4216
            extractnewxibeibureaudatabasedata(exday + 1)
            # print("前%s日局库动态日数据提取完毕！" % str(exday + 1))
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 开始提取局库数据
def extractnewxibeibureaudatabasedata(exday):
    begintime = getstartdate(exday)

    # print(begintime)

    # 生产日报
    StrFieds = ""
    tablename = "CJXT.YS_DBA01"
    GetField = ["JH", "RQ", "DWMC", "SCCW", "SCSJ", "BJ", "BJ1", "BS", "PL", "YZ", "CC", "CC1", "YY", "YY1", "TY",
                "TY1", "HY", "HY1", "JKWD", "JKWD1", "RCYL1", "RCQL", "QYB", "HS", "DY", "DL", "SXDL", "XXDL", "HDL",
                "CRFS", "CRYLX", "HHYL", "HHYHS", "HHYMD", "RCYL2", "ZCB", "BZ", "PL1"]
    FieldName = ["JH", "RQ", "DWMC", "SCCW", "SCSJ", "BJ", "BJ1", "BS", "PL", "YZ", "CC", "CC1", "YY", "YY1", "TY",
                 "TY1", "HY", "HY1", "JKWD", "JKWD1", "RCYL1", "RCQL", "QYB", "HS", "DY", "DL", "SXDL", "XXDL", "HDL",
                 "CRFS", "CRYLX", "HHYL", "HHYHS", "HHYMD", "RCYL2", "ZCB", "BZ", "PL1"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" \
                   % (StrFieds, tablename, "RQ", begintime)
    dynamicdata = getdatasql(daily_oracle_data_servre, get_data_sql)
    dynamicdata = pd.DataFrame(dynamicdata, columns=FieldName)
    dynamicdata = dynamicdata.set_index(["JH"])
    # print(dynamicdata)

    # 注水日报
    StrFieds = ""
    tablename = "CJXT.YS_DBA02"
    GetField = ["JH", "RQ", "ZSCW", "SCSJ", "ZSFS", "RZSL"]
    FieldName = ["JH", "RQ", "ZSCW", "ZSSJ", "ZSFS", "RZSL"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" \
                   % (StrFieds, tablename, "RQ", begintime)
    injectdata = getdatasql(daily_oracle_data_servre, get_data_sql)
    injectdata = pd.DataFrame(injectdata, columns=FieldName)
    injectdata = injectdata.set_index(["JH"])
    # print(injectdata)

    # 注气日报
    StrFieds = ""
    tablename = "CJXT.YS_DBC04"
    GetField = ["JH", "RQ", "ZQJLB", "ZRJZ", "ZRFS", "LJZQLC", "ZRSJ", "ZRYL", "RZQL", "RZYL", "RZSL"]
    FieldName = ["JH", "RQ", "ZQJLB", "ZRJZ", "ZRFS", "LJZQLC", "ZRSJ", "ZRYL", "RZQL", "RZYL", "RZSL"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" \
                   % (StrFieds, tablename, "RQ", begintime)
    gasinjectdata = getdatasql(daily_oracle_data_servre, get_data_sql)
    gasinjectdata = pd.DataFrame(gasinjectdata, columns=FieldName)
    gasinjectdata = gasinjectdata.set_index(["JH"])
    # print(gasinjectdata)

    # 液面数据
    StrFieds = ""
    tablename = "CJXT.YS_DCA03"
    GetField = ["JH", "CSRQ", "DYM", "JYM"]
    LevelFieldName = ["JH", "RQ", "DYM", "JYM"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" \
                   % (StrFieds, tablename, "CSRQ", begintime)
    yemiandata = getdatasql(daily_oracle_data_servre, get_data_sql)
    yemiandata = pd.DataFrame(yemiandata, columns=LevelFieldName)
    yemiandata = yemiandata.set_index(["JH"])
    # print(yemiandata)

    # 补充数据
    StrFieds = ""
    tablename = "XBYY.DBA01"
    GetField = ["JH", "RQ", "LY", "JY", "DMYYMD"]
    AddFieldName = ["JH", "RQ", "LY", "JY", "DMYYMD"]
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" \
                   % (StrFieds, tablename, "RQ", begintime)
    adddata = getdatasql(daily_oracle_data_servre, get_data_sql)
    adddata = pd.DataFrame(adddata, columns=AddFieldName)
    adddata = adddata.set_index(["JH"])
    # print(yemiandata)

    colnames = ["JH", "RQ"]
    get_data_sql = "SELECT DISTINCT JH, RQ FROM" \
                   "(SELECT JH, RQ  FROM CJXT.YS_DBA01 UNION SELECT JH, RQ FROM CJXT.YS_DBA02 )CYJ " \
                   "WHERE %s = TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" \
                   % ("RQ", begintime)
    data = getdatasql(daily_oracle_data_servre, get_data_sql)
    data = pd.DataFrame(data, columns=colnames)
    data = data.set_index(["JH"])
    # print(data)

    filenames = ["DWMC", "SCCW", "SCSJ", "BJ", "BJ1", "BS", "PL", "YZ", "CC", "CC1", "YY", "YY1", "TY",
                 "TY1", "HY", "HY1", "JKWD", "JKWD1", "RCYL1", "RCQL", "QYB", "HS", "DY", "DL", "SXDL", "XXDL", "HDL",
                 "CRFS", "CRYLX", "HHYL", "HHYHS", "HHYMD", "RCYL2", "ZCB", "BZ"]
    for col in filenames:
        data[col] = dynamicdata[col]
    # print(data)

    data["ZSSJ"] = injectdata["ZSSJ"]
    filenames = ["ZSCW", "ZSFS", "RZSL"]
    for col in filenames:
        data[col] = injectdata[col]
    # print(data)

    data["ZQLB"] = gasinjectdata["ZQJLB"]
    data["ZQFS"] = gasinjectdata["ZRFS"]
    data["ZQLC"] = gasinjectdata["LJZQLC"]
    data["ZQSJ"] = gasinjectdata["ZRSJ"]
    data["ZQBS"] = gasinjectdata["RZSL"]
    filenames = ["ZRJZ", "ZRYL", "RZQL", "RZYL"]
    for col in filenames:
        data[col] = gasinjectdata[col]
    # print(data)

    filenames = ["DYM", "JYM"]
    for col in filenames:
        data[col] = yemiandata[col]

    filenames = ["LY", "JY", "DMYYMD"]
    for col in filenames:
        data[col] = adddata[col]

    data.dropna(subset=["RQ"], inplace=True)
    # print(data)

    # 删除已有数据
    tablename = "XBYTDailyReport_B"
    del_data_sql = "DELETE FROM %s WHERE %s = '%s'" % (tablename, "RQ", begintime)
    executesql(SqlserverDataServre_BRU, del_data_sql)

    # 更新数据
    data.to_sql(tablename, GlOBAL_mssql_engine_brU, if_exists='append')
    # global GlOBAL_mssql_engine_brU
    # print(data)

    print("%s日%s口井局库动态数据更新完毕！" % (begintime, len(data)))


# # 提取局库数据test
# def extract_xibei_bureau_database_datatest():
#     global GLOBAL_Logger
#
#     # GLOBAL_Logger.info("开始提取局库动态日数据！")
#     print("......................................................")
#     print(getnowtime(), "开始提取局库动态日数据!")
#
#     extractdays = 365*50
#     try:
#         for exday in range(0, extractdays):
#             print(exday)
#             extractxibeibureaudatabasedatatest(exday + 1)
#         # extractxibeibureaudatabasedatatest(77 + 1)
#     except Exception as e:
#         # GLOBAL_Logger.info(traceback.format_exc())
#         print(traceback.format_exc())
#
# # 开始提取局库数据
# def extractxibeibureaudatabasedatatest(exday):
#     begintime = getstartdate(exday)
#
#     # 液面数据
#     StrFieds = ""
#     tablename = "CJXT.YS_DCA03"
#     GetField = ["JH", "CSRQ", "DYM", "JYM"]
#     LevelFieldName = ["JH", "RQ", "DYM", "JYM"]
#     for field in GetField:
#         if StrFieds == "":
#             StrFieds = field
#         else:
#             StrFieds = StrFieds + ", " + field
#     get_data_sql = "SELECT %s FROM %s WHERE %s = TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" \
#                    % (StrFieds, tablename, "CSRQ", begintime)
#     yemiandata = getdatasql(daily_oracle_data_servre, get_data_sql)
#     yemiandata = pd.DataFrame(yemiandata, columns=LevelFieldName)
#     # yemiandata = yemiandata.set_index(["JH"])
#     # print(yemiandata)
#     #
#     # print(len(yemiandata))
#
#     tablename = "XBYTDailyReport"
#     fields = ["DYM", "JYM"]
#
#     for i in range(len(yemiandata)):
#         wellname = yemiandata.iloc[i]["JH"]
#         date = yemiandata.iloc[i]["RQ"]
#         dyemian = yemiandata.iloc[i]["DYM"]
#         jyemian = yemiandata.iloc[i]["JYM"]
#
#         # print(jyemian is None)
#
#         if dyemian is not None and dyemian >= 0:
#             StrFieds = "DYM = '%s'" % dyemian
#             update_data_sql = "UPDATE %s SET %s WHERE %s = '%s' and %s = '%s'" % \
#                               (tablename, StrFieds, "JH", wellname, "RQ", date)
#             executesql(SqlserverDataServre, update_data_sql)
#
#         if jyemian is not None and jyemian >= 0:
#             StrFieds = "JYM = '%s'" % jyemian
#             update_data_sql = "UPDATE %s SET %s WHERE %s = '%s' and %s = '%s'" % \
#                               (tablename, StrFieds, "JH", wellname, "RQ", date)
#             executesql(SqlserverDataServre, update_data_sql)
#
#     print("%s日液面数据更新完毕！" % begintime)

# 提取管道参数数据（每天）
def extract_pipeline_daily_data():
    global GLOBAL_Logger

    print("......................................................")
    print(getnowtime(), "提取昨天管道数据!")

    try:
        extractpipelinedatadaily()
    except Exception as e:
        GLOBAL_Logger.info("提取昨天管道数据！")
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 提取管道参数数据（每天）
def extractpipelinedatadaily():
    global GlOBAL_pipeline_oracle_engine
    global GlOBAL_mssql_engine

    # 获取数据
    begintime = getstartdate(1)
    tablename = "XBYQJS.GC_GDXX"
    get_data_sql = "SELECT * FROM %s WHERE %s >= TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss')" % (
        tablename, "GXRQ", begintime)
    data = pd.read_sql(get_data_sql, GlOBAL_pipeline_oracle_engine, index_col=["id"])
    if len(data.index) > 0:
        data.columns = [item.upper() for item in pd.DataFrame(data).columns.values.tolist()]

        # 删除已有数据
        tablename = "EcGatheringPipeline"
        for id in data.index:
            del_data_sql = "DELETE FROM %s WHERE %s = '%s'" % (tablename, "ID", id)
            executesql(SqlserverDataServre, del_data_sql)

        # 更新数据
        data.to_sql(tablename, GlOBAL_mssql_engine, if_exists='append')
        print("%s到目前，共%s口井管道数据更新完毕！" % (begintime, len(data)))
    else:
        print("%s到目前，无管道数据可更新！" % begintime)


# 提取管道参数数据（每月）
def extractpipelinedatamonthly():
    global GlOBAL_mssql_engine
    global GlOBAL_pipeline_oracle_engine

    # 获取数据
    tablename = "XBYQJS.GC_GDXX"
    get_data_sql = "SELECT * FROM %s" % (tablename)
    data = pd.read_sql(get_data_sql, GlOBAL_pipeline_oracle_engine, index_col=["id"])
    data.columns = [item.upper() for item in pd.DataFrame(data).columns.values.tolist()]

    # 删除已有数据
    tablename = "EcGatheringPipeline"
    del_data_sql = "DELETE FROM %s" % (tablename)
    executesql(SqlserverDataServre, del_data_sql)

    # 更新数据
    data.to_sql(tablename, GlOBAL_mssql_engine, if_exists='append')

    # print(data)

    print("共%s口井管道数据更新完毕！" % (len(data)))


# 提取管道参数数据（每月）
def extract_pipeline_monthly_data():
    global GLOBAL_Logger

    print("......................................................")
    print(getnowtime(), "提取所有管道数据!")

    try:
        extractpipelinedatamonthly()
    except Exception as e:
        GLOBAL_Logger.info("提取所有管道数据！")
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 若数字为空，则返回0
def formatnum(num):
    return 0 if num is None else num


# 计算最近3天措施井增油量
def extractmeasureoilincrement(caldate):
    global GlOBAL_mssql_engine
    tablename = "EcWellAccountMeasure"
    oil_increment_table = "EcWellOilIncrementMeasure"
    ecsjb = "ecsjb"

    # 获取措施表中的所有井号
    get_wellname = "SELECT %s FROM %s" % ("wellname", tablename)
    wellnamelist = pd.read_sql(get_wellname, GlOBAL_mssql_engine)
    wellnamelist.drop_duplicates(inplace=True)
    wellnamelist = [x[0] for x in list(wellnamelist.values)]

    curdate_str = getstartdate(caldate).split(" ")[0]
    curyear_str = time.strftime("%Y-01-01",
                                (datetime.fromtimestamp(time.time()) - relativedelta(days=caldate)).timetuple())
    curdate = datetime.strptime(curdate_str, '%Y-%m-%d')

    count = 0
    for wellname in wellnamelist:

        # 一次性获取最新的两次数据，如果最新一次措施还没开始，则以前面一次增油基值为准，如果措施已经结束，则以最新一次增油基值为准
        get_date = "SELECT TOP 2 * FROM %s WHERE wellname = '%s' AND geologicaldate <= '%s' AND geologicaldate >= '%s'" \
                   "ORDER BY startdate DESC" % (tablename, wellname, curdate_str, curyear_str)
        datares = pd.read_sql(get_date, GlOBAL_mssql_engine)

        if len(datares) > 0:
            # startdate = datetime.strptime(dataRes.loc[0, "startdate"], '%Y-%m-%d')

            # 删除已有数据
            del_data_sql = "DELETE FROM %s WHERE %s = '%s' AND  %s = '%s'" % (
                oil_increment_table, "wellname", wellname, "date", curdate_str)
            executesql(SqlserverDataServre, del_data_sql)

            # 获取日报数据中的管理区、区块、产油量数据
            get_sjb = "SELECT DWMC, CYQ, RCYL FROM %s WHERE JH = '%s' AND RQ = '%s'" \
                      % (ecsjb, wellname, curdate_str)
            ecsjb_res = pd.read_sql(get_sjb, GlOBAL_mssql_engine)
            ecsjb_res.columns = ["area", "block", "oilOutput"]

            # 若有日报数据，则进行后续计算
            if len(ecsjb_res) > 0:
                res = pd.DataFrame()

                # 若该井已有施工计划，但还未施工，即最近一次开工日期大于当前日期，则当前的增油量应以前一次措施的增油基值为准
                if len(datares) == 2 and datares.loc[0, "geologicaldate"] is None:
                    geologicaldate = datetime.strptime(datares.loc[1, "geologicaldate"], '%Y-%m-%d')
                    oil_increment = formatnum(ecsjb_res.loc[0, "oilOutput"]) - formatnum(datares.loc[1, "basevalue"])
                    if oil_increment < 0:
                        oil_increment = 0
                    temp_res = datares.iloc[[1]]
                    temp_res.index = [0]
                    res = pd.concat([res, temp_res], axis=1)

                # 若该井施工完毕，即最近一次地质完工日期小于等于当前日期，则当前的增油量以最后一次措施基值为准
                elif (not (datares.loc[0, "geologicaldate"] is None)) and \
                        (datetime.strptime(datares.loc[0, "geologicaldate"], '%Y-%m-%d') - curdate).days <= 0:
                    geologicaldate = datetime.strptime(datares.loc[0, "geologicaldate"], '%Y-%m-%d')
                    oil_increment = formatnum(ecsjb_res.loc[0, "oilOutput"]) - formatnum(datares.loc[0, "basevalue"])
                    if oil_increment < 0:
                        oil_increment = 0
                    res = pd.concat([res, datares.iloc[[0]]], axis=1)
                else:
                    continue

                # 插入数据
                if (curdate.year - geologicaldate.year) == 0:
                    res = pd.concat([ecsjb_res, res], axis=1)
                    res.loc[0, "addvalue"] = oil_increment
                    res.loc[0, "date"] = curdate_str
                    res.to_sql(oil_increment_table, GlOBAL_mssql_engine, if_exists='append', index=False)
                    count = count + 1
    print("%s日%s条措施数据更新完毕！" % (curdate_str, count))


# 计算最近3天措施井增油量
def extract_measure_oil_increment():
    global GLOBAL_Logger

    print("......................................................")
    print(getnowtime(), "计算每日措施增油量!")

    extractdays = 3
    try:
        for exday in range(extractdays):
            extractmeasureoilincrement(exday)
    except Exception as e:
        GLOBAL_Logger.info("计算每日措施增油量！")
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 提取倒运数据（每小时）
def extract_tanker_shipment_data():
    global GLOBAL_Logger
    print("......................................................")

    try:
        extracttankershipmentdata()
    except Exception as e:
        GLOBAL_Logger.info("提取三倒数据！")
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 提取三倒数据（每小时）
def extracttankershipmentdata():
    global GlOBAL_tanker_shipment_oracle_engine
    global GlOBAL_mssql_engine

    # 获取数据
    begintime = datetime.fromtimestamp(time.time()) - relativedelta(days=2)
    begintime = time.strftime("%Y-%m-%d %H:%M:%S", begintime.timetuple())
    tablename = "XBBF1.DYSJ_RC"
    get_data_sql = "SELECT * FROM %s WHERE %s >= TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') or %s >= TO_DATE('%s', " \
                   "'yyyy-mm-dd,hh24:mi:ss') or %s >= TO_DATE('%s', 'yyyy-mm-dd,hh24:mi:ss') or %s >= TO_DATE('%s', " \
                   "'yyyy-mm-dd,hh24:mi:ss') " % (
                       tablename, "PCSJ", begintime, "ZBSJ", begintime, "QBSJ", begintime, "WCSJ", begintime)
    # get_data_sql = "SELECT * FROM %s " % tablename

    data = pd.read_sql(get_data_sql, GlOBAL_tanker_shipment_oracle_engine)
    data.drop_duplicates(subset=["pcdh"], inplace=True)
    data.set_index(["pcdh"], inplace=True)
    # print(data)

    if len(data.index) > 0:
        data.columns = [item.upper() for item in pd.DataFrame(data).columns.values.tolist()]

        # 删除已有数据
        tablename = "EcTankerShipment"
        for bh in data.index:
            del_data_sql = "DELETE FROM %s WHERE %s = '%s'" % (tablename, "PCDH", bh)
            executesql(SqlserverDataServre, del_data_sql)

        # 更新数据
        data.to_sql(tablename, GlOBAL_mssql_engine, if_exists='append')
        print("%s到目前，共%s口井三倒数据更新完毕！" % (begintime, len(data)))
    else:
        print("%s到目前，无三倒数据可更新！" % begintime)


# 生成机采井月报数据
def mechanicalextractmonthlyreport():
    global GLOBAL_WellNameReference
    global GlOBAL_mssql_engine

    dytablename = "ecsjb"
    densitytablename = "EcWellDensity"
    yemiantablename = "EcWellTest"

    # 提取检泵周期数据
    starttime = getbegintime(5000).split(' ')[0]
    fields = ["JH", "RQ", "SCSJ", "BS"]
    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field
    get_welldetail_sql = "SELECT %s FROM %s WHERE RQ > '%s' AND SCSJ > 0 " % (str_fieds, dytablename, starttime)
    welldbsdata = pd.read_sql(get_welldetail_sql, GlOBAL_mssql_engine, index_col="JH")
    welldbsdata.index.names = ['wellname']
    welldbsdata["BSgroup"] = welldbsdata["BS"].round(0)
    welldbsdata = welldbsdata.groupby([welldbsdata.index, welldbsdata["BSgroup"]], ).sum()
    # print(welldbsdata)

    pumpdic = {}
    for x, y in welldbsdata.index:
        # print("...........")
        wellname = x
        pumpdeep = y
        productdays = round(welldbsdata.loc[(x, y), ["SCSJ"]][0] / 24, 2)

        if wellname not in pumpdic:
            pumpdic[wellname] = [pumpdeep, productdays, 0, 0]
        else:
            pumpfac = pumpdic[wellname]
            pumpfac[2] = pumpdeep
            pumpfac[3] = productdays
            pumpdic[wellname] = pumpfac

    fieldnames = ["pumpdeep", "pumpdays", "pumpdeep2", "pumpdays2"]
    welldbsdata = pd.DataFrame(pumpdic, index=fieldnames)
    welldbsdata = welldbsdata.stack()
    welldbsdata = welldbsdata.unstack(level=0)
    welldbsdata["puminspectionperiod"] = welldbsdata.apply(lambda x: max(x.pumpdays, x.pumpdays2), axis=1)
    # print(welldbsdata)

    # 提取液面数据
    fields = ["wellname", "date", "dynamicfluidlevel", "staticfluidlevel"]
    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field
    get_density_sql = "SELECT %s FROM %s WHERE date > '%s' ORDER BY date DESC" % \
                      (str_fieds, yemiantablename, getbegintime(180))
    wellfluidlevel = pd.read_sql(get_density_sql, GlOBAL_mssql_engine, index_col="wellname")

    # 计算液面
    def getfluidlevel(dynamicfluidlevel, staticfluidlevel):
        if dynamicfluidlevel >= 0:
            fluidlevel = dynamicfluidlevel
        else:
            fluidlevel = staticfluidlevel
        return fluidlevel

    wellfluidlevel["fluidlevel"] = wellfluidlevel.apply(
        lambda x: getfluidlevel(x.dynamicfluidlevel, x.staticfluidlevel), axis=1)

    wellfluidlevel["sortname"] = wellfluidlevel.index
    wellfluidlevel.drop_duplicates("sortname", inplace=True)
    # print(wellfluidlevel)

    # 提取密度数据
    fields = ["wellname", "testdate", "standarddensity"]
    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field
    get_density_sql = "SELECT %s FROM %s WHERE testdate > '%s' ORDER BY testdate DESC" % \
                      (str_fieds, densitytablename, getbegintime(180))
    welldensity = pd.read_sql(get_density_sql, GlOBAL_mssql_engine, index_col="wellname")
    welldensity["sortname"] = welldensity.index
    welldensity = welldensity[welldensity["standarddensity"] > 0]

    welldensity = welldensity.groupby(welldensity.index).mean()
    # print(welldensity)
    #
    # 提取动态数据
    days = calendar.mdays[time.localtime(time.time()).tm_mon]

    starttime = getbegintime(days).split(' ')[0]
    endtime = getbegintime(1).split(' ')[0]

    fields = ["JH", "RQ", "DWMC", "CYQ", "SCSJ", "BJ", " BJ1", "BS", "PL", "YZ", "CC", "CC1", "RCYL1", "RCYL", "RCXL",
              "HHHS", "SCFS", "PL1"]
    str_fieds = ""
    for field in fields:
        if str_fieds == "":
            str_fieds = field
        else:
            str_fieds = str_fieds + ", " + field
    get_welldetail_sql = "SELECT %s FROM %s WHERE RQ >= '%s' and RQ <= '%s' " % \
                         (str_fieds, dytablename, starttime, endtime)
    welldynimicdata = pd.read_sql(get_welldetail_sql, GlOBAL_mssql_engine, index_col="JH")
    welldynimicdata.index.names = ['wellname']

    # print(welldynimicdata)
    # # 计算标签列
    def getTag(pumpType):
        return {
            '螺杆泵': 0,
            '管式泵': 1,
            '抽稠泵': 2,
            '电潜泵': 3,
        }.get(pumpType, 4)

    # 计算油井基本信息
    wellsortdata = welldynimicdata.loc[:, ["RQ", "DWMC", "CYQ", "SCFS"]]
    wellsortdata["wellname"] = wellsortdata.index
    wellsortdata.sort_values("RQ", ascending=False, inplace=True)
    wellsortdata.drop_duplicates("wellname", inplace=True)
    wellsortdata['typetag'] = wellsortdata.apply(lambda x: getTag(x.SCFS), axis=1)

    welldata = wellsortdata.loc[:, ["DWMC", "CYQ", "SCFS", "typetag"]]
    welldata.rename(columns={"DWMC": "area", "CYQ": "block", "SCFS": "welltype"}, inplace=True)
    welldata["welltype"] = welldata['welltype'].fillna('油管自喷')

    welldata["date"] = time.strftime("%Y/%m/%d", time.localtime())
    welldata["year"] = time.localtime(time.time()).tm_year
    welldata["month"] = time.localtime(time.time()).tm_mon
    welldata["sumcalendar"] = days * 24

    welldata["pumpdeep"] = welldbsdata.loc[:, ["pumpdeep"]]
    welldata["pumpdays"] = welldbsdata.loc[:, ["pumpdays"]]
    welldata["puminspectionperiod"] = welldbsdata.loc[:, ["puminspectionperiod"]]

    welldata["fluidlevel"] = wellfluidlevel["fluidlevel"]

    welldata["density"] = welldensity.loc[:, ["standarddensity"]]

    # 计算求和项
    # 计算理论排量
    def gettheory_pl(teg, pl, hz):
        if teg == 3:
            pl = pl / hz * 50
        elif teg == 4:
            pl = np.nan
        return pl

    # 计算实际理论排量
    def getactualtheory_pl(teg, pl, cs):
        if teg == 4:
            pl = np.nan
        else:
            if cs == 0:
                pl = np.nan
            else:
                pl = pl / 24 * cs
        return pl

    wellsumdate = welldynimicdata.loc[:, ["RQ", "SCFS", "PL", "PL1", "SCSJ", "RCYL1", "RCYL", "RCXL"]]
    wellsumdate.sort_values("RQ", ascending=False, inplace=True)
    wellsumdate['pumptype'] = wellsumdate.apply(lambda x: getTag(x.SCFS), axis=1)
    wellsumdate['sumtheorypl'] = wellsumdate.apply(lambda x: gettheory_pl(x.pumptype, x.PL, x.PL1), axis=1)
    wellsumdate['sumactualtheorypl'] = wellsumdate.apply(lambda x: getactualtheory_pl(x.pumptype, x.PL, x.SCSJ), axis=1)

    wellsumdate = wellsumdate.groupby([wellsumdate.index, wellsumdate["pumptype"]]).sum()
    wellsumdate["wellteg"] = [x[0] for x in wellsumdate.index]
    wellsumdate.drop_duplicates("wellteg", inplace=True)
    wellsumdate.drop(columns=['pumptype'], inplace=True)
    wellsumdate.reset_index(level=1, inplace=True)
    # print(wellsumdate)

    welldata["sumtheorypl"] = wellsumdate.loc[:, ["sumtheorypl"]]
    welldata["sumactualtheorypl"] = wellsumdate.loc[:, ["sumactualtheorypl"]]
    welldata["theorypl"] = welldata["sumtheorypl"] / days
    welldata["sumchanyet"] = wellsumdate["RCYL1"]
    welldata["sumchanyout"] = wellsumdate["RCYL"]
    welldata["sumchanxit"] = wellsumdate["RCXL"]

    # 计算地产液量
    def getchanyef(density, chanye, chanyou, chanxit):
        if chanye <= 0:
            chanyef = 0
        else:
            if density > 0:
                chanyef = (chanye + chanxit) / density - chanxit / 0.91
            else:
                chanyef = (chanye - chanyou) / 1.14 + chanyou
        return chanyef

    # 计算混产液量
    def gethunyef(density, chanye, chanyou, chanxit):
        if chanye <= 0:
            hunyef = 0
        else:
            if density > 0:
                hunyef = (chanye + chanxit) / density
            else:
                hunyef = (chanye - chanyou) / 1.14 + chanyou + chanxit * 0.91
        return hunyef

    welldata['sumchanyef'] = welldata.apply(
        lambda x: getchanyef(x.density, x.sumchanyet, x.sumchanyout, x.sumchanxit), axis=1)
    welldata['sumhunyef'] = welldata.apply(
        lambda x: gethunyef(x.density, x.sumchanyet, x.sumchanyout, x.sumchanxit), axis=1)

    # 计算泵效
    def getpumpefficiency(chanye, pailiang):
        if pailiang > 0:
            efficiency = chanye / pailiang * 100
        else:
            efficiency = np.nan
        return efficiency

    welldata["pumpefficiency"] = welldata.apply(lambda x: getpumpefficiency(x.sumhunyef, x.sumtheorypl), axis=1)

    # 计算统计泵效
    def getstpumpefficiency(teg, pumpefficiency):
        if teg == 3:
            if pumpefficiency <= 120:
                stpumpefficiency = pumpefficiency
            else:
                stpumpefficiency = np.nan
        elif teg < 4:
            if pumpefficiency <= 100:
                stpumpefficiency = pumpefficiency
            else:
                stpumpefficiency = np.nan
        else:
            stpumpefficiency = np.nan
        return stpumpefficiency

    welldata["stpumpefficiency"] = welldata.apply(
        lambda x: getstpumpefficiency(x.typetag, x.pumpefficiency), axis=1)

    # print(welldata)

    wellpumbdate = welldynimicdata.loc[:, ["RQ", "BJ", "BJ1"]]
    wellpumbdate.sort_values("RQ", ascending=False, inplace=True)
    wellpumbdate["wellname"] = wellpumbdate.index
    wellpumbdate.drop_duplicates("wellname", inplace=True)

    # 计算泵径
    def getbengjing(bj1, bj2):
        if bj2 >= 0:
            bj = str(int(bj1)) + "/" + str(int(bj2))
        elif bj1 >= 0:
            bj = str(int(bj1))
        else:
            bj = np.nan
        return bj

    wellpumbdate['bengjing'] = wellpumbdate.apply(lambda x: getbengjing(x.BJ, x.BJ1), axis=1)

    # 计算折算泵径
    def getzhesuanbengjiang(bj):
        return {
            '56/38': 41.13,
            '70/44': 54.44,
            '70/32': 62.26,
            '83/44': 70.38,
        }.get(bj, np.nan)

    wellpumbdate['zhesuanbengjing'] = wellpumbdate.apply(lambda x: getzhesuanbengjiang(x.bengjing), axis=1)
    welldata["bengjing"] = wellpumbdate["bengjing"]
    welldata["zhesuanbengjing"] = wellpumbdate["zhesuanbengjing"]
    # print(welldata)

    welldynimicmeandata = welldynimicdata.groupby(welldynimicdata.index).mean()
    # print(welldynimicmeandata)

    welldata["chongcheng"] = welldynimicmeandata["CC"]
    welldata["chongce"] = welldynimicmeandata["CC1"]
    welldata["youzui"] = welldynimicmeandata["YZ"]
    welldata["hunhehanshui"] = welldynimicmeandata["HHHS"]

    welldynimicsumdata = welldynimicdata.groupby(welldynimicdata.index).sum()
    #
    # print(welldynimicsumdata)
    welldata["sumproducttime"] = welldynimicsumdata["SCSJ"]
    welldata["timeefficiency"] = welldata["sumproducttime"] / welldata["sumcalendar"] * 100
    # print(welldata)

    welldata["avgchanyef"] = welldata["sumchanyef"] / welldata["sumproducttime"] * 24
    welldata["avgchanyet"] = welldata["sumchanyet"] / welldata["sumproducttime"] * 24
    welldata["avgchanyout"] = welldata["sumchanyout"] / welldata["sumproducttime"] * 24
    welldata["avgchanxit"] = welldata["sumchanxit"] / welldata["sumproducttime"] * 24

    # 统计液面
    def getstfluidlevel(teg, pumpdeep, fluidlevel, chanye):
        if teg < 4 and chanye > 0:
            if fluidlevel >= 0:
                stfluidlevel = fluidlevel
            else:
                stfluidlevel = pumpdeep - 1000
        else:
            stfluidlevel = np.nan

        return stfluidlevel

    welldata["statisticfluidlevel"] = welldata.apply(
        lambda x: getstfluidlevel(x.typetag, x.pumpdeep, x.fluidlevel, x.sumchanyet), axis=1)
    welldata["submergence"] = welldata["pumpdeep"] - welldata["fluidlevel"]

    # print(welldata)
    tablename = "EcWellAccountMachineMining"
    checkyear = time.localtime(time.time()).tm_year
    checkmonth = time.localtime(time.time()).tm_mon
    checkdate = time.strftime("%Y/%m/%d", time.localtime())
    if len(welldata.index) > 0:
        # 删除本月已有数据
        get_checkdata_sql = "SELECT * FROM %s WHERE month = '%s' AND year = '%s' " % \
                            (tablename, checkmonth, checkyear)
        wellcheckdata = pd.read_sql(get_checkdata_sql, GlOBAL_mssql_engine)
        if len(wellcheckdata.index) > 0:
            del_data_sql = "DELETE FROM %s WHERE month = '%s' AND year = '%s'" % (tablename, checkmonth, checkyear)
            executesql(SqlserverDataServre, del_data_sql)

        # 更新数据
        welldata.to_sql(tablename, GlOBAL_mssql_engine, if_exists='append')
        print("%s日%s口井机采数据更新完毕！" % (checkdate, len(welldata)))
    else:
        print("%s日无机采数据可更新！" % checkdate)


# 生成机采井月报数据
def mechanical_extract_monthly_report():
    global GLOBAL_Logger

    print("......................................................")
    print(getnowtime(), "开始提取机采参数数据!")

    try:
        mechanicalextractmonthlyreport()
        print("机采参数数据提取完毕！")
    except Exception as e:
        GLOBAL_Logger.info("提取机采参数数据！")
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


# 开始提取二厂单井曲线数据
def extracterchangwelldynicurvedata(exday):
    begintime = getstartdate(exday)

    # print(begintime)

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
    for field in GetField:
        if StrFieds == "":
            StrFieds = field
        else:
            StrFieds = StrFieds + ", " + field
    get_data_sql = "SELECT %s FROM %s WHERE %s = '%s'" % (StrFieds, tablename, "RQ", begintime)
    dynamicdata = getdatasql(SqlserverDataServre, get_data_sql)
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
    enleveldata = getdatasql(SqlserverDataServre, get_data_sql)
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
    meteringdata = getdatasql(SqlserverDataServre, get_data_sql)
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
    viscositydata = getdatasql(SqlserverDataServre, get_data_sql)
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
    moisturedata = getdatasql(SqlserverDataServre, get_data_sql)
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
    densitydata = getdatasql(SqlserverDataServre, get_data_sql)
    densitydata = pd.DataFrame(densitydata, columns=FieldName)
    densitydata.sort_values(by="MD", inplace=True)
    densitydata.drop_duplicates(subset=["JH"], keep='last', inplace=True)
    densitydata = densitydata.set_index(["JH"])
    # print(densitydata)

    # # 静态数据
    # StrFieds = ""
    # tablename = "EcWellHisData"
    # GetField = ["RQ", "JH", "DWMC", "CYQ", "DYFS", "CXFS", "SCFS", "ISCX", "JB", "SCZT", "ZYLX", "JLFS"]
    # FieldName = ["RQ", "JH", "DWMC", "CYQ", "DYFS", "CXFS", "SCFS", "ISCX", "JB", "SCZT", "ZYLX", "JLFS"]
    # for field in GetField:
    #     if StrFieds == "":
    #         StrFieds = field
    #     else:
    #         StrFieds = StrFieds + ", " + field
    # get_data_sql = "SELECT %s FROM %s WHERE %s >= '%s'" % (StrFieds, tablename, "RQ", getstartdate(10))
    # staticdata = getdatasql(SqlserverDataServre, get_data_sql)
    # staticdata = pd.DataFrame(staticdata, columns=FieldName)
    # staticdata.sort_values(by="RQ", inplace=True)
    # staticdata.drop_duplicates(subset=["JH", "DWMC", "CYQ", "DYFS", "CXFS", "ZYLX", "JLFS"], keep='last', inplace=True)
    # staticdata.fillna(method="ffill", inplace=True)
    # staticdata.drop_duplicates(subset=["JH"], keep='last', inplace=True)
    # staticdata = staticdata.set_index(["JH"])

    # print(staticdata)

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

    # 更新数据

    print(dynamicdata)

    dynamicdata.to_sql(tablename, GlOBAL_mssql_engine, if_exists='append')

    # # 删除已有数据
    # tablename = "EcWellStaticData"
    # del_data_sql = "DELETE FROM %s " % tablename
    # executesql(SqlserverDataServre, del_data_sql)
    #
    # # 更新数据
    # staticdata.to_sql(tablename, GlOBAL_mssql_engine, if_exists='append')

    print("%s日%s口井单井曲线数据更新完毕！" % (begintime, len(dynamicdata)))


# 提取二厂单井曲线数据
def extract_erchang_welldynicurve_data():
    global GLOBAL_Logger

    # GLOBAL_Logger.info("开始提取二厂单井曲线数据！")
    print("......................................................")
    print(getnowtime(), "开始提取二厂单井曲线数据!")

    extractdays = 5
    try:
        for exday in range(1, extractdays):
            # if True:
            #     exday = 4216
            extracterchangwelldynicurvedata(exday)
            # print("前%s日二厂单井曲线数据提取完毕！" % str(exday + 1))
    except Exception as e:
        GLOBAL_Logger.info(traceback.format_exc())
        print(traceback.format_exc())


if __name__ == '__main__':
    print("开始初始化数据")
    initconfig()

    # extract_tanker_shipment_data()
    extract_erchang_welldynicurve_data()
    # extract_level_date()

    # # print("开始迭代更新数据")
    # scheduler = BackgroundScheduler()
    # # 提取日报数据
    # job1 = scheduler.add_job(extract_daily_date, 'interval', hours=6)
    # # 提取液面数据
    # job2 = scheduler.add_job(extract_level_date, 'interval', hours=5)
    # # 提取功图数据
    # job3 = scheduler.add_job(extract_indicator_date, 'interval', hours=7)
    # # 计算动态能力水平数据
    # job4 = scheduler.add_job(extract_dynamic_level_data, 'interval', hours=11)
    # # 提取单井实时掺稀数据
    # job5 = scheduler.add_job(extract_well_realtime_data, 'cron', minute=1)
    # # 提取计转站实时数据
    # job6 = scheduler.add_job(extract_station_realtime_data, 'cron', minute=1)
    # # 整理计转站来油对比数据
    # job7 = scheduler.add_job(sort_station_input_compare_report_data, 'cron', minute=5)
    # # 提取单井参数数据
    # job8 = scheduler.add_job(extract_well_parameter_data, 'cron', hour=20)
    # # 计算单井掺稀基值
    # job9 = scheduler.add_job(sort_well_base_chanxi, 'cron', hour=5)
    # # 计算单井掺稀调整跟踪表
    # job10 = scheduler.add_job(sort_well_daily_chanxi_upadjust_tail, 'cron', hour=0)
    # # 提取注水站实时数据
    # job11 = scheduler.add_job(extract_watter_station_realtime_data, 'cron', minute=2)
    # # 注水井实时数据
    # job12 = scheduler.add_job(extract_watter_well_realtime_data, 'cron', minute=2)
    # # 单井小时参数
    # job13 = scheduler.add_job(extract_well_hour_parameter_data, 'cron', minute=2)
    # # 提取前一日修改管道参数数据
    # job14 = scheduler.add_job(extract_pipeline_daily_data, 'cron', hour=3)
    # # 提取所有管道参数数据
    # job15 = scheduler.add_job(extract_pipeline_monthly_data, 'cron', day=3, hour=4)
    # # 提取局库动态数据
    # job16 = scheduler.add_job(extract_xibei_bureau_database_data, 'cron', hour="2,15,21")
    # # 计算措施增油量数据
    # job17 = scheduler.add_job(extract_measure_oil_increment, 'cron', hour="11,13,14")
    # # 提取三倒数据
    # job18 = scheduler.add_job(extract_tanker_shipment_data, 'cron', minute="30")
    # # 提取机采参数数据
    # job19 = scheduler.add_job(mechanical_extract_monthly_report, 'cron', hour="15")
    # # 提取厂单井曲线数据
    # job20 = scheduler.add_job(extract_erchang_welldynicurve_data, 'cron', minute="5")
    # ## 提取新版局库动态数据
    # ## job21 = scheduler.add_job(extract_new_xibei_bureau_database_data, 'cron', hour="2,15,21")
    #
    # scheduler.start()
    #
    # while True:
    #     time.sleep(0.1)
