# -*- coding: utf-8 -*-
import os
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import codecs
import traceback
import configparser
import inspect
import cx_Oracle
import pymssql
import pandas as pd
import numpy as np
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import create_engine
# from wxpy import *
import logging
# import matplotlib
# import xlsxwriter
from urllib3 import encode_multipart_formdata
import requests
import json

pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 100)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
# pd.set_option('display.max_colwidth', 5000)
pd.set_option('display.width', 1000)


# 日志函数
# 返回日志文件
def setlog():
    logger = logging.getLogger('logger')
    logger.setLevel(logging.INFO)

    this_file = inspect.getfile(inspect.currentframe())
    dirpath = os.path.abspath(os.path.dirname(this_file))

    # create a file handlerINFO
    handler = logging.FileHandler(os.path.join(dirpath, 'EcInfoPushServer.log'))
    handler.setLevel(logging.INFO)

    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)

    return logger


# 获取文件上传token
def get_access_token(corpid, corpsecret):
    url = 'https://oapi.dingtalk.com/gettoken?corpid=%s&corpsecret=%s' % (corpid, corpsecret)
    response = ""
    while response == '':
        try:
            response = requests.get(url)
            response_str = response.text
            response_dict = json.loads(response_str)
        except:
            time.sleep(5)
            continue

    error_code_key = "errcode"
    access_token_key = "access_token"

    if (error_code_key in response_dict) and response_dict[error_code_key] == 0 and (access_token_key in response_dict):
        return response_dict[access_token_key]
    else:
        return ''


# 日期转化函数
# 返回目前日期
def getnowtime():
    NowDate = time.strftime("%Y-%m-%d %H:%M:%S", datetime.fromtimestamp(time.time()).timetuple())
    return NowDate


# 返回间隔前日期
def getstartdate(durtime):
    NowTime = time.time()
    DateBeginDay = datetime.fromtimestamp(NowTime) - relativedelta(days=durtime)
    StrBeginDay = time.strftime("%Y-%m-%d 00:00:00", DateBeginDay.timetuple())
    return StrBeginDay


# 推送消息
def postdata(url, post_data):
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
            print(response)
            print(response_str)
            print(response_dict)
            print(traceback.format_exc())
            GLOBAL_Logger.info(traceback.format_exc())
            time.sleep(5)
            continue


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


# 初始化程序
def initconfig():
    # global GLOBAL_Factors
    # global AbnormalRankServerConfig
    # global GLOBAL_AbnormalRankServer
    # global SensorDateServerConfig
    # global GLOBAL_SensorDateServer
    global GLOBAL_Logger
    # global GlOBAL_engine
    # global GLOBAL_HistoryData
    global GlOBAL_mssql_engine_GBK
    global GlOBAL_mssql_engine_UTF8
    global GLOBLE_headers
    global GLOBLE_corp_id
    global GLOBLE_corp_secret
    global GLOBLE_rob_url

    GLOBLE_headers = {"Content-Type": "application/json; charset=utf-8"}
    GLOBLE_corp_id = "ding4acb78f9e8af3f0635c2f4657eb6378f"
    GLOBLE_corp_secret = "zUm-_VjM_s_O4zNHBqfdrf1SE9ejfQVoXviIFRK9Y02dgG-t1ZI5PGvhPsoLblEe"
    GLOBLE_rob_url = "https://oapi.dingtalk.com/robot/send?access_token=4aa2484ddab078a06fbe514eeb3668a55feeddf2eba9913ae80d7a77962aa72a"

    # 采油厂生产数据关系库
    SqlserverDataServre = {"host": '10.16.192.40',
                           "user": 'zhangdkl',
                           "password": '4687607',
                           "database": 'cyec',
                           "charset": 'utf8',
                           "servertype": 'sqlserver'}

    # 采油厂动态生产数据库连接（GBK）
    GlOBAL_mssql_engine_GBK = create_engine('mssql+pymssql://%s:%s@%s/%s' % (
        SqlserverDataServre["user"], SqlserverDataServre["password"], SqlserverDataServre["host"],
        SqlserverDataServre["database"]), connect_args={'charset': 'GBK'})
    # 采油厂动态生产数据库连接（UTF8）
    GlOBAL_mssql_engine_UTF8 = create_engine('mssql+pymssql://%s:%s@%s/%s' % (
        SqlserverDataServre["user"], SqlserverDataServre["password"], SqlserverDataServre["host"],
        SqlserverDataServre["database"]))

    # # 判断数字类型函数
    # def is_number(s):
    #     try:
    #         float(s)
    #         return True
    #     except ValueError:
    #         pass
    #     return False

    # GLOBAL_HistoryData = pd.DataFrame()

    # # 根据配置文件同步备注数据
    # cf = configparser.ConfigParser()
    # configfilename = "config.ini"

    # if os.path.exists(configfilename):
    #     cf.read_file(codecs.open(configfilename, "r", "utf-8"))
    #     for key, value in GLOBAL_Factors.items():
    #         if is_number(cf.get('factors', key)):
    #             GLOBAL_Factors[key] = float(cf.get('factors', key))
    #     for key, value in AbnormalRankServerConfig.items():
    #         AbnormalRankServerConfig[key] = cf.get('abnormalrankserver', key)
    #     for key, value in SensorDateServerConfig.items():
    #         SensorDateServerConfig[key] = cf.get('sensordateserver', key)
    #     for key, value in GLOBAL_DatabaseTableNames.items():
    #         GLOBAL_DatabaseTableNames[key] = cf.get('databasetablenames', key)
    # else:
    #     cf.add_section("factors")
    #     for key, value in GLOBAL_Factors.items():
    #         cf.set('factors', key, str(value))
    #     cf.add_section("abnormalrankserver")
    #     for key, value in AbnormalRankServerConfig.items():
    #         cf.set('abnormalrankserver', key, str(value))
    #     cf.add_section("sensordateserver")
    #     for key, value in SensorDateServerConfig.items():
    #         cf.set('sensordateserver', key, str(value))
    #     cf.add_section("databasetablenames")
    #     for key, value in GLOBAL_DatabaseTableNames.items():
    #         cf.set('databasetablenames', key, str(value))
    #     cf.write(codecs.open(configfilename, "w", "utf-8"))

    # # 更新远传数据库连接信息
    # if SensorDateServerConfig["servertype"] == "oracle":
    #     GLOBAL_SensorDateServer = {"user": SensorDateServerConfig["user"],
    #                                "password": SensorDateServerConfig["password"],
    #                                "dsn_tns": cx_Oracle.makedsn(SensorDateServerConfig["host"],
    #                                                             SensorDateServerConfig["port"],
    #                                                             SensorDateServerConfig["sid"]),
    #                                "servertype": 'oracle'}
    # else:
    #     GLOBAL_SensorDateServer = {"host": SensorDateServerConfig["host"],
    #                                "user": SensorDateServerConfig["user"],
    #                                "password": SensorDateServerConfig["password"],
    #                                "database": SensorDateServerConfig["database"],
    #                                "charset": SensorDateServerConfig["charset"],
    #                                "servertype": 'sqlserver'}
    #
    # # 更新异常评级数据库连接信息
    # if AbnormalRankServerConfig["servertype"] == "oracle":
    #     GLOBAL_AbnormalRankServer = {"user": AbnormalRankServerConfig["user"],
    #                                  "password": AbnormalRankServerConfig["password"],
    #                                  "dsn_tns": cx_Oracle.makedsn(AbnormalRankServerConfig["host"],
    #                                                               AbnormalRankServerConfig["port"],
    #                                                               AbnormalRankServerConfig["sid"]),
    #                                  "servertype": 'oracle'}
    # else:
    #     GLOBAL_AbnormalRankServer = {"host": AbnormalRankServerConfig["host"],
    #                                  "user": AbnormalRankServerConfig["user"],
    #                                  "password": AbnormalRankServerConfig["password"],
    #                                  "database": AbnormalRankServerConfig["database"],
    #                                  "charset": AbnormalRankServerConfig["charset"],
    #                                  "servertype": 'sqlserver'}

    GLOBAL_Logger = setlog()

    # GlOBAL_engine = create_engine('mssql+pymssql://%s:%s@%s/%s' % (
    #     GLOBAL_AbnormalRankServer["user"], GLOBAL_AbnormalRankServer["password"], GLOBAL_AbnormalRankServer["host"],
    #     GLOBAL_AbnormalRankServer["database"]))


# 通报计转站外输数据线情况
def pushStationExtransmissionDropInfo(fri="hour"):
    endtime = getbegintime(0 * 60, "minutes")
    # print(endtime)

    tablename = 'EcStationExtransmissionAutoHour'
    fields = ["time", "devicetype", "devicestatus", "sumdata", "diffhourdata", "hourdata", "station", "pressure", "temperature",
              "stacheck", "evrhourdata"]
    fieldstr = fields[0]
    for field in fields[1:]:
        fieldstr = fieldstr + ", " + field
    getstr = "SELECT %s  FROM %s WHERE time = '%s'" % (fieldstr, tablename, endtime)
    meter_drop_ora_data = pd.read_sql(getstr, GlOBAL_mssql_engine_GBK)

    if len(meter_drop_ora_data) > 0:
        normaldata = meter_drop_ora_data[meter_drop_ora_data["devicestatus"] == "正常"]
        nodata = meter_drop_ora_data[meter_drop_ora_data["devicestatus"] == "无数据"]
        nodatawellnames = list(set(list(nodata["station"].values)))
        abnormaldata = meter_drop_ora_data[meter_drop_ora_data["devicestatus"] == "底数异常"]
        abnormaldatawellnames = list(set(list(abnormaldata["station"].values)))

        totalnum = len(meter_drop_ora_data)
        nornum = len(normaldata)
        nodatanum = len(nodata)
        abnornum = len(abnormaldata)

        if fri == "hour":
            if abnornum > 0:
                namestr = ""
                dropmsg = "数据异常共%s处：" % abnornum
                abdata = abnormaldata.set_index("station")["stacheck"]
                for wellname in abnormaldatawellnames:
                    abrio = abdata.loc[wellname].round(1)
                    if len(namestr) > 0:
                        namestr = namestr + chr(10) + wellname + ",数据异常率%s%%" % abrio
                    else:
                        namestr = wellname + ",数据异常率%s%%" % abrio
                dropmsg = dropmsg + chr(10) + namestr + "!" + chr(10) + "请检查异常原因，立即整改恢复！"

                msgdate = datetime.fromtimestamp(time.time()).timetuple()
                strmsgdate = "%s月%s日%s时" % (msgdate.tm_mon, msgdate.tm_mday, msgdate.tm_hour)
                msg = "计转站原油外输远传掉线情况小时通报：" + chr(10) + strmsgdate + dropmsg

                # print(msg)
                post_data = {
                    "msgtype": "text",
                    "text": {"content": msg},
                    # "at": {"isAtAll": True}
                }
                postdata(GLOBLE_rob_url, post_data)
        elif fri == "day":
            if abnornum + nodatanum > 0:
                dropmsg = "数据回传%s处，其中正常%s处！" % (totalnum, nornum)
                if len(nodatawellnames) > 0:
                    namestr = ""
                    for wellname in nodatawellnames:
                        if len(namestr) > 0:
                            namestr = namestr + "," + wellname
                        else:
                            namestr = wellname
                    dropmsg = dropmsg + chr(10) + "无数据%s处：" % nodatanum + namestr + "!请核实是否掉线！"
                if len(abnormaldatawellnames) > 0:
                    namestr = ""
                    for wellname in abnormaldatawellnames:
                        if len(namestr) > 0:
                            namestr = namestr + "," + wellname
                        else:
                            namestr = wellname
                    dropmsg = dropmsg + chr(10) + "数据异常%s处：" % abnornum + namestr + "! 请检查异常原因，立即整改恢复！"

                msgdate = datetime.fromtimestamp(time.time()).timetuple()
                strmsgdate = "%s月%s日%s时" % (msgdate.tm_mon, msgdate.tm_mday, msgdate.tm_hour)
                msg = "计转站原油外输远传掉线情况日度通报：" + chr(10) + strmsgdate + "，本日" + dropmsg

                # print(msg)
                post_data = {
                    "msgtype": "text",
                    "text": {"content": msg},
                    "at": {"isAtAll": True}
                }
                postdata(GLOBLE_rob_url, post_data)


# 通报计转站来油掉线情况数据
def pushStationChanxiInputDropInfo(fri="hour"):
    endtime = getbegintime(0 * 60, "minutes")
    # print(endtime)

    tablename = 'EcStationChanxiInputAutoHour'
    fields = ["time", "devicetype", "devicestatus", "sumchanxi", "diffhourchanxi", "hourchanxi", "station", "pressure", "temperature",
              "stacheck", "avrhourchanxi"]
    fieldstr = fields[0]
    for field in fields[1:]:
        fieldstr = fieldstr + ", " + field
    getstr = "SELECT %s  FROM %s WHERE time = '%s'" % (fieldstr, tablename, endtime)
    meter_drop_ora_data = pd.read_sql(getstr, GlOBAL_mssql_engine_GBK)

    if len(meter_drop_ora_data) > 0:
        normaldata = meter_drop_ora_data[meter_drop_ora_data["devicestatus"] == "正常"]
        nodata = meter_drop_ora_data[meter_drop_ora_data["devicestatus"] == "无数据"]
        nodatawellnames = list(set(list(nodata["station"].values)))
        abnormaldata = meter_drop_ora_data[meter_drop_ora_data["devicestatus"] == "底数异常"]
        abnormaldatawellnames = list(set(list(abnormaldata["station"].values)))

        totalnum = len(meter_drop_ora_data)
        nornum = len(normaldata)
        nodatanum = len(nodata)
        abnornum = len(abnormaldata)

        if fri == "hour":
            if abnornum > 0:
                namestr = ""
                dropmsg = "数据异常共%s处：" % abnornum
                abdata = abnormaldata.set_index("station")["stacheck"]
                for wellname in abnormaldatawellnames:
                    abrio = abdata.loc[wellname].round(1)
                    if len(namestr) > 0:
                        namestr = namestr + chr(10) + wellname + ",数据异常率%s%%" % abrio
                    else:
                        namestr = wellname + ",数据异常率%s%%" % abrio
                dropmsg = dropmsg + chr(10) + namestr + "!" + chr(10) + "请检查异常原因，立即整改恢复！"

                msgdate = datetime.fromtimestamp(time.time()).timetuple()
                strmsgdate = "%s月%s日%s时" % (msgdate.tm_mon, msgdate.tm_mday, msgdate.tm_hour)
                msg = "计转站掺稀来油远传掉线情况小时通报：" + chr(10) + strmsgdate + dropmsg

                # print(msg)
                post_data = {
                    "msgtype": "text",
                    "text": {"content": msg},
                    # "at": {"isAtAll": True}
                }
                postdata(GLOBLE_rob_url, post_data)
        elif fri == "day":
            if abnornum + nodatanum > 0:
                dropmsg = "数据回传%s处，其中正常%s处！" % (totalnum, nornum)
                if len(nodatawellnames) > 0:
                    namestr = ""
                    for wellname in nodatawellnames:
                        if len(namestr) > 0:
                            namestr = namestr + "," + wellname
                        else:
                            namestr = wellname
                    dropmsg = dropmsg + chr(10) + "无数据%s处：" % nodatanum + namestr + "!请核实是否掉线！"
                if len(abnormaldatawellnames) > 0:
                    namestr = ""
                    for wellname in abnormaldatawellnames:
                        if len(namestr) > 0:
                            namestr = namestr + "," + wellname
                        else:
                            namestr = wellname
                    dropmsg = dropmsg + chr(10) + "数据异常%s处：" % abnornum + namestr + "! 请检查异常原因，立即整改恢复！"

                msgdate = datetime.fromtimestamp(time.time()).timetuple()
                strmsgdate = "%s月%s日%s时" % (msgdate.tm_mon, msgdate.tm_mday, msgdate.tm_hour)
                msg = "计转站掺稀来油远传掉线情况日度通报：" + chr(10) + strmsgdate + "，本日" + dropmsg

                # print(msg)
                post_data = {
                    "msgtype": "text",
                    "text": {"content": msg},
                    # "at": {"isAtAll": True}
                }
                postdata(GLOBLE_rob_url, post_data)


# 通报单井掺稀掉线情况数据
def pushWellChanximissionDropInfo():
    # endtime = getstartdate(0)
    endtime = getbegintime(0 * 60, "minutes")
    # print(endtime)

    tablename = 'EcWellChanxiRealtimeAutoHour'
    fields = ["wellname", "compname", "devicelocation", "devicestatus", "station"]
    fieldstr = fields[0]
    for field in fields[1:]:
        fieldstr = fieldstr + ", " + field
    getstr = "SELECT %s  FROM %s WHERE time = '%s'" % (fieldstr, tablename, endtime)
    meter_drop_ora_data = pd.read_sql(getstr, GlOBAL_mssql_engine_GBK)

    meter_drop_ora_data = meter_drop_ora_data[~(meter_drop_ora_data["devicestatus"] == "未开井")]
    meter_drop_ora_data = meter_drop_ora_data[~(meter_drop_ora_data["devicelocation"] == "不掺稀")]
    allnum = len(meter_drop_ora_data)

    in_meter_drop_ora_data = meter_drop_ora_data[meter_drop_ora_data["devicelocation"] == "站内"]
    inallnum = len(in_meter_drop_ora_data)
    out_meter_drop_ora_data = meter_drop_ora_data[meter_drop_ora_data["devicelocation"] == "站外"]
    outallnum = len(out_meter_drop_ora_data)

    meter_drop_ora_data = meter_drop_ora_data[~(meter_drop_ora_data["devicestatus"] == "正常")]
    dropnum = len(meter_drop_ora_data)
    nornum = allnum - dropnum

    if allnum > 0:
        droprio = round(dropnum / allnum * 100, 2)

        inwelldropdata = meter_drop_ora_data[meter_drop_ora_data["devicelocation"] == "站内"]
        innum = len(inwelldropdata)
        indroprio = round(innum / inallnum * 100, 2)

        outwelldropdata = meter_drop_ora_data[meter_drop_ora_data["devicelocation"] == "站外"]
        outnum = len(outwelldropdata)
        outdroprio = round(outnum / outallnum * 100, 2)

        instadata = inwelldropdata.groupby(["devicestatus"]).count().sort_values(by=["wellname"], ascending=False)
        instadata = instadata["wellname"]

        innostadate = inwelldropdata.groupby(["station"]).count().sort_values(by=["wellname"], ascending=False)
        dropmean = innostadate["wellname"].median()
        innostadate = innostadate[innostadate["wellname"] >= dropmean][0:5]
        innostadate = innostadate["wellname"]

        outstadata = outwelldropdata.groupby(["devicestatus"]).count().sort_values(by=["wellname"], ascending=False)
        outstadata = outstadata["wellname"]

        outnostadate = outwelldropdata.groupby(["station"]).count().sort_values(by=["wellname"], ascending=False)
        dropmean = outnostadate["wellname"].median()
        outnostadate = outnostadate[outnostadate["wellname"] >= dropmean][0:100]
        outnostadate = outnostadate["wellname"]

        if dropnum > 0:
            dropmsg = "单井掺稀掉线%s处，掉线率%s！" % (dropnum, droprio)

            if innum > 0:
                dropmsg = dropmsg + chr(10) + "站内方面，掉线%s处，掉线率%s。" % (innum, indroprio)

                typestr = ""
                for reason in instadata.index:
                    if len(typestr) == 0:
                        typestr = typestr + reason + str(instadata.loc[reason]) + "处"
                    else:
                        typestr = typestr + "," + reason + str(instadata.loc[reason]) + "处"

                stationstr = ""
                for reason in innostadate.index:
                    if len(stationstr) == 0:
                        stationstr = stationstr + reason + "掉线" + str(innostadate.loc[reason]) + "处"
                    else:
                        stationstr = stationstr + "," + reason + "掉线" + str(innostadate.loc[reason]) + "处"
                dropmsg = dropmsg + chr(10) + "掉线分类：" + typestr + "。" + chr(10) + "以下站库掉线较多：" + stationstr + "!务必尽快处置！"

            if outnum > 0:
                dropmsg = dropmsg + chr(10) + "站外方面，掉线%s处，掉线率%s。" % (outnum, outdroprio)

                typestr = ""
                for reason in outstadata.index:
                    if len(typestr) == 0:
                        typestr = typestr + reason + str(outstadata.loc[reason]) + "处"
                    else:
                        typestr = typestr + "," + reason + str(outstadata.loc[reason]) + "处"

                stationstr = ""
                for reason in outnostadate.index:
                    if len(stationstr) == 0:
                        stationstr = stationstr + reason + "掉线" + str(outnostadate.loc[reason]) + "处"
                    else:
                        stationstr = stationstr + "," + reason + "掉线" + str(outnostadate.loc[reason]) + "处"
                dropmsg = dropmsg + chr(10) + "掉线分类：" + typestr + "。" + chr(10) + "以下站库掉线较多：" + stationstr + "!"

            msgdate = datetime.fromtimestamp(time.time()).timetuple()
            strmsgdate = "%s月%s日%s时" % (msgdate.tm_mon, msgdate.tm_mday, msgdate.tm_hour)
            msg = "单井掺稀远传掉线情况日度通报：" + chr(10) + strmsgdate + "，本日" + dropmsg

            # print(msg)
            post_data = {
                "msgtype": "text",
                "text": {"content": msg},
                # "at": {"isAtAll": True}
            }
            postdata(GLOBLE_rob_url, post_data)


def startservice():
    # GLOBLE_Scheduler_Runstatus = False
    initconfig()

    # pushStationExtransmissionDropInfo("hour")
    # pushStationChanxiInputDropInfo("day")
    # pushWellChanximissionDropInfo()

    # sched.add_job(job1, 'interval', seconds=1, args=["a", "b", "c"])

    print("start abnormal info pushing service....")
    # GLOBAL_Logger.info("start abnormal info pushing service....")
    #
    # # pushchanxitailinfo()
    #
    scheduler = BackgroundScheduler()
    scheduler.add_job(pushStationExtransmissionDropInfo, 'cron', hour='10', minute=15, args=["day"])
    scheduler.add_job(pushStationChanxiInputDropInfo, 'cron', hour='10', minute=15, args=["day"])
    scheduler.add_job(pushWellChanximissionDropInfo, 'cron', hour='10', minute=15)

    scheduler.start()
    while True:
        time.sleep(0.1)

    # def startscheduler():
    #     global GLOBLE_Scheduler_Runstatus
    #     if GLOBLE_Scheduler_Runstatus:
    #         scheduler.shutdown()
    #         GLOBLE_Scheduler_Runstatus = False
    #     scheduler.start()
    #     GLOBLE_Scheduler_Runstatus = True
    #
    # def checkexception():
    #     try:
    #         startscheduler()
    #     except Exception as e:
    #         print(traceback.format_exc())
    #         GLOBAL_Logger.info(traceback.format_exc())
    #         return False
    #     else:
    #         return True
    #
    # while not checkexception():
    #     pass

    # while True:
    #     time.sleep(0.1)


if __name__ == '__main__':
    startservice()
