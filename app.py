from flask import (
    Flask,
    g,
    redirect,
    render_template,
    flash,
    request,
    session,
    url_for
)
# from bson.regex import Regex
import pandas as pd
from flask import Flask,json
from datetime import datetime
# from pandas.io.json import json_normalize
import pandas as pd
# import pycountry
# import mysql.connector
import numpy as np
from flask_cors import CORS
# from geolite2 import geolite2
import time
from datetime import timedelta
import pymongo
from pymongo import MongoClient
# from pprint import pprint
import urllib.parse
from pandas import DataFrame
from bson.objectid import ObjectId
import datetime
from flask import Flask, make_response
import pyexcel as pe
import io 
import dateutil.parser
from datetime import date
# import calendar
import re
import json
# from urllib.request import urlopen
# from sort_dataframeby_monthorweek import *
# from pytz import timezone
# from six.moves import urllib
# from numpyencoder import NumpyEncoder
# from flask_csv import send_csv


app = Flask(__name__)
CORS(app)
from flask import Response

@app.route('/bubble_dataframe.csv')
def buble_district12():
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    dateStr = "2020-01-01T00:00:00.000Z"
    myDatetime = dateutil.parser.parse(dateStr)
    collection = db.user_master
    qraaa=[
        {"$match":{'$and':[
        {'USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
        {'EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
        {'EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
        {'USER_NAME':{'$not':{'$regex':'1gen', '$options':'i'}}},
        {'DISTRICT_ID.DISTRICT_NAME':{'$not':{'$regex':'test', '$options':'i'}}},
        {"schoolId._id":{"$in":db.school_master.distinct( "_id", { "IS_PORTAL": "Y" } )}},
        {'DISTRICT_ID':{'$exists':1}},
        {'INCOMPLETE_SIGNUP':{"$ne":'Y'}}, 
        {'CREATED_DATE':{"$gte":myDatetime}},
        {'ROLE_ID._id':{'$ne':ObjectId("5f155b8a3b6800007900da2b")}},
        {'IS_BLOCKED':{"$ne":'Y'}}, 
        {'IS_DISABLED':{"$ne":'Y'}}, {'schoolId.NAME':{'$not':{'$regex':'test', '$options':'i'}}},
        ]}},
        {'$group':{'_id':{"district":'$DISTRICT_ID._id',"month":{"$month": "$CREATED_DATE"}},
        'NAME_DISTRICT':{'$first':'$DISTRICT_ID.DISTRICT_NAME'},
        'usercount':{'$sum':1}}},
            {"$project":{"_id":0,"DISTRICT_ID":"$_id.district","MONTH":"$_id.month","NAME_DISTRICT":1,"usercount":1,
                        }}]
    merge11=list(collection.aggregate(qraaa))
    df1=pd.DataFrame(merge11)
    #######################################################
    db=client.compass
    dateStr = "2020-01-01T00:00:00.000Z"
    myDatetime = dateutil.parser.parse(dateStr)
    collection = db.user_master
    qra=[
        {"$match":{'$and':[
        {'USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
        {'EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
        {'EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
        {'USER_NAME':{'$not':{'$regex':'1gen', '$options':'i'}}},
        {'DISTRICT_ID.DISTRICT_NAME':{'$not':{'$regex':'test', '$options':'i'}}},
        {"schoolId._id":{"$in":db.school_master.distinct( "_id", { "IS_PORTAL": "Y" } )}},
        {'DISTRICT_ID':{'$exists':1}},
        {'INCOMPLETE_SIGNUP':{"$ne":'Y'}}, 
        {'CREATED_DATE':{"$lt":myDatetime}},
        {'ROLE_ID._id':{'$ne':ObjectId("5f155b8a3b6800007900da2b")}},
        {'IS_BLOCKED':{"$ne":'Y'}}, 
        {'IS_DISABLED':{"$ne":'Y'}}, {'schoolId.NAME':{'$not':{'$regex':'test', '$options':'i'}}},
        ]}},
        {'$group':{'_id':{"district":'$DISTRICT_ID._id'},
        'NAME_DISTRICT':{'$first':'$DISTRICT_ID.DISTRICT_NAME'},
        'usercount':{'$sum':1}}},
            {"$project":{"_id":0,"DISTRICT_ID":"$_id.district","NAME_DISTRICT":1,"usercount":1,
                        }}]
    merge11233=list(collection.aggregate(qra))
    dfCV=pd.DataFrame(merge11233)
    #########################################################################
    df1=df1.sort_values(by=['NAME_DISTRICT'], ascending=True)
    x=df1[df1['NAME_DISTRICT']=="Belleville School District"]
    x["usercount"].sum()
    dislist=list(set(df1["NAME_DISTRICT"]))
    df2=df1[["NAME_DISTRICT","MONTH","usercount"]]
    overall=pd.DataFrame(columns=["NAME_DISTRICT","MONTH","usercount"])
    # dislist
    result=[]
    for k in dislist:
    #     print(k)
        df45=df2[df2["NAME_DISTRICT"]==k]
        df45.reset_index()
        for i in range(1,13):
            if i in list(df45["MONTH"]):
                pass
    #              print("month_present",i)
            else:
                df45.loc[i+len(df45)] = [k] +[i]+[0]
    #     df46=df45.groupby(['NAME_DISTRICT', 'MONTH']).reset_index()
        df46=df45.groupby(['NAME_DISTRICT', 'MONTH']).sum().groupby(level=0).cumsum().reset_index()
    #     print(df46)
        sorted_df =df46.sort_values(by=['MONTH'], ascending=True)
        sorted_df1=sorted_df.reset_index()
        result.append(sorted_df)
    buubleusercount = pd.concat(result)
    ######family ########
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    dateStr = "2020-01-01T00:00:00.000Z"
    myDatetime = dateutil.parser.parse(dateStr)
    collection = db.user_master
    qraaa=[
        {"$match":{'$and':[
        {'USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
        {'EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
        {'EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
        {'USER_NAME':{'$not':{'$regex':'1gen', '$options':'i'}}},
        {'DISTRICT_ID.DISTRICT_NAME':{'$not':{'$regex':'test', '$options':'i'}}},
        {"schoolId._id":{"$in":db.school_master.distinct( "_id", { "IS_PORTAL": "Y" } )}},
        {'DISTRICT_ID':{'$exists':1}},
        {'INCOMPLETE_SIGNUP':{"$ne":'Y'}}, 
        {'CREATED_DATE':{"$gte":myDatetime}},
        {'ROLE_ID._id':{'$eq':ObjectId("5f155b8a3b6800007900da2b")}},
        {'IS_BLOCKED':{"$ne":'Y'}}, 
        {'IS_DISABLED':{"$ne":'Y'}}, {'schoolId.NAME':{'$not':{'$regex':'test', '$options':'i'}}},
        ]}},
        {'$group':{'_id':{"district":'$DISTRICT_ID._id',"month":{"$month": "$CREATED_DATE"}},
        'NAME_DISTRICT':{'$first':'$DISTRICT_ID.DISTRICT_NAME'},
        'famcount':{'$sum':1}}},
            {"$project":{"_id":0,"DISTRICT_ID":"$_id.district","MONTH":"$_id.month","NAME_DISTRICT":1,"famcount":1,
                        }}]
    merge11=list(collection.aggregate(qraaa))
    df1=pd.DataFrame(merge11)
    #######################################################
    db=client.compass
    dateStr = "2020-01-01T00:00:00.000Z"
    myDatetime = dateutil.parser.parse(dateStr)
    collection = db.user_master
    qra=[
        {"$match":{'$and':[
        {'USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
        {'EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
        {'EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
        {'USER_NAME':{'$not':{'$regex':'1gen', '$options':'i'}}},
        {'DISTRICT_ID.DISTRICT_NAME':{'$not':{'$regex':'test', '$options':'i'}}},
        {"schoolId._id":{"$in":db.school_master.distinct( "_id", { "IS_PORTAL": "Y" } )}},
        {'DISTRICT_ID':{'$exists':1}},
        {'INCOMPLETE_SIGNUP':{"$ne":'Y'}}, 
        {'CREATED_DATE':{"$lt":myDatetime}},
        {'ROLE_ID._id':{'$ne':ObjectId("5f155b8a3b6800007900da2b")}},
        {'IS_BLOCKED':{"$ne":'Y'}}, 
        {'IS_DISABLED':{"$ne":'Y'}}, {'schoolId.NAME':{'$not':{'$regex':'test', '$options':'i'}}},
        ]}},
        {'$group':{'_id':{"district":'$DISTRICT_ID._id'},
        'NAME_DISTRICT':{'$first':'$DISTRICT_ID.DISTRICT_NAME'},
        'usercount19':{'$sum':1}}},
            {"$project":{"_id":0,"DISTRICT_ID":"$_id.district","NAME_DISTRICT":1,"usercount19":1,
                        }}]
    merge11233=list(collection.aggregate(qra))
    dfCV=pd.DataFrame(merge11233)
    #########################################################################
    df1=df1.sort_values(by=['NAME_DISTRICT'], ascending=True)
    x=df1[df1['NAME_DISTRICT']=="Belleville School District"]
    x["famcount"].sum()
    dislist=list(set(df1["NAME_DISTRICT"]))
    df2=df1[["NAME_DISTRICT","MONTH","famcount"]]
    overall=pd.DataFrame(columns=["NAME_DISTRICT","MONTH","famcount"])
    # dislist
    result=[]
    for k in dislist:
    #     print(k)
        df45=df2[df2["NAME_DISTRICT"]==k]
        df45.reset_index()
        for i in range(1,13):
            if i in list(df45["MONTH"]):
                pass
    #              print("month_present",i)
            else:
                df45.loc[i+len(df45)] = [k] +[i]+[0]
    #     df46=df45.groupby(['NAME_DISTRICT', 'MONTH']).reset_index()
        df46=df45.groupby(['NAME_DISTRICT', 'MONTH']).sum().groupby(level=0).cumsum().reset_index()
    #     print(df46)
        sorted_df =df46.sort_values(by=['MONTH'], ascending=True)
        sorted_df1=sorted_df.reset_index()
        result.append(sorted_df)
    buublefamily = pd.concat(result)
    buubleusercount["idu"]=buubleusercount["NAME_DISTRICT"]+buubleusercount["MONTH"].map(str)
    buublefamily["idf"]=buublefamily["NAME_DISTRICT"]+buublefamily["MONTH"].map(str)
    mergeucfc=pd.merge(buubleusercount, buublefamily, how='left', left_on='idu', right_on='idf')
    mergeucfc=mergeucfc.fillna(0)
    mergeucfc1=pd.merge(mergeucfc, dfCV, how='left', left_on='NAME_DISTRICT_x', right_on='NAME_DISTRICT')
    mergeucfc12=mergeucfc1.fillna(0)
    mergeucfc12["totaluser"]=mergeucfc12["usercount"]+mergeucfc12["usercount19"]
    finmerge=mergeucfc12[["NAME_DISTRICT_x","MONTH_x","idu","totaluser","famcount"]]
    ###ACTIVE USER
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    dateStr = "2020-01-01T00:00:00.000Z"
    myDatetime = dateutil.parser.parse(dateStr)
    collection = db.audio_track_master
    qra12=[
        {"$match":{'$and':[{'USER_ID.USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
        {'USER_ID.EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
        {'USER_ID.EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
        {'USER_ID.USER_NAME':{'$not':{'$regex':'1gen', '$options':'i'}}},
        {'USER_ID.DISTRICT_ID.DISTRICT_NAME':{'$not':{'$regex':'test', '$options':'i'}}},
        {"USER_ID.schoolId._id":{"$in":db.school_master.distinct( "_id", { "IS_PORTAL": "Y" } )}},
        {'USER_ID.DISTRICT_ID':{'$exists':1}},
        {'USER_ID.INCOMPLETE_SIGNUP':{"$ne":'Y'}}, 
        {'MODIFIED_DATE':{"$gte":myDatetime}},
        {'USER_ID.ROLE_ID._id':{'$ne':ObjectId("5f155b8a3b6800007900da2b")}},
        {'USER_ID.IS_BLOCKED':{"$ne":'Y'}}, 
        {'USER_ID.IS_DISABLED':{"$ne":'Y'}}, {'USER_ID.schoolId.NAME':{'$not':{'$regex':'test', '$options':'i'}}},
        ]}},
        {'$group':{'_id':{"district":'$USER_ID.DISTRICT_ID._id',"year":{"$year": "$MODIFIED_DATE"},"month":{"$month": "$MODIFIED_DATE"}},
        'NAME_DISTRICT':{'$first':'$USER_ID.DISTRICT_ID.DISTRICT_NAME'},
        'PRACTICE':{'$sum':1},
        "ACTIVE_USER":{'$addToSet':"$USER_ID._id"},
        'Mindful_Minutes':{"$sum":{"$round":[{"$divide":[{"$subtract":['$CURSOR_END','$cursorStart']}, 60]},2]}}}},
            {"$project":{"_id":0,"DISTRICT_ID":"$_id.district","MONTH":"$_id.month","YEAR":"$_id.year","NAME_DISTRICT":1,"PRACTICE":1,"ACTIVE_USER":{"$size":"$ACTIVE_USER"},
                       "Mindful_Minutes":1 }}]
    merge121=list(collection.aggregate(qra12))
    df1=pd.DataFrame(merge121)
    df1=df1.sort_values(by=['NAME_DISTRICT'], ascending=True)
    # print(df1)
    # x=df1[df1['NAME_DISTRICT']=="Belleville School District"]
    dislist=list(set(df1["NAME_DISTRICT"]))
    df2=df1[["NAME_DISTRICT","MONTH","ACTIVE_USER"]]
    overall=pd.DataFrame(columns=["NAME_DISTRICT","MONTH","ACTIVE_USER"])
    result=[]
    for k in dislist:
    #     print(k)
        df45=df2[df2["NAME_DISTRICT"]==k]
        df45.reset_index()
        for i in range(1,13):
            if i in list(df45["MONTH"]):
                pass
    #              print("month_present",i)
            else:
                df45.loc[i+len(df45)] = [k] +[i]+[0]
    #     df46=df45.groupby(['NAME_DISTRICT', 'MONTH']).reset_index()
    #     df46=df45.groupby(['NAME_DISTRICT', 'MONTH']).sum().groupby(level=0).cumsum().reset_index()
    #     print(df46)
        sorted_df =df45.sort_values(by=['MONTH'], ascending=True)
        sorted_df1=sorted_df.reset_index()
        result.append(sorted_df)
    buubleactuser = pd.concat(result)
    buubleactuser["acuid"]=buubleactuser["NAME_DISTRICT"]+buubleactuser["MONTH"].map(str)
    # buubleactuser
    finmergeu=pd.merge(finmerge, buubleactuser, how='left', left_on='idu', right_on='acuid')
    ###ACTIVE FAMILY
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    dateStr = "2020-01-01T00:00:00.000Z"
    myDatetime = dateutil.parser.parse(dateStr)
    collection = db.audio_track_master
    qra12=[
        {"$match":{'$and':[{'USER_ID.USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
        {'USER_ID.EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
        {'USER_ID.EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
        {'USER_ID.USER_NAME':{'$not':{'$regex':'1gen', '$options':'i'}}},
        {'USER_ID.DISTRICT_ID.DISTRICT_NAME':{'$not':{'$regex':'test', '$options':'i'}}},
        {"USER_ID.schoolId._id":{"$in":db.school_master.distinct( "_id", { "IS_PORTAL": "Y" } )}},
        {'USER_ID.DISTRICT_ID':{'$exists':1}},
        {'USER_ID.INCOMPLETE_SIGNUP':{"$ne":'Y'}}, 
        {'MODIFIED_DATE':{"$gte":myDatetime}},
        {'USER_ID.ROLE_ID._id':ObjectId("5f155b8a3b6800007900da2b")},
        {'USER_ID.IS_BLOCKED':{"$ne":'Y'}}, 
        {'USER_ID.IS_DISABLED':{"$ne":'Y'}}, {'USER_ID.schoolId.NAME':{'$not':{'$regex':'test', '$options':'i'}}},
        ]}},
        {'$group':{'_id':{"district":'$USER_ID.DISTRICT_ID._id',"year":{"$year": "$MODIFIED_DATE"},"month":{"$month": "$MODIFIED_DATE"}},
        'NAME_DISTRICT':{'$first':'$USER_ID.DISTRICT_ID.DISTRICT_NAME'},
        'PRACTICE':{'$sum':1},
        "ACTIVE_FAM":{'$addToSet':"$USER_ID._id"},
        'Mindful_Minutes':{"$sum":{"$round":[{"$divide":[{"$subtract":['$CURSOR_END','$cursorStart']}, 60]},2]}}}},
            {"$project":{"_id":0,"DISTRICT_ID":"$_id.district","MONTH":"$_id.month","YEAR":"$_id.year","NAME_DISTRICT":1,"PRACTICE":1,"ACTIVE_FAM":{"$size":"$ACTIVE_FAM"},
                       "Mindful_Minutes":1 }}]
    merge121=list(collection.aggregate(qra12))
    df1=pd.DataFrame(merge121)
    df1=df1.sort_values(by=['NAME_DISTRICT'], ascending=True)
    # x=df1[df1['NAME_DISTRICT']=="Belleville School District"]
    dislist=list(set(df1["NAME_DISTRICT"]))
    df2=df1[["NAME_DISTRICT","MONTH","ACTIVE_FAM"]]
    overall=pd.DataFrame(columns=["NAME_DISTRICT","MONTH","ACTIVE_FAM"])
    result=[]
    for k in dislist:
    #     print(k)
        df45=df2[df2["NAME_DISTRICT"]==k]
        df45.reset_index()
        for i in range(1,13):
            if i in list(df45["MONTH"]):
                pass
    #              print("month_present",i)
            else:
                df45.loc[i+len(df45)] = [k] +[i]+[0]
    #     df46=df45.groupby(['NAME_DISTRICT', 'MONTH']).reset_index()
    #     df46=df45.groupby(['NAME_DISTRICT', 'MONTH']).sum().groupby(level=0).cumsum().reset_index()
    #     print(df46)
        sorted_df =df45.sort_values(by=['MONTH'], ascending=True)
        sorted_df1=sorted_df.reset_index()
        result.append(sorted_df)
    buubleactfam = pd.concat(result)
    buubleactfam["acuidf"]=buubleactfam["NAME_DISTRICT"]+buubleactfam["MONTH"].map(str)
    finmergeuf=pd.merge(finmergeu, buubleactfam, how='left', left_on='idu', right_on='acuidf')
    finmergeuf["USER ENGAGEMENT"]=round((finmergeuf["ACTIVE_USER"]/finmergeuf["totaluser"])*100)
    finmergeuf["FAMILY ENGAGEMENT"]=round((finmergeuf["ACTIVE_FAM"]/finmergeuf["famcount"])*100)
    finmergeufo=finmergeuf[["NAME_DISTRICT_x","MONTH_x","USER ENGAGEMENT","FAMILY ENGAGEMENT"]]
    finmergeufo=finmergeufo.fillna(0)
    finmergeufo=finmergeufo.loc[:,~finmergeufo.columns.duplicated()]
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    # dateStr = "2020-01-01T00:00:00.000Z"
    myDatetime = dateutil.parser.parse(dateStr)
    collection = db.audio_track_master
    qra12=[
        {"$match":{'$and':[{'USER_ID.USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
        {'USER_ID.EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
        {'USER_ID.EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
        {'USER_ID.USER_NAME':{'$not':{'$regex':'1gen', '$options':'i'}}},
        {'USER_ID.DISTRICT_ID.DISTRICT_NAME':{'$not':{'$regex':'test', '$options':'i'}}},
        {"USER_ID.schoolId._id":{"$in":db.school_master.distinct( "_id", { "IS_PORTAL": "Y" } )}},
        {'USER_ID.DISTRICT_ID':{'$exists':1}},
        {'USER_ID.INCOMPLETE_SIGNUP':{"$ne":'Y'}}, 
    #     {'MODIFIED_DATE':{"$gte":myDatetime}},
    #     {'USER_ID.ROLE_ID._id':ObjectId("5f155b8a3b6800007900da2b")},
        {'USER_ID.IS_BLOCKED':{"$ne":'Y'}}, 
        {'USER_ID.IS_DISABLED':{"$ne":'Y'}}, {'USER_ID.schoolId.NAME':{'$not':{'$regex':'test', '$options':'i'}}},
        ]}},
        {'$group':{'_id':{"district":'$USER_ID.DISTRICT_ID._id'},
        'NAME_DISTRICT':{'$first':'$USER_ID.DISTRICT_ID.DISTRICT_NAME'},
        'PRACTICE':{'$sum':1},
                  }},
            {"$project":{"_id":0,"DISTRICT_ID":"$_id.district","NAME_DISTRICT":1,"PRACTICE":1 }}]
    merge1211=list(collection.aggregate(qra12))
    df1111=pd.DataFrame(merge1211)
    df1111=df1111.sort_values(by=['NAME_DISTRICT'], ascending=True)
    DISPRACTO=df1111[["NAME_DISTRICT","PRACTICE"]]
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    # dateStr = "2020-01-01T00:00:00.000Z"
    myDatetime = dateutil.parser.parse(dateStr)
    collection = db.user_master
    qra12=[
        {"$match":{'$and':[{'USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
        {'EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
        {'EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
        {'USER_NAME':{'$not':{'$regex':'1gen', '$options':'i'}}},
        {'DISTRICT_ID.DISTRICT_NAME':{'$not':{'$regex':'test', '$options':'i'}}},
        {"schoolId._id":{"$in":db.school_master.distinct( "_id", { "IS_PORTAL": "Y" } )}},
        {'DISTRICT_ID':{'$exists':1}},
        {'INCOMPLETE_SIGNUP':{"$ne":'Y'}}, 
    #     {'MODIFIED_DATE':{"$gte":myDatetime}},
    #     {'ROLE_ID._id':ObjectId("5f155b8a3b6800007900da2b")},
        {'IS_BLOCKED':{"$ne":'Y'}}, 
        {'IS_DISABLED':{"$ne":'Y'}}, {'schoolId.NAME':{'$not':{'$regex':'test', '$options':'i'}}},
        ]}},
        {'$group':{'_id':{"district":'$DISTRICT_ID._id'},
        'NAME_DISTRICT':{'$first':'$DISTRICT_ID.DISTRICT_NAME'},
        "SCHOOL COUNT":{'$addToSet':"$schoolId._id"},
                  }},
            {"$project":{"_id":0,"DISTRICT_ID":"$_id.district","NAME_DISTRICT":1,"SCHOOL COUNT":{"$size":"$SCHOOL COUNT"} }}]
    merge1211=list(collection.aggregate(qra12))
    df1111=pd.DataFrame(merge1211)
    df1111=df1111.sort_values(by=['NAME_DISTRICT'], ascending=True)
    DISSCHOOL=df1111[["NAME_DISTRICT","SCHOOL COUNT"]]
    finmergeufosch=pd.merge(finmergeufo, DISSCHOOL, how='left', left_on='NAME_DISTRICT_x', right_on='NAME_DISTRICT')
    finmergeufoschprac=pd.merge(finmergeufosch, DISPRACTO, how='left', left_on='NAME_DISTRICT_x', right_on='NAME_DISTRICT')
    final_buuble_data=finmergeufoschprac[["NAME_DISTRICT_x","MONTH_x","USER ENGAGEMENT","FAMILY ENGAGEMENT","SCHOOL COUNT","PRACTICE"]]
    finaldata=final_buuble_data.rename(columns={"NAME_DISTRICT_x": "DISTRICT_NAME","USER ENGAGEMENT":"USER_ENGAGEMENT", "FAMILY ENGAGEMENT":"FAMILY_ENGAGEMENT","MONTH_x": "MONTH"})
    finaldata=finaldata.loc[:,~finaldata.columns.duplicated()]
    # findict=finaldata.T.to_dict().values()
    # response = make_response(finaldata.to_csv())
    # response.headers['Content-Type'] = 'text/csv'
    li = [finaldata.columns.values.tolist()] + finaldata.values.tolist() 
    sheet = pe.Sheet(li)
    print(sheet,"sheet")
    print(type(sheet),"sheet type")
    ioO = io.StringIO()
    sheet.save_to_memory("csv", ioO)
    output = make_response(ioO.getvalue())
    # output.headers["Content-Disposition"] = "attachment; filename=export.csv"
    output.headers["Content-type"] = "text/csv"
    return output
    # return Response(finaldata.to_csv())
# py2.7, for python3, please use import io

# app = Flask(__name__)






if __name__ == '__main__':
   app.run()
