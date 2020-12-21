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
from bson.regex import Regex
import pandas as pd
from flask import Flask,json
from datetime import datetime
from pandas.io.json import json_normalize
import pandas as pd
import pycountry
# import mysql.connector
import numpy as np
from flask_cors import CORS
from geolite2 import geolite2
import time
from datetime import timedelta
import pymongo
from pymongo import MongoClient
from pprint import pprint
import urllib.parse
from pandas import DataFrame
from bson.objectid import ObjectId
import datetime
from flask import Flask, make_response
import pyexcel as pe
import io 
import dateutil.parser
from datetime import date
import calendar
import re
import json
from urllib.request import urlopen
from sort_dataframeby_monthorweek import *
from pytz import timezone
from six.moves import urllib
from numpyencoder import NumpyEncoder
from flask_csv import send_csv


app = Flask(__name__)
CORS(app)
from flask import Response

@app.route('/large.csv')
def generate_large_csv():
    def generate():
        for row in iter_all_rows():
            yield ','.join(row) + '\n'
    return Response(generate(), mimetype='text/csv')
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

data = [
    ["REVIEW_DATE","AUTHOR","ISBN","DISCOUNTED_PRICE"],
    ["1985/01/21","Douglas Adams",'0345391802',5.95],
    ["1990/01/12","Douglas Hofstadter",'0465026567',9.95],
    ["1998/07/15","Timothy \"The Parser\" Campbell",'0968411304',18.99],
    ["1999/12/03","Richard Friedman",'0060630353',5.95],
    ["2004/10/04","Randel Helms",'0879755725',4.50]
]

@app.route('/download')
def download():
    sheet = pe.Sheet(data)
    print(sheet,"sheet")
    print(type(sheet),"sheet type")
    ioO = io.StringIO()
    sheet.save_to_memory("csv", ioO)
    output = make_response(ioO.getvalue())
    # output.headers["Content-Disposition"] = "attachment; filename=export.csv"
    output.headers["Content-type"] = "text/csv"
    return output
# #     output = StringIO()
# #     finaldata.to_csv(output)
#     response_stream = BytesIO(finaldata.to_csv().encode())
#     return send_file(response_stream,mimetype="text/csv",attachment_filename="export.csv")
    # return send_csv(findict,"test.csv", ["DISTRICT NAME","MONTH","USER ENGAGEMENT","FAMILY ENGAGEMENT","SCHOOL COUNT","PRACTICE"])

#     return Response(output.getvalue(), mimetype="text/csv")
#     return (fig_json)


@app.route('/schoolwisefamilycount/<districtid>')
def schpuc(districtid):
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('I#L@teST^m0NGO_2o20!')
    client = MongoClient("mongodb://%s:%s@34.214.24.229:27017/" % (username, password))
    db=client.compass 
    collection = db.user_master
    df = DataFrame(list(collection.aggregate([
    {"$match":
         {'$and': [
             {'ROLE_ID._id' :{'$eq':ObjectId("5f155b8a3b6800007900da2b")}},
                {"IS_DISABLED":{"$ne":"Y"}},
                  {"IS_BLOCKED":{"$ne":"Y"}},
                 {"INCOMPLETE_SIGNUP":{"$ne":"Y"}},
                { 'USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
                { 'USER_NAME':{"$not":{"$regex":"1gen",'$options':'i'}}},
    # //               {'IS_ADMIN':'Y'},
                {'IS_PORTAL':'Y'},
                 {'EMAIL_ID':{'$ne':''}},
                   {'DISTRICT_ID._id':{'$eq':ObjectId(""+districtid+"")}},
#                  {'DISTRICT_ID.DISTRICT_NAME':'Broward County Public Schools'},
    # //              {'EMAIL_ID':{'$regex':'broward','$options':'i'}},
                 {'schoolId.NAME':{"$not":{"$regex":"test",'$options':'i'}}},
             {'schoolId.BLOCKED_BY_CAP':{'$exists':False}},
                           {'EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
                             {'EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}}]}},
            {'$group':{'_id':'$schoolId._id','ID':{'$addToSet':'$_id'},'NAME':{'$first':'$schoolId.NAME'},'district':{'$first':'$DISTRICT_ID.DISTRICT_NAME'}}},
                  {'$project':{'_id':1,'user_count':{'$size':'$ID'},'name':'$NAME','district':'$district'}},
                   { '$sort' : { 'user_count' : -1}}
    # //               {'$count':'count'}
                  ])))

    # df['SCH_CREATED_DATE']=pd.to_datetime(df['SCH_CREATED_DATE'])

#     df=df.nlargest(20,'user_count')
#     df= df.groupby(df['district'])
#     df= df.get_group(''+district+'')


    if df.empty == True:
        
        schname=[]
        uc=[]
      
    else:
        schname=df['name'].tolist()
        uc=df['user_count'].tolist()
    
    

    data={'schname':schname[0:20],'Familycount':uc[0:20]}
    
    return json.dumps(data)


@app.route('/schoolwisefamilypracticecount/<districtid>')
def schppcfamily(districtid):
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('I#L@teST^m0NGO_2o20!')
    client = MongoClient("mongodb://%s:%s@34.214.24.229:27017/" % (username, password))
    db=client.compass 
    collection = db.audio_track_master
    df = DataFrame(list(collection.aggregate([
    {"$match":
         {'$and': [
              {'USER_ID.ROLE_ID._id' :{'$eq':ObjectId("5f155b8a3b6800007900da2b")}},
                {"USER_ID.IS_DISABLED":{"$ne":"Y"}},
                  {"USER_ID.IS_BLOCKED":{"$ne":"Y"}},
                 {"USER_ID.INCOMPLETE_SIGNUP":{"$ne":"Y"}},
                { 'USER_ID.USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
                { 'USER_ID.USER_NAME':{"$not":{"$regex":"1gen",'$options':'i'}}},
                {'USER_ID.schoolId._id':{'$ne':None}},
    # //               {'IS_ADMIN':'Y'},
    # //             {'USER_ID.IS_PORTAL':'Y'},
                 {'USER_ID.EMAIL_ID':{'$ne':''}},
              {'USER_ID.DISTRICT_ID._id':{'$eq':ObjectId(""+districtid+"")}},
#                  {'USER_ID.DISTRICT_ID.DISTRICT_NAME':'Broward County Public Schools'},

    # //              {'EMAIL_ID':{'$regex':'broward','$options':'i'}},
                 {'USER_ID.schoolId.NAME':{"$not":{"$regex":"test",'$options':'i'}}},
             {'USER_ID.schoolId.BLOCKED_BY_CAP':{'$exists':False}},
                           {'USER_ID.EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
                             {'USER_ID.EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}}]}},
            {'$group':{'_id':'$USER_ID.schoolId._id','pc':{'$sum':1},'NAME':{'$first':'$USER_ID.schoolId.NAME'},'district':{'$first':'$USER_ID.DISTRICT_ID.DISTRICT_NAME'}}},
                  {'$project':{'_id':1,'practice_count':'$pc','name':'$NAME','district':'$district'}},
            { '$sort' : { 'practice_count' : -1}}
    # //               {'$count':'count'}
                  ])))
    if df.empty == True:
        
        schname=[]
        pc=[]
      
    else:
        schname=df['name'].tolist()
        pc=df['practice_count'].tolist()

    # df['SCH_CREATED_DATE']=pd.to_datetime(df['SCH_CREATED_DATE'])

#     df=df.nlargest(20,'user_count')
#     df= df.groupby(df['district'])
#     df= df.get_group(''+district+'')

    

    data={'schname':schname[0:20],'Familypracticecount':pc[0:20]}
    
    return json.dumps(data)



@app.route('/schoolwisepracticecounttop20/<districtid>')
def schwisepc(districtid):
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('I#L@teST^m0NGO_2o20!')
    client = MongoClient("mongodb://%s:%s@34.214.24.229:27017/" % (username, password))
    db=client.compass 
    collection = db.audio_track_master
    df = DataFrame(list(collection.aggregate([
    {"$match":
         {'$and': [
               {'USER_ID.ROLE_ID._id' :{'$ne':ObjectId("5f155b8a3b6800007900da2b")}},
                {"USER_ID.IS_DISABLED":{"$ne":"Y"}},
                  {"USER_ID.IS_BLOCKED":{"$ne":"Y"}},
                 {"USER_ID.INCOMPLETE_SIGNUP":{"$ne":"Y"}},
                { 'USER_ID.USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
                { 'USER_ID.USER_NAME':{"$not":{"$regex":"1gen",'$options':'i'}}},
                {'USER_ID.schoolId._id':{'$ne':None}},
    # //               {'IS_ADMIN':'Y'},
    # //             {'USER_ID.IS_PORTAL':'Y'},
                 {'USER_ID.EMAIL_ID':{'$ne':''}},
               {'USER_ID.DISTRICT_ID._id':{'$eq':ObjectId(""+districtid+"")}},
#                  {'USER_ID.DISTRICT_ID.DISTRICT_NAME':'Broward County Public Schools'},

    # //              {'EMAIL_ID':{'$regex':'broward','$options':'i'}},
                 {'USER_ID.schoolId.NAME':{"$not":{"$regex":"test",'$options':'i'}}},
             {'USER_ID.schoolId.BLOCKED_BY_CAP':{'$exists':False}},
                           {'USER_ID.EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
                             {'USER_ID.EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}}]}},
            {'$group':{'_id':'$USER_ID.schoolId._id','pc':{'$sum':1},'NAME':{'$first':'$USER_ID.schoolId.NAME'},'district':{'$first':'$USER_ID.DISTRICT_ID.DISTRICT_NAME'}}},
                  {'$project':{'_id':1,'practice_count':'$pc','name':'$NAME','district':'$district'}},
            { '$sort' : { 'practice_count' : -1}}
    # //               {'$count':'count'}
                  ])))
    if df.empty == True:
        
        schname=[]
        pc=[]
      
    else:
        schname=df['name'].tolist()
        pc=df['practice_count'].tolist()


    # df['SCH_CREATED_DATE']=pd.to_datetime(df['SCH_CREATED_DATE'])

#     df=df.nlargest(20,'user_count')
#     df= df.groupby(df['district'])
#     df= df.get_group(''+district+'')
    

    data={'schname':schname[0:20],'top20practicecount':pc[0:20]}
    
    return json.dumps(data)




@app.route('/schoolwiseusercounttop20/<districtid>')
def schwiseuc(districtid):
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('I#L@teST^m0NGO_2o20!')
    client = MongoClient("mongodb://%s:%s@34.214.24.229:27017/" % (username, password))
    db=client.compass 
    collection = db.user_master
    df = DataFrame(list(collection.aggregate([
    {"$match":
         {'$and': [
             {'ROLE_ID._id' :{'$ne':ObjectId("5f155b8a3b6800007900da2b")}},
                {"IS_DISABLED":{"$ne":"Y"}},
                  {"IS_BLOCKED":{"$ne":"Y"}},
                 {"INCOMPLETE_SIGNUP":{"$ne":"Y"}},
                { 'USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
                { 'USER_NAME':{"$not":{"$regex":"1gen",'$options':'i'}}},
    # //               {'IS_ADMIN':'Y'},
                {'IS_PORTAL':'Y'},
                 {'EMAIL_ID':{'$ne':''}},
                   {'DISTRICT_ID._id':{'$eq':ObjectId(""+districtid+"")}},
#                  {'DISTRICT_ID.DISTRICT_NAME':'Broward County Public Schools'},
    # //              {'EMAIL_ID':{'$regex':'broward','$options':'i'}},
                 {'schoolId.NAME':{"$not":{"$regex":"test",'$options':'i'}}},
             {'schoolId.BLOCKED_BY_CAP':{'$exists':False}},
                           {'EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
                             {'EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}}]}},
            {'$group':{'_id':'$schoolId._id','ID':{'$addToSet':'$_id'},'NAME':{'$first':'$schoolId.NAME'},'district':{'$first':'$DISTRICT_ID.DISTRICT_NAME'}}},
                  {'$project':{'_id':1,'user_count':{'$size':'$ID'},'name':'$NAME','district':'$district'}},
                   { '$sort' : { 'user_count' : -1}}
    # //               {'$count':'count'}
                  ])))

    # df['SCH_CREATED_DATE']=pd.to_datetime(df['SCH_CREATED_DATE'])

#     df=df.nlargest(20,'user_count')
#     df= df.groupby(df['district'])
#     df= df.get_group(''+district+'')

    if df.empty == True:
        
        schname=[]
        pc=[]
      
    else:
        schname=df['name'].tolist()
        uc=df['user_count'].tolist()
   
    data={'schname':schname[0:20],'usercount':uc[0:20]}
    
    return json.dumps(data)


@app.route('/monthwisepracticedistrict/<districtid>')
def monthwisepc(districtid):
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('I#L@teST^m0NGO_2o20!')
    client = MongoClient("mongodb://%s:%s@34.214.24.229:27017/" % (username, password))
    db=client.compass 
    collection = db.audio_track_master
    df = DataFrame(list(collection.aggregate([
    {"$match":
    {'$and': [
    #           {'USER_ID.ROLE_ID._id' :{'$eq':ObjectId("5f155b8a3b6800007900da2b")}},
        {"USER_ID.IS_DISABLED":{"$ne":"Y"}},
          {"USER_ID.IS_BLOCKED":{"$ne":"Y"}},
         {"USER_ID.INCOMPLETE_SIGNUP":{"$ne":"Y"}},
        { 'USER_ID.USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
        { 'USER_ID.USER_NAME':{"$not":{"$regex":"1gen",'$options':'i'}}},
        {'USER_ID.schoolId._id':{'$ne':None}},
    # //               {'IS_ADMIN':'Y'},
    # //             {'USER_ID.IS_PORTAL':'Y'},
         {'USER_ID.EMAIL_ID':{'$ne':''}},
               {'USER_ID.DISTRICT_ID._id':{'$eq':ObjectId(""+districtid+"")}},
#              {'USER_ID.DISTRICT_ID.DISTRICT_NAME':'Broward County Public Schools'},
         {'MODIFIED_DATE':{'$gte':datetime.datetime(2020,1,1)}},
    # //              {'EMAIL_ID':{'$regex':'broward','$options':'i'}},
         {'USER_ID.schoolId.NAME':{"$not":{"$regex":"test",'$options':'i'}}},
     {'USER_ID.schoolId.BLOCKED_BY_CAP':{'$exists':False}},
                   {'USER_ID.EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
                     {'USER_ID.EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}}]}},
    {'$group':{'_id':{'$month':'$MODIFIED_DATE'},'pc':{'$sum':1},'district':{'$first':'$USER_ID.DISTRICT_ID.DISTRICT_NAME'}}},
          {'$project':{'_id':1,'practice_count':'$pc','district':'$district'}},
    { '$sort' : { '_id' : 1} }
              ])))
    
    
    
    

#     df= df.groupby(df['district'])
#     df= df.get_group(''+district+'')
    df.rename(columns = { '_id': 'Month'}, inplace = True)

    d = dict(enumerate(calendar.month_abbr))    # to convert monthnumber of dataframe into monthname
    
    try:
        df['Month'] = df['Month'].map(d)
    except:
        pass

    if df.empty == True:
        Month=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
        pc=[0,0,0,0,0,0,0,0,0,0,0,0]
    else:
        Month=df['Month'].tolist()
        pc=df['practice_count'].tolist()




    

    # #     data=[]    
    # #     for i,k in zip(schname,uc):

    # #         data.append([i,k])

    # #     for i in range(len(schname)):
    # #             schname[i] = schname[i]
   
    data={'monthname':Month,'practice_count':pc}
    return json.dumps(data)


@app.route('/90daysuserpractising/<districtid>')
def user_practice_90days(districtid):
    from datetime import datetime
    from datetime import timedelta
    
    today1= datetime.utcnow()
    tod1= today1+ timedelta(hours=4)
    start1= tod1-timedelta(days=90)
    
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('I#L@teST^m0NGO_2o20!')
    client = MongoClient("mongodb://%s:%s@34.214.24.229:27017/" % (username, password))
    db=client.compass 
    collection = db.audio_track_master
    df = DataFrame(list(collection.aggregate([
    {"$match":
     {'$and': [
#           {'USER_ID.ROLE_ID._id' :{'$eq':ObjectId("5f155b8a3b6800007900da2b")}},
            {"USER_ID.IS_DISABLED":{"$ne":"Y"}},
              {"USER_ID.IS_BLOCKED":{"$ne":"Y"}},
             {"USER_ID.INCOMPLETE_SIGNUP":{"$ne":"Y"}},
            { 'USER_ID.USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
            { 'USER_ID.USER_NAME':{"$not":{"$regex":"1gen",'$options':'i'}}},
            {'USER_ID.schoolId._id':{'$ne':None}},
# //               {'IS_ADMIN':'Y'},
# //             {'USER_ID.IS_PORTAL':'Y'},
             {'USER_ID.EMAIL_ID':{'$ne':''}},
             {'USER_ID.DISTRICT_ID._id':{'$eq':ObjectId(""+districtid+"")}},
#              {'USER_ID.DISTRICT_ID.DISTRICT_NAME':'Broward County Public Schools'},
             {'MODIFIED_DATE':{'$gte':start1}},
# //              {'EMAIL_ID':{'$regex':'broward','$options':'i'}},
             {'USER_ID.schoolId.NAME':{"$not":{"$regex":"test",'$options':'i'}}},
         {'USER_ID.schoolId.BLOCKED_BY_CAP':{'$exists':False}},
                       {'USER_ID.EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
                         {'USER_ID.EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}}]}},
        {'$group':{'_id':{"$dateToString": { "format": "%Y-%m-%d", "date": "$MODIFIED_DATE"}},'pc':{'$addToSet':'$USER_ID._id'},'district':{'$first':'$USER_ID.DISTRICT_ID.DISTRICT_NAME'}}},
              {'$project':{'_id':1,'user_count':{'$size':'$pc'},'district':'$district'}},
    { '$sort' : { '_id' : 1} }
    

# //               {'$count':'count'}
              ])))
    # df['SCH_CREATED_DATE']=pd.to_datetime(df['SCH_CREATED_DATE'])
    df.rename(columns = { '_id': 'date'}, inplace = True)
    
    if df.empty == True:
        
        date=[]
        uc=[]
      
    else:
        date=df['date'].tolist()
        uc=df['user_count'].tolist()
#     df=df.nlargest(20,'user_count')
#     df= df.groupby(df['district'])
#     df= df.get_group(''+district+'')

    
#     data=[]    
#     for i,k in zip(schname,uc):

#         data.append([i,k])
    
#     for i in range(len(schname)):
#             schname[i] = schname[i]
    data={'date':date,'daysuserpractising':uc}
    
    return json.dumps(data)



@app.route('/90daysuserloggedindetail/<districtid>')
def user_logins_90days(districtid):
    from datetime import datetime
    from datetime import timedelta
    today1= datetime.utcnow()
    tod1= today1+ timedelta(hours=4)
    start1= tod1-timedelta(days=90)
    
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('I#L@teST^m0NGO_2o20!')
    client = MongoClient("mongodb://%s:%s@34.214.24.229:27017/" % (username, password))
    db=client.compass 
    collection = db.login_logs
    df = DataFrame(list(collection.aggregate([
    {"$match":
     {'$and': [
#           {'USER_ID.ROLE_ID._id' :{'$eq':ObjectId("5f155b8a3b6800007900da2b")}},
            {"USER_ID.IS_DISABLED":{"$ne":"Y"}},
              {"USER_ID.IS_BLOCKED":{"$ne":"Y"}},
             {"USER_ID.INCOMPLETE_SIGNUP":{"$ne":"Y"}},
            { 'USER_ID.USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
            { 'USER_ID.USER_NAME':{"$not":{"$regex":"1gen",'$options':'i'}}},
            {'USER_ID.schoolId._id':{'$ne':None}},
# //               {'IS_ADMIN':'Y'},
# //             {'USER_ID.IS_PORTAL':'Y'},
             {'USER_ID.EMAIL_ID':{'$ne':''}},
             {'USER_ID.DISTRICT_ID._id':{'$eq':ObjectId(""+districtid+"")}},
#              {'USER_ID.DISTRICT_ID.DISTRICT_NAME':'Broward County Public Schools'},
             {'LAST_LOGGED_IN':{'$gte':start1}},
# //              {'EMAIL_ID':{'$regex':'broward','$options':'i'}},
             {'USER_ID.schoolId.NAME':{"$not":{"$regex":"test",'$options':'i'}}},
         {'USER_ID.schoolId.BLOCKED_BY_CAP':{'$exists':False}},
                       {'USER_ID.EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
                         {'USER_ID.EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}}]}},
        {'$group':{'_id':{"$dateToString": { "format": "%Y-%m-%d", "date": "$LAST_LOGGED_IN"}},'pc':{'$addToSet':'$USER_ID._id'},'NAME':{'$first':'$USER_ID.schoolId.NAME'},'un':{'$first':'$USER_ID.USER_NAME'}}},
              {'$project':{'_id':1,'user_count':{'$size':'$pc'}}},
    { '$sort' : { '_id' : 1} }
    

# //               {'$count':'count'}
              ])))
    # df['SCH_CREATED_DATE']=pd.to_datetime(df['SCH_CREATED_DATE'])
    df.rename(columns = { '_id': 'date'}, inplace = True)
#     df=df.nlargest(20,'user_count')
#     df= df.groupby(df['district'])
#     df= df.get_group(''+district+'')



    if df.empty == True:
        
        date=[]
        uc=[]
      
    else:
        date=df['date'].tolist()
        uc=df['user_count'].tolist()
    
  
    
#     data=[]    
#     for i,k in zip(schname,uc):

#         data.append([i,k])
    
#     for i in range(len(schname)):
#             schname[i] = schname[i]
    data={'date':date,'daysuserlogins':uc}
    
    return json.dumps(data)


@app.route('/top20userspractisinginfo/<districtid>')
def topusers_practice(districtid):
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('I#L@teST^m0NGO_2o20!')
    client = MongoClient("mongodb://%s:%s@34.214.24.229:27017/" % (username, password))
    db=client.compass 
    collection = db.audio_track_master
    df = DataFrame(list(collection.aggregate([
    {"$match":
    {'$and': [
#           {'USER_ID.ROLE_ID._id' :{'$eq':ObjectId("5f155b8a3b6800007900da2b")}},
            {"USER_ID.IS_DISABLED":{"$ne":"Y"}},
              {"USER_ID.IS_BLOCKED":{"$ne":"Y"}},
             {"USER_ID.INCOMPLETE_SIGNUP":{"$ne":"Y"}},
            { 'USER_ID.USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
            { 'USER_ID.USER_NAME':{"$not":{"$regex":"1gen",'$options':'i'}}},
            {'USER_ID.schoolId._id':{'$ne':None}},
# //               {'IS_ADMIN':'Y'},
# //             {'USER_ID.IS_PORTAL':'Y'},
             {'USER_ID.EMAIL_ID':{'$ne':''}},
                    {'USER_ID.DISTRICT_ID._id':{'$eq':ObjectId(""+districtid+"")}},
#              {'USER_ID.DISTRICT_ID.DISTRICT_NAME':'Broward County Public Schools'},
#              {'MODIFIED_DATE':{'$gte':ISODate('2020-11-01'),'$lt':ISODate('2020-12-01')}},
# //              {'EMAIL_ID':{'$regex':'broward','$options':'i'}},
             {'USER_ID.schoolId.NAME':{"$not":{"$regex":"test",'$options':'i'}}},
         {'USER_ID.schoolId.BLOCKED_BY_CAP':{'$exists':False}},
                       {'USER_ID.EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
                         {'USER_ID.EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}}]}},
        {'$group':{'_id':'$USER_ID._id','pc':{'$sum':1},'NAME':{'$first':'$USER_ID.schoolId.NAME'},'un':{'$first':'$USER_ID.USER_NAME'}}},
              {'$project':{'_id':1,'practice_count':'$pc','name':'$NAME','user_name':'$un'}},
    { '$sort' : { 'practice_count' : -1} }
    

# //               {'$count':'count'}
              ])))
    
    
    
#     df["users"] = df["user_name"] +','+' ' + df["name"]

    # df['SCH_CREATED_DATE']=pd.to_datetime(df['SCH_CREATED_DATE'])

#     df=df.nlargest(20,'user_count')
#     df= df.groupby(df['district'])
#     df= df.get_group(''+district+'')


    if df.empty == True:
        
        schname=[]
        pc=[]
      
    else:
        df["users"] = df["user_name"] +','+' ' + df["name"]
        schname=df['users'].tolist()
        pc=df['practice_count'].tolist()
    


    
#     data=[]    
#     for i,k in zip(schname,uc):

#         data.append([i,k])
    
#     for i in range(len(schname)):
#             schname[i] = schname[i]
    data={'schname':schname[0:20],'practicecount':pc[0:20]}
    
    return json.dumps(data)


@app.route('/districtcardsinfo/<districtid>')
def district_count_cards(districtid):
    from datetime import datetime
    from datetime import timedelta
    
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('I#L@teST^m0NGO_2o20!')
    client = MongoClient("mongodb://%s:%s@34.214.24.229:27017/" % (username, password))
    db=client.compass 
    collection1 = db.user_master
    collection2=db.audio_track_master
    collection3=db.login_logs
    df1 = DataFrame(list(collection1.aggregate([
     {"$match":
         {'$and': [
                {"IS_DISABLED":{"$ne":"Y"}},
                  {"IS_BLOCKED":{"$ne":"Y"}},
                 {"INCOMPLETE_SIGNUP":{"$ne":"Y"}},
                { 'USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
                { 'USER_NAME':{"$not":{"$regex":"1gen",'$options':'i'}}},
                 {'IS_PORTAL':'Y'},
                 {'EMAIL_ID':{'$ne':''}},
                 {'DISTRICT_ID._id':{'$eq':ObjectId(""+districtid+"")}},
    #              {'DISTRICT_ID.DISTRICT_NAME':'Sarasota County'},
    # //              {'EMAIL_ID':{'$regex':'broward','$options':'i'}},
                 {'schoolId.NAME':{"$not":{"$regex":"test",'$options':'i'}}},
             {'schoolId.BLOCKED_BY_CAP':{'$exists':False}},
                           {'EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
                             {'EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}}]}},
            {'$group':{'_id':'','ID':{'$addToSet':'$schoolId._id'},'dn':{'$first':'$DISTRICT_ID.DISTRICT_NAME'}}},
                  {'$project':{'_id':1,'school_count':{'$size':'$ID'},'district':'$dn'}}
                  ])))
    df2 = DataFrame(list(collection1.aggregate([ {"$match":
         {'$and': [
              {'ROLE_ID._id' :{'$eq':ObjectId("5f155b8a3b6800007900da2a")}},
                {"IS_DISABLED":{"$ne":"Y"}},
                  {"IS_BLOCKED":{"$ne":"Y"}},
                 {"INCOMPLETE_SIGNUP":{"$ne":"Y"}},
                { 'USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
                { 'USER_NAME':{"$not":{"$regex":"1gen",'$options':'i'}}},
#                  {'IS_PORTAL':'Y'},
                 {'EMAIL_ID':{'$ne':''}},
                 {'DISTRICT_ID._id':{'$eq':ObjectId(""+districtid+"")}},
                 {'schoolId.NAME':{"$not":{"$regex":"test",'$options':'i'}}},
             {'schoolId.BLOCKED_BY_CAP':{'$exists':False}},
                           {'EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
                             {'EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}}]}},
            {'$group':{'_id':'','ID':{'$addToSet':'$_id'}}},
                  {'$project':{'_id':1,'teacher_count':{'$size':'$ID'}}}
                  ])))
    df5 = DataFrame(list(collection1.aggregate([ {"$match":
         {'$and': [
              {'ROLE_ID._id' :{'$eq':ObjectId("5f155b8a3b6800007900da2b")}},
                {"IS_DISABLED":{"$ne":"Y"}},
                  {"IS_BLOCKED":{"$ne":"Y"}},
                 {"INCOMPLETE_SIGNUP":{"$ne":"Y"}},
                { 'USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
                { 'USER_NAME':{"$not":{"$regex":"1gen",'$options':'i'}}},
                 {'IS_PORTAL':'Y'},
                 {'EMAIL_ID':{'$ne':''}},
                 {'DISTRICT_ID._id':{'$eq':ObjectId(""+districtid+"")}},
                 {'schoolId.NAME':{"$not":{"$regex":"test",'$options':'i'}}},
             {'schoolId.BLOCKED_BY_CAP':{'$exists':False}},
                           {'EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
                             {'EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}}]}},
            {'$group':{'_id':'','ID':{'$addToSet':'$_id'}}},
                  {'$project':{'_id':1,'family_count':{'$size':'$ID'}}}
                  ])))
    today1= datetime.utcnow()
    tod1= today1+ timedelta(hours=4)
    start1= tod1-timedelta(days=30)
    df3=DataFrame(list(collection2.aggregate([
     {"$match":
         {'$and': [
    # //          {'ROLE_ID._id' :{'$':ObjectId("5f155b8a3b6800007900da2b")}},
                {"USER_ID.IS_DISABLED":{"$ne":"Y"}},
                  {"USER_ID.IS_BLOCKED":{"$ne":"Y"}},
                 {"USER_ID.INCOMPLETE_SIGNUP":{"$ne":"Y"}},
                { 'USER_ID.USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
                { 'USER_ID.USER_NAME':{"$not":{"$regex":"1gen",'$options':'i'}}},
    # //             {'USER_ID.IS_PORTAL':'Y'},
                 {'USER_ID.EMAIL_ID':{'$ne':''}},
                 {'USER_ID.DISTRICT_ID._id':{'$eq':ObjectId(""+districtid+"")}},
    #              {'USER_ID.DISTRICT_ID.DISTRICT_NAME':'Sarasota County'},
                 {'MODIFIED_DATE':{'$gte':start1}},
    # //              {'EMAIL_ID':{'$regex':'broward','$options':'i'}},
                 {'USER_ID.schoolId.NAME':{"$not":{"$regex":"test",'$options':'i'}}},
             {'USER_ID.schoolId.BLOCKED_BY_CAP':{'$exists':False}},
                           {'USER_ID.EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
                             {'USER_ID.EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}}]}},
            {'$group':{'_id':'','pc':{'$sum':1}}},
                  {'$project':{'_id':1,'practice_sessions':'$pc'}}])))
    df4=DataFrame(list(collection3.aggregate([{"$match":
         {'$and': [
    #           {'USER_ID.ROLE_ID._id' :{'$eq':ObjectId("5f155b8a3b6800007900da2b")}},
                {"USER_ID.IS_DISABLED":{"$ne":"Y"}},
                  {"USER_ID.IS_BLOCKED":{"$ne":"Y"}},
                 {"USER_ID.INCOMPLETE_SIGNUP":{"$ne":"Y"}},
                { 'USER_ID.USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
                { 'USER_ID.USER_NAME':{"$not":{"$regex":"1gen",'$options':'i'}}},
                {'USER_ID.schoolId._id':{'$ne':None}},
    # //               {'IS_ADMIN':'Y'},
    # //             {'USER_ID.IS_PORTAL':'Y'},
                 {'USER_ID.EMAIL_ID':{'$ne':''}},
                 {'USER_ID.DISTRICT_ID._id':{'$eq':ObjectId(""+districtid+"")}},
    #              {'USER_ID.DISTRICT_ID.DISTRICT_NAME':'Broward County Public Schools'},
                 {'LAST_LOGGED_IN':{'$gte':start1}},
    # //              {'EMAIL_ID':{'$regex':'broward','$options':'i'}},
                 {'USER_ID.schoolId.NAME':{"$not":{"$regex":"test",'$options':'i'}}},
             {'USER_ID.schoolId.BLOCKED_BY_CAP':{'$exists':False}},
                           {'USER_ID.EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
                             {'USER_ID.EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}}]}},
            {'$group':{'_id':'','pc':{'$sum':1}}},
                  {'$project':{'_id':1,'logins':'$pc'}}])))
    
#     if df4.empty:
#         lc=0
#     else:
        
#     df1 = df1.replace(np.nan, 'No Schools', regex=True)
#     df2 = df2.replace(np.nan, 'No Teachers', regex=True)
#     df3 = df3.fillna(0)
#     df4 = df4.fillna(0)
    sc=[0]
    try:
        sc=df1['school_count']
    except:
        sc=[0]
        
    tc=[0]
    try:
        tc=df2['teacher_count']
    except:
        tc=[0]
    
    pc=[0]
    try:
        pc=df3['practice_sessions']
    except:
        pc=[0]
    
    lc=[0]
    try:
        lc=df4['logins']
    except:
        lc=[0]
        
    fc=[0]
    try:
        fc=df5['family_count']
    except:
        fc=[0]
    
    
    
    
    dn=[0]
    try:
        dn=df1['district']
    except:
        dn=[0]
    
    
#     print(lc)
    
    data={"schoolcount":str(sc[0]),"teachercount":str(tc[0]),"familycount":str(fc[0]),"practicecount":str(pc[0]),"logincount":str(lc[0]),'district':str(dn[0])}
    return json.dumps(data)
@app.route('/portal_new_api/<smcategory>')
def portal_new_api(smcategory):    
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    collection = db.school_master
    from bson.objectid import ObjectId
#     smcategory="Agawam School district"
    query=[{'$match':{'$and':[
    { 'CATEGORY':{"$regex":""+smcategory+"",'$options':'i'}},]
    }},
    {"$project":{"_id":0,
    "UMSCHOOLID":'$_id',
    "UMSCHOOLNAME":'$NAME',
    "is_paid":"$FULL_EXPERIENCE",
                }},
    ]
    merge11=list(collection.aggregate(query))

    overallum11=pd.DataFrame(merge11)
#     print(overallum11,"helloooooooooooo1achsdkjcbsdkjcbsdku")
    # print(len(set(list(overallum11["UMSCHOOLID"]))),"school_count")
    lifetimelist=list(set(overallum11["UMSCHOOLID"]))
    total_school=len(lifetimelist)
    collection = db.user_master
    query=[{'$match':{'$and':[{
    "schoolId._id": {
    "$in":lifetimelist
    }   
    },
    { 'USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
    {'EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
    {'EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
    {'INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'IS_DISABLED':{"$ne":'Y'}},
    {'IS_BLOCKED':{"$ne":'Y'}},
    {'schoolId.BLOCKED_BY_CAP':{'$exists':0}},
    ]
    }},
    {"$project":{"_id":0,
    'ROLE':'$ROLE_ID.ROLE_NAME',
    'UMUSER_ID':'$_id',"USER_NAME":'$USER_NAME',
    "UMSCHOOLID":'$schoolId._id',
    "DISTRICT_NAME":"$DISTRICT_ID.DISTRICT_NAME",
                 "UMSCHOOLNAME":'$schoolId.NAME',
                }},
    ]
    merge1=list(collection.aggregate(query))
    overallum=pd.DataFrame(merge1)
#     print(overallum,"overallum")
    email=""
    schoolid=[]
    try:
        email=list(overallum["UMUSER_ID"])
        schoolid=list(overallum["UMSCHOOLID"])
    except:
        pass
    ################################sub_master################################
    collection = db.subscription_master
    qr=[
    {"$match":{"$and":[{'USER_ID._id':{"$in":email}},]}},
    {"$project":{"_id":0,
    'SMUSER_ID':'$USER_ID._id',
    "RENEWAL_DATE":"$SUBSCRIPTION_EXPIRE_DATE",
    }},]
    merge=[]
    overall=[]
    mergeddf=[]
    try:
        merge=list(collection.aggregate(qr))
        overall=pd.DataFrame(merge)
        mergeddf=pd.merge(overallum, overall, how='left', left_on='UMUSER_ID', right_on='SMUSER_ID')
    except:
        pass
    db=client.compass
    collection = db.audio_track_master
    qra=[
    {"$match":{'$and':[
        {'USER_ID.USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
    {'USER_ID.EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
    {'USER_ID.EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
    {'USER_ID.USER_NAME':{'$not':{'$regex':'1gen', '$options':'i'}}},
    {'USER_ID.INCOMPLETE_SIGNUP':{"$ne":'Y'}}, 
    {'USER_ID.IS_BLOCKED':{"$ne":'Y'}}, 
    {'USER_ID.IS_DISABLED':{"$ne":'Y'}}, {'USER_ID.schoolId.NAME':{'$not':{'$regex':'test', '$options':'i'}}},
    {'USER_ID.schoolId._id':{'$in':schoolid}},
    ]}},
    {'$group':{'_id':'$USER_ID.schoolId._id', 
    'atdLastpractice':{'$max':'$MODIFIED_DATE'},
    'atdPracticecount':{'$sum':1},
    'atdTotal_Mindful_Minutes':{"$sum":{"$round":[{"$divide":[{"$subtract":['$CURSOR_END','$cursorStart']}, 60]},2]}}}}]
    merge110=[]
    atd=[]
    mmm=0
    try:
        merge110=list(collection.aggregate(qra))
        atd=pd.DataFrame(merge110)
        mmm=str(round(sum(atd["atdTotal_Mindful_Minutes"])))
        finalmerge=pd.merge(mergeddf, atd, how='left', left_on='UMSCHOOLID', right_on='_id')
        finaldata=finalmerge[["DISTRICT_NAME","UMSCHOOLID","UMSCHOOLNAME","UMUSER_ID","ROLE","atdLastpractice","RENEWAL_DATE","atdPracticecount"]]
        finaldata["atdPracticecount"] = finaldata['atdPracticecount'].fillna(0)
        finaldata["atdPracticecount"] = finaldata['atdPracticecount'].astype('int')
        finaldata["atdPracticecount"] = finaldata['atdPracticecount'].astype('str')
    except:
        pass
#     print(finaldata)
    usercount=0
    try:
        usercount=len(finaldata[finaldata["ROLE"]=='user'])
    except:
        pass
    familycount=0
    try:
        familycount=len(finaldata[finaldata["ROLE"]=='PRESENT'])
    except:
        pass

    data2=[]
    totschnew=len(overallum11[overallum11["is_paid"]=="Y"])
    overallum11["UMSCHOOLID"] = overallum11["UMSCHOOLID"].astype('str')
    overallum11=overallum11.sort_values(by=['UMSCHOOLNAME'], ascending=True)
    for i in range(len(overallum11)):
        data2.append({"school_id":overallum11["UMSCHOOLID"][i],"school_name":overallum11["UMSCHOOLNAME"][i],"is_paid":overallum11["is_paid"][i]})
    finaldata={"data":data2,"total_school":totschnew,"user_count":usercount,"family_count":familycount,"mindful_minutes":mmm}
    return json.dumps(finaldata)
@app.route('/bubbleoverall')
def buble_district():
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
    finaldata=final_buuble_data.rename(columns={"NAME_DISTRICT_x": "DISTRICT NAME", "MONTH_x": "MONTH"})
    finaldata=finaldata.loc[:,~finaldata.columns.duplicated()]
    finaldata.to_csv("bubblesuper.csv")
    import plotly.express as px
#     import pandas as pd
    df=pd.read_csv("bubblesuper.csv")
    df1=df.fillna(0)
    fig=px.scatter(df1, x="USER ENGAGEMENT", y="FAMILY ENGAGEMENT", animation_frame="MONTH", animation_group="DISTRICT NAME",
               size="PRACTICE", color="SCHOOL COUNT", hover_name="DISTRICT NAME",
    #            log_x = True,
               size_max=40, width=1000, height=700,range_x=[-25,105],range_y=[-25,105])
    fig=fig.update_yaxes(tick0=5, dtick=10)
    fig=fig.update_xaxes(tick0=5, dtick=10)
    fig_json = fig.to_json()
    return (fig_json)
@app.route('/portal_test_new_api/<smcategory>')
def portal_testing_new_api(smcategory):    
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('test!_2o20')
    client = MongoClient("mongodb://%s:%s@52.37.152.224:27017/" % (username, password))
    db=client.compass
    collection = db.school_master
    from bson.objectid import ObjectId
#     smcategory="Agawam School district"
    query=[{'$match':{'$and':[
    { 'CATEGORY':{"$regex":""+smcategory+"",'$options':'i'}},]
    }},
    {"$project":{"_id":0,
    "UMSCHOOLID":'$_id',
    "UMSCHOOLNAME":'$NAME',
    "is_paid":"$FULL_EXPERIENCE",
                }},
    ]
    merge11=list(collection.aggregate(query))

    overallum11=pd.DataFrame(merge11)
    print(overallum11,"helloooooooooooo1achsdkjcbsdkjcbsdku")
    # print(len(set(list(overallum11["UMSCHOOLID"]))),"school_count")
    lifetimelist=[]
    try:
        
        lifetimelist=list(set(overallum11["UMSCHOOLID"]))
    except:
        pass
    total_school=len(lifetimelist)
    collection = db.user_master
    query=[{'$match':{'$and':[{
    "schoolId._id": {
    "$in":lifetimelist
    }   
    },
    { 'USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
    {'EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
    {'EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
    {'INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'IS_DISABLED':{"$ne":'Y'}},
    {'IS_BLOCKED':{"$ne":'Y'}},
    {'schoolId.BLOCKED_BY_CAP':{'$exists':0}},
    ]
    }},
    {"$project":{"_id":0,
    'ROLE':'$ROLE_ID.ROLE_NAME',
    'UMUSER_ID':'$_id',"USER_NAME":'$USER_NAME',
    "UMSCHOOLID":'$schoolId._id',
    "DISTRICT_NAME":"$DISTRICT_ID.DISTRICT_NAME",
                 "UMSCHOOLNAME":'$schoolId.NAME',
                }},
    ]
    merge1=list(collection.aggregate(query))
    overallum=pd.DataFrame(merge1)
#     print(overallum,"overallum")
    email=""
    schoolid=[]
    try:
        email=list(overallum["UMUSER_ID"])
        schoolid=list(overallum["UMSCHOOLID"])
    except:
        pass
    ################################sub_master################################
    collection = db.subscription_master
    qr=[
    {"$match":{"$and":[{'USER_ID._id':{"$in":email}},]}},
    {"$project":{"_id":0,
    'SMUSER_ID':'$USER_ID._id',
    "RENEWAL_DATE":"$SUBSCRIPTION_EXPIRE_DATE",
    }},]
    merge=[]
    overall=[]
    mergeddf=[]
    try:
        merge=list(collection.aggregate(qr))
        overall=pd.DataFrame(merge)
        mergeddf=pd.merge(overallum, overall, how='left', left_on='UMUSER_ID', right_on='SMUSER_ID')
    except:
        pass
    db=client.compass
    collection = db.audio_track_master
    qra=[
    {"$match":{'$and':[
        {'USER_ID.USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
    {'USER_ID.EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
    {'USER_ID.EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
    {'USER_ID.USER_NAME':{'$not':{'$regex':'1gen', '$options':'i'}}},
    {'USER_ID.INCOMPLETE_SIGNUP':{"$ne":'Y'}}, 
    {'USER_ID.IS_BLOCKED':{"$ne":'Y'}}, 
    {'USER_ID.IS_DISABLED':{"$ne":'Y'}}, {'USER_ID.schoolId.NAME':{'$not':{'$regex':'test', '$options':'i'}}},
    {'USER_ID.schoolId._id':{'$in':schoolid}},
    ]}},
    {'$group':{'_id':'$USER_ID.schoolId._id', 
    'atdLastpractice':{'$max':'$MODIFIED_DATE'},
    'atdPracticecount':{'$sum':1},
    'atdTotal_Mindful_Minutes':{"$sum":{"$round":[{"$divide":[{"$subtract":['$CURSOR_END','$cursorStart']}, 60]},2]}}}}]
    merge110=[]
    atd=[]
    mmm=0
    try:
        merge110=list(collection.aggregate(qra))
        atd=pd.DataFrame(merge110)
        mmm=str(round(sum(atd["atdTotal_Mindful_Minutes"])))
        finalmerge=pd.merge(mergeddf, atd, how='left', left_on='UMSCHOOLID', right_on='_id')
        finaldata=finalmerge[["DISTRICT_NAME","UMSCHOOLID","UMSCHOOLNAME","UMUSER_ID","ROLE","atdLastpractice","RENEWAL_DATE","atdPracticecount"]]
        finaldata["atdPracticecount"] = finaldata['atdPracticecount'].fillna(0)
        finaldata["atdPracticecount"] = finaldata['atdPracticecount'].astype('int')
        finaldata["atdPracticecount"] = finaldata['atdPracticecount'].astype('str')
    except:
        pass
#     print(finaldata)
    usercount=0
    try:
        usercount=len(finaldata[finaldata["ROLE"]=='user'])
    except:
        pass
    familycount=0
    try:
        familycount=len(finaldata[finaldata["ROLE"]=='PRESENT'])
    except:
        pass

    data2=[]
    totschnew=len(overallum11[overallum11["is_paid"]=="Y"])
    overallum11["UMSCHOOLID"] = overallum11["UMSCHOOLID"].astype('str')
    overallum11=overallum11.sort_values(by=['UMSCHOOLNAME'], ascending=True)
    for i in range(len(overallum11)):
        data2.append({"school_id":overallum11["UMSCHOOLID"][i],"school_name":overallum11["UMSCHOOLNAME"][i],"is_paid":overallum11["is_paid"][i]})
    finaldata={"data":data2,"total_school":totschnew,"user_count":usercount,"family_count":familycount,"mindful_minutes":mmm}
    return json.dumps(finaldata)





if __name__ == '__main__':
   app.run()
