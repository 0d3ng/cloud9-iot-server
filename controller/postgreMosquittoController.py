#!/usr/bin/python3

import sys
from bson import ObjectId
import json 
from function import *
import datetime
import pandas as pd
from controller import sensorController
from pytz import timezone
import statistics

import psycopg2
import bcrypt
import uuid
from datetime import datetime as dt

from configparser import ConfigParser

from function import cloud9Lib
config = ConfigParser()
config.read("config.ini")

#Config
host    = config["PSQL"]["host"]
port    = int(config["PSQL"]["port"])
uname   = config["PSQL"]["uname"]
pwd     = config["PSQL"]["pwd"]
db      = config["PSQL"]["db"]

def connection():
    conn = psycopg2.connect(
        database=db,
        user=uname,
        password=pwd,
        host=host,
        port=port
    )
    return conn

def addUser(fillData):
    if 'mqtt_username' not in fillData:
        return False
    if 'mqtt_pass' not in fillData:
        return False        
    conn = connection()
    cursor = conn.cursor()

    password = fillData['mqtt_pass']
    # converting password to array of bytes
    bytes = password.encode('utf-8')
    # generating the salt
    salt = bcrypt.gensalt(rounds=10)
    # Hashing the password
    hash = bcrypt.hashpw(bytes, salt)
    passHash = str(hash,'utf-8')
    userName = fillData['mqtt_username']
    queryUser = """ INSERT INTO "public"."MqttUsers" ("id", "userName", "passwordHash", "isAdmin", "isActive","createdAt","updatedAt") 
    VALUES(%s, %s, %s, %s, %s, %s, %s)"""
    ###For Checking Only
    print("---------------------")
    print(queryUser, (str(uuid.uuid4()),userName,passHash,"f","t",dt.now(),dt.now(),))
    print("---------------------")
    sys.stdout.flush()

    cursor.execute(queryUser, (str(uuid.uuid4()),userName,passHash,"f","t",dt.now(),dt.now(),))
    conn.commit()

    #Add MQTT Topic
    addTopic(fillData)

    return True


def addTopic(fillData):
    if 'mqtt_username' not in fillData:
        return False
    if 'topic' not in fillData:
        return False
    userName = fillData['mqtt_username']
    topic = fillData['topic']

    conn = connection()
    cursor = conn.cursor()
    postgreSQL_select_Query = """select * from "public"."MqttUsers"  where "userName" = %s """
    cursor.execute(postgreSQL_select_Query,(userName,))
    publisher_records = cursor.fetchall()
    userId = ""
    for row in publisher_records:        
        userId = row[0]
    if userId == "":
        return False
    
    queryTopic = """ INSERT INTO "public"."MqttAcls" ("id", "topic", "rw", "userId", "createdAt", "updatedAt") 
    VALUES(%s, %s, %s, %s, %s, %s)"""

    cursor.execute(queryTopic, (str(uuid.uuid4()),topic,999,userId,dt.now(),dt.now(),)) #999 --> all access
    conn.commit()
    if conn:
        cursor.close()
        conn.close()
    return True