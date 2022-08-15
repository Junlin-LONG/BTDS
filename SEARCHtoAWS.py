#!/usr/bin/env python
# coding: utf-8



import MySQLdb
import datetime
from datetime import timedelta, timezone
import time
import sys
import boto3


#get aws sns ARN from input name
def getARN( input_Name ):
    if input_Name == 'Ming':
        return 'arn:aws:sns:us-east-2:532957216038:ToMing'
    elif input_Name == 'Andy':
        return 'arn:aws:sns:us-east-2:532957216038:ToJunlin'
    elif input_Name == 'William':
        return 'arn:aws:sns:us-east-2:532957216038:ToYuxuan'
    else:
        return None
    
#call sns api
def SendEmail( Arn, Nme, Locat_Nme ):
    sns = boto3.client('sns')
                
    message_text = "Hello {0}, the system shows that you were in {1} building in 3 days and may have been in contact with a feverish person, please go to the hospital for covid-19 testing as soon as possible to ensure that the risk of epidemic is excluded.".format(
            str(Nme),
            str(Locat_Nme)
        )
                
    response = sns.publish(
            TopicArn = Arn,
            Message = message_text
        )
    
    return



while True:
    conn = MySQLdb.connect(host="database-1.cvffj12sng3f.us-east-2.rds.amazonaws.com",port=3306,user="admin",passwd="mypassword",db="IoT")
    cursor = conn.cursor()
    try:
        tz=timezone(timedelta(hours=-5))
        dt=datetime.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
        
        #general: '"+ dt + r"'
        #test: '2021-12-06 18:06:30'
        
        #step 1 and 2: select location id and date
        sql = r"select distinct Location_ID, date(date) from IoT.TravelLog where User_ID=(select distinct MAX(User_ID) from IoT.TravelLog where access='deny' and IoT.TravelLog.date > date_add('"+ dt + r"', interval -100 SECOND)) and IoT.TravelLog.date >= DATE_ADD('"+ dt + r"', INTERVAL -3 DAY);"
        cursor.execute(sql)
        rest = cursor.fetchall()
        print("Get location and date:")
        
        #step 3
        for i in rest:
            print("===  Location_ID: "+i[0]+", date: "+str(i[1])+"  ===")
                
            sql_3 = r"select distinct Name, Location_Nme from (IoT.TravelLog left join IoT.User using(User_ID)) left join IoT.Location using(Location_ID) where Location_ID = '"+i[0]+r"' and date(date) = '"+str(i[1])+"' ;"
            #print(sql_3)
            cursor.execute(sql_3)
            rest_3 = cursor.fetchall()
                
            rest_3 = list(rest_3) #[(Name, Location)]
            if rest_3 != None:
                for j in rest_3:
                    Name = j[0]
                    Location_Nme = j[1]
                    Arn = getARN(Name)
                    if Arn != None:
                        SendEmail(Arn, Name, Location_Nme)
                        print(Name+" | "+Location_Nme+" | send")
                    else: 
                        print(Name+" | "+Location_Nme+" | no email")
       
        print ("===== SELECT AND SEND SUCCESS  ====="+dt)
    except Exception as e:
        print ("********  SELECT FAIL    ********")
        print (e)
            
    sys.stdout.flush()
#    print(rest)
    cursor.close()
    conn.close() 
    time.sleep(100)
    







