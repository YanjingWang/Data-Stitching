#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 14 17:41:33 2020

@author: yanjingwang
"""
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#pip install mssql-scripter
import pyodbc
import time
import _thread
import os
import datetime
import smtplib
from email.mime.text import MIMEText
from email.header import Header


sqls = {"SELECT DISTINCT O.ServerName,O.DatabaseName,O.SchemaName,O.ObjectName FROM Object_Usage_Details AS O \
LEFT JOIN \
NDESQL21.NDE_POCDB.INFORMATION_SCHEMA.TABLES AS T \
ON T.TABLE_SCHEMA = O.ServerName +'_'+O.DatabaseName+'_'+O.SchemaName \
AND T.TABLE_NAME = O.ObjectName \
WHERE O.ObjectType = 'USER_TABLE' AND O.ServerName = 'NDESQL04' AND T.TABLE_SCHEMA IS NULL AND  T.TABLE_NAME IS NULL--AND O.DatabaseName = 'STG_MART_NDE'\
EXCEPT \
SELECT distinct ServerName,DatabaseName,SchemaName,ParentTable FROM [DataStitching].[ReferenceLookup] WHERE ServerName = 'NDESQL04' --AND DatabaseName = 'STG_MART_NDE'\
except \
SELECT distinct ServerName,DatabaseName,SchemaName,ChildTable FROM [DataStitching].[ReferenceLookup] WHERE ServerName = 'NDESQL04' --AND DatabaseName = 'STG_MART_NDE'":"Initial",
"SELECT DISTINCT ServerName,DatabaseName,SchemaName,ObjectName FROM [DataStitching].[Object_Usage_Details] \
WHERE Servername = 'NDESQL04' AND ObjectType = 'USER_TABLE' AND CONVERT(DATE,[LastWritetime])  = CONVERT(DATE,[RunDate]) \
except \
SELECT distinct ServerName,DatabaseName,SchemaName,ParentTable FROM [DataStitching].[ReferenceLookup] WHERE Servername = 'NDESQL04' \
except \
SELECT distinct ServerName,DatabaseName,SchemaName,ChildTable FROM [DataStitching].[ReferenceLookup] WHERE Servername = 'NDESQL04' ":"DML",   
"SELECT DISTINCT ServerName,DatabaseName,SchemaName,ObjectName FROM [DataStitching].[Object_Usage_Details] \
WHERE Servername = 'NDESQL04' AND ObjectType = 'USER_TABLE' AND CONVERT(DATE,[ModifiedDate])  = CONVERT(DATE,[RunDate]) \
except \
SELECT distinct ServerName,DatabaseName,SchemaName,ParentTable FROM [DataStitching].[ReferenceLookup] WHERE Servername = 'NDESQL04' \
except \
SELECT distinct ServerName,DatabaseName,SchemaName,ChildTable FROM [DataStitching].[ReferenceLookup] WHERE Servername = 'NDESQL04'":"ALTER",
"select  ServerName,DatabaseName,SchemaName,ObjectName FROM [DataStitching].[Object_Usage_Details] WHERE ObjectType = 'USER_TABLE' AND Servername = 'NDESQL04' and CONVERT(DATE,[RunDate]) = CONVERT(DATE,GETDATE()-1) \
EXCEPT \
select  ServerName,DatabaseName,SchemaName,ObjectName FROM [DataStitching].[Object_Usage_Details] WHERE ObjectType = 'USER_TABLE' AND Servername = 'NDESQL04' and CONVERT(DATE,[RunDate]) = CONVERT(DATE,GETDATE()) \
except \
SELECT distinct ServerName,DatabaseName,SchemaName,ParentTable FROM [DataStitching].[ReferenceLookup] WHERE Servername = 'NDESQL04' \
except \
SELECT distinct ServerName,DatabaseName,SchemaName,ChildTable FROM [DataStitching].[ReferenceLookup] WHERE Servername = 'NDESQL04'":"DROP" }



def insert_record(sql, thread_id):
    db = pyodbc.connect('Driver={SQL Server Native Client 11.0};Server=NDESQL21;Database=NDE_POCDB;Trusted_Connection=yes;')


    # using cursor 
    cursor = db.cursor()

    # sql query
    try:
       # execute sql query
        begin_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        print("sql  start at [%s]"%begin_time, "insert succeed")
        cursor.execute(sql)
        db.commit()
       
        #print("INSERT SUCCESS.")
    except Exception as ex:
        
        print ("insert failed",ex)
       # If error then rollback
        db.rollback()
    finally:
        # close DB connection
        print("threads finish :" "%s" % (time.ctime(time.time())))
        #count += 1
        db.close()

# Insert sql queries in threads
# Data means the list of all tuples containing servername, databasename,schemaname and objectname in current thread
# id means Thread ID
def insert_record_thread(data, thread_id, type, lock):
    for row in data:
        if type == 'DROP':
            sql = "EXECUTE [DataStitching].[DataStitchingLoad_DDL_DROP] @ServerName = '"+ row[0] + "'," + "@DatabaseName = '" + row[1] + "'," +"@SchemaName = '" + row[2] + "'," +"@TableName = '" + row[3] + "'"
            print(sql)
            insert_record(sql, thread_id)
            continue
        cmd = 'mssql-scripter -S ' + row[0] + ' -d ' + row[1] + ' --include-objects ' + row[2] + "." + row[3]
        res = os.popen(cmd)
        res_sql = res.read()
        res_sql = res_sql[3:]
        q1 = "[" + row[2] + "].[" + row[3] + "]"
        q2 = "["+ row[0] + "_" + row[1] + "_" + row[2] + "].[" + row[3] + "]"
        q3 = "USE [" + row[1] + "]"
        q4 = "USE [NDE_POCDB]"
        q5 = "GO"
        q6 = " "
        #print(q1)
        #print(q2)
        #print(q3)
        #print(q4)
        #if ("'" + row[2] + "." + row[3] + "'" in res_sql ):
        res_sql = res_sql.replace(q3,q4)
        res_sql = res_sql.replace(q1,q2)
        res_sql = res_sql.replace(q5,q6)
        #print(res_sql)
        #mssql-scripter -S NDESQL21 -d NDE_POCDB --include-objects NDESQL04_STG_MART_NDE_dbo.TCS_CURRENT_CERTIFICATES
        if type == 'DML':
            if ("IDENTITY(1,1)" in res_sql ):
                sql = "EXECUTE [DataStitching].[DataStitchingLoad_DML_ID] @ServerName = '"+ row[0] + "'," + "@DatabaseName = '" + row[1] + "'," +"@SchemaName = '" + row[2] + "'," +"@TableName = '" + row[3] + "'," +"@TableCreation = '"+ res_sql + "'"
            else:
                sql = "EXECUTE [DataStitching].[DataStitchingLoad_DML] @ServerName = '"+ row[0] + "'," + "@DatabaseName = '" + row[1] + "'," +"@SchemaName = '" + row[2] + "'," +"@TableName = '" + row[3] + "'," +"@TableCreation = '"+ res_sql + "'"
        elif type == 'ALTER':
            if ("IDENTITY(1,1)" in res_sql ):
                sql = "EXECUTE [DataStitching].[DataStitchingLoad_DDL_ALTER_ID] @ServerName = '"+ row[0] + "'," + "@DatabaseName = '" + row[1] + "'," +"@SchemaName = '" + row[2] + "'," +"@TableName = '" + row[3] + "'," +"@TableCreation = '"+ res_sql + "'"
            else:
                sql = "EXECUTE [DataStitching].[DataStitchingLoad_DDL_ALTER] @ServerName = '"+ row[0] + "'," + "@DatabaseName = '" + row[1] + "'," +"@SchemaName = '" + row[2] + "'," +"@TableName = '" + row[3] + "'," +"@TableCreation = '"+ res_sql + "'"
        elif type == 'Initial':
            if ("IDENTITY(1,1)" in res_sql ):
                sql = "EXECUTE [DataStitching].DataStitchingLoad_Initial_ID @ServerName = '"+ row[0] + "'," + "@DatabaseName = '" + row[1] + "'," +"@SchemaName = '" + row[2] + "'," +"@TableName = '" + row[3] + "'," +"@TableCreation = '"+ res_sql + "'"
            else:
                sql = "EXECUTE [DataStitching].DataStitchingLoad_Initial @ServerName = '"+ row[0] + "'," + "@DatabaseName = '" + row[1] + "'," +"@SchemaName = '" + row[2] + "'," +"@TableName = '" + row[3] + "'," +"@TableCreation = '"+ res_sql + "'"


        print(sql)
        #print(res_sql)
        #print(type(res_sql))
        #print(type(res))
        insert_record(sql, thread_id)
    lock.release()

def parseServerName(sql, type, num_threads, locks):
    db = pyodbc.connect('Driver={SQL Server Native Client 11.0};Server=NDESQL21;Database=NDE_POCDB;Trusted_Connection=yes;')
    cursor = db.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    db.close()

    # new_data： [[1, 2], [1, 2] ...]
    # The first position is a tuple，containing SERVERNAME,DatabaseName,SchemaName,ObjectName
    # The second position is the count of the database records
    new_data = []
    db = pyodbc.connect('Driver={SQL Server Native Client 11.0};Server=NDESQL21;Database=NDE_POCDB;Trusted_Connection=yes;')
    for row in data:
        cursor = db.cursor()
        temp_list = []
        try:
            # The first position
            temp_sql = "SELECT count(*) From " + row[0] + "." + row[1] + "." + row[2] + "." + row[3]
            cursor.execute(temp_sql)
            count = cursor.fetchall()[0]
            temp_list.append(row)
            # The second position is the count of the database records
            temp_list.append(count)
            new_data.append(temp_list)
            #print(row, "the number of rows of this table", count)
        except Exception as ex:
            #print(row,ex)
            db.rollback()
    db.close()
    
    ############db.close()
    #print(new_data)
    #new_data.sort(key=lambda x:x[1])
    thread_data = []
    for i in range(num_threads):
        thread_data.append([])

    j = 0
    for i in range(len(new_data)):
        thread_data[j].append(new_data[i][0])
        j = (j + 1) % num_threads

    for i in range(num_threads):
        _thread.start_new_thread(insert_record_thread, (thread_data[i], i, type, locks[i]))



def main():
    start_time = datetime.datetime.now()
    start_time_timestampStr = start_time.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    num_threads_per_sql = 1
    locks = []
    for i in range(num_threads_per_sql * len(sqls)):
        lock = _thread.allocate_lock()
        lock.acquire()
        locks.append(lock)
    i = 0
    for key, value in sqls.items():
        parseServerName(key, value, num_threads_per_sql, 
                        locks[i * num_threads_per_sql : (i + 1)*num_threads_per_sql])
        i += 1
    
    for i in range(num_threads_per_sql * len(sqls)):
        while(locks[i].locked()):
            pass
    print ("All done")
    
    end_time = datetime.datetime.now()
    end_time_timestampStr = end_time.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    content = 'start time: ' + start_time_timestampStr + '\nend_time: ' + end_time_timestampStr
    #send_email(content)

if __name__ == "__main__":  
    main()