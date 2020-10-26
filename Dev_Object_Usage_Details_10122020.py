###############################################################################################################
#-- ==========================================================================================================
#-- Author      :Data Architecture & Business Intelligence Team
#-- Create date:10/08/2020
#-- Description :This process can run daily and get all the information about the objects present in various NDE servers.
#-- Version     :1.0
#-- Please check the design doc for more information about teh script.
#-- ==========================================================================================================
##############################################################################################################

### Import the python libraries
import pyodbc
import time
import os
import datetime
import sys
import shutil
import smtplib

startTime = datetime.datetime.now()




############################################################################
####### Path variable declarations##########################################
############################################################################
logPath                         = r'C:\Test\DataStitching\log\objectUsageDetails' #Place where the log file will be created < This need to be updated based on the environment it will run>
objectUsageDetailLookupFile     =r'C:\Test\DataStitching\config\objectUsageDetailLookupFile.sql' #This file contains teh table creation statement
referenceLookupFile             =r'C:\Test\DataStitching\config\referenceLookupFile.sql' #Reference Lookup sql
insertObjectUsageDetailsFile    =r'C:\Test\DataStitching\config\insert_object_usage_details.sql' #This file contains teh table creation statement
insertReferenceLookupFile       =r'C:\Test\DataStitching\config\insert_ReferenceLookup.sql' #Reference Lookup sql
serverFileList                  = r'C:\Test\DataStitching\config\sql_servers.TXT'
#targetServerDB                  = r'Driver={SQL Server};Server=NDESQL21;database=NDE_POCDB; Trusted_Connection=yes;'
targetServerDB                  = r'Driver={SQL Server};Server=NDESQL21;database=NDE_POCDB;UID=dsadm;PWD=Mx!33826ZXL;utocommit=True; '




#db = pyodbc.connect('Driver={SQL Server Native Client 11.0};Server=NDESQL21;Database=NDE_DataLake_PreFinal;UID=dsadm;PWD=Mx!33826ZXL;utocommit=True')
############################################################################
####### Variable Declaration      ##########################################
############################################################################

connection_list =[] # declare the connection  list
noAccessDblist =[]
servers = []  # declare the server list


############################################################################
#### Log Path and Log file creation process#################################
############################################################################
logFileName= 'NDEObjectUsageDetailLog_'+str(datetime.datetime.now().strftime("%d-%b-%Y_%H_%M_%S_%f")+'.txt')
print(str(datetime.datetime.now())+' :Log File Created')
logFile = open(logPath+'\\'+logFileName, 'a')
logFile.write('#--------------------Log Process for Object Usage Details  ---------------------------#\n\n\n')
logFile.write(str(datetime.datetime.now())+' :Start time of the process: '+str(startTime) +'\n\n' )
print(str(datetime.datetime.now())+' :Start time of the process: '+str(startTime) +'\n\n' )

logFile.close()




#############################################################################
#### Server  Details (List of serevrs on which the process will run) ########
#############################################################################

server_file_list = open(serverFileList,'r').read().split('\n')
#print(server_file_list)
for server in server_file_list:

    #servers.append('Driver={SQL Server Native Client 11.0};Server=' + server + '; Trusted_Connection=yes;')  # Modify the Driver= values based on the serevers
     servers.append('Driver={SQL Server Native Client 11.0};Server=' + server + '; UID=dsadm;PWD=Mx!33826ZXL;utocommit=True;')  # Modify the Driver= values based on the serevers



# Log entry process for server names:
logFile = open(logPath+'\\'+logFileName, 'a')
logFile.write(str(datetime.datetime.now())+' :The Object usage detail will run for the below servers:  '+'\n\n' )
print(str(datetime.datetime.now())+' :The Object usage detail will run for the below servers:  '+'\n\n' )

for serverConnectionDetails in servers:
    logFile.write(serverConnectionDetails.split(';')[1].split('=')[1]+'\n')
    print(serverConnectionDetails.split(';')[1].split('=')[1]+'\n')

#log
logFile.write('\n\n\n' )
logFile.close()



############################################################################
#### Read the required SQL files which will be used to object usage detail #
# process                                                           ########
#--------------------------------------------------------------------------#
############################################################################
#log
logFile = open(logPath+'\\'+logFileName, 'a')
logFile.write(str(datetime.datetime.now())+' :reading the sql config  files ..   '+'\n\n' )
print((str(datetime.datetime.now())+' :reading the sql config  files ..   '+'\n\n' ))



#-------------Objcct usage detail sql srting initialization ----------------------------------------------------#
objectUsageDetail_sql = open(objectUsageDetailLookupFile, 'r')
objectUsageDetail_sql_str = objectUsageDetail_sql.read()
objectUsageDetail_sql.close()
#print(objectUsageDetail_sql_str)

#log
logFile.write(str(datetime.datetime.now())+' :SQL for Object USage Detail is initialized ..  '+'\n' )
print(str(datetime.datetime.now())+' :SQL for Object USage Detail is initialized ..  '+'\n' )



#-------------Reference lookup sql string initialization----------------------------------------------------#
referenceLookupFile_sql = open(referenceLookupFile, 'r')
referenceLookupFile_sql_str = referenceLookupFile_sql.read()
referenceLookupFile_sql.close()
#print(referenceLookupFile_sql_str)

#log
logFile.write(str(datetime.datetime.now())+' :SQL for reference Lookup load is initialized ..  '+'\n' )
print(str(datetime.datetime.now())+' :SQL for reference Lookup load is initialized ..  '+'\n' )



#-------------Objcct usage detail insert initialization  ----------------------------------------------------#
insertObjectUsageDetailsFile_sql = open(insertObjectUsageDetailsFile, 'r')
insertObjectUsageDetailsFile_sql_str = insertObjectUsageDetailsFile_sql.read()
insertObjectUsageDetailsFile_sql.close()
print(insertObjectUsageDetailsFile_sql_str)

#log
logFile.write(str(datetime.datetime.now())+' :SQL for Object USage Detail insert initialized ..  '+'\n' )
print(str(datetime.datetime.now())+' :SQL for Object USage Detail insert initialized ..  '+'\n' )





#-------------Reference lookup  insert initialization  ----------------------------------------------------#
insertReferenceLookupFile_sql = open(insertReferenceLookupFile, 'r')
insertReferenceLookupFile_sql_str = insertReferenceLookupFile_sql.read()
insertReferenceLookupFile_sql.close()
print(insertReferenceLookupFile_sql_str)

#log
logFile.write(str(datetime.datetime.now())+' :SQL for reference lookup insert initialized ..  '+'\n' )
print(str(datetime.datetime.now())+' :SQL for reference lookup insert initialized ..  '+'\n' )
logFile.write('\n\n' )
print('\n\n' )
logFile.close()


################################################################################
##Create the connection string to connect to the target DB serevr   "###########
################################################################################
write_conn = pyodbc.connect(targetServerDB)
write_cursor = write_conn.cursor()
#print(targetServerDB)

#log
logFile = open(logPath+'\\'+logFileName, 'a')
logFile.write(str(datetime.datetime.now())+' :Target db server  '+'"'+str(targetServerDB.split(';')[1])+'"'+ ' initialized ..'+ '\n\n\n' )
print(str(datetime.datetime.now())+' :Target db server  '+'"'+str(targetServerDB.split(';')[1])+'"'+ ' initialized ..'+ '\n\n\n' )
logFile.close()




##########################################################################################
##get all the connction strings across all the serevrs and all the databases  "###########
##########################################################################################

###################################################################################################################
# This for loop is to select all the user created databases (except 'master','tempdb','model','msdb') #############
#in all the srev8942360432ers the script is scanning###############################################################
###################################################################################################################
for server_details in servers:
    conn = pyodbc.connect(server_details)
    connNoAccess = pyodbc.connect(server_details)
    cursor = conn.cursor()
    cursorNoAccess = connNoAccess.cursor()
    databases =cursor.execute("SELECT name FROM master.sys.databases where name not in ('master','tempdb','model','msdb','ReportServer','SSISDB') and  HAS_DBACCESS(name) = 1")
    #databases = cursor.execute("SELECT name FROM master.sys.databases where name  in ('NDE_ETL','AQuESTT','NDE_DataWarehouse')")
    databasesNoAccess=cursorNoAccess.execute("SELECT name FROM master.sys.databases where name not in ('master','tempdb','model','msdb','ReportServer','SSISDB') and  HAS_DBACCESS(name) != 1")


    #databases = cursor.execute("SELECT name FROM master.sys.databases where name  in ('NDE_ETL','AQuESTT','NDE_DataWarehouse')")

    for database in databases:
        #connnection_string= "Driver={"+ server_details.split(';')[0][8:-1]+"};Server=" + server_details.split(';')[1][7:] + ';database=' + str(database)[2:-4]+ ";Trusted_Connection=yes;"
        connnection_string ="Driver={" + server_details.split(';')[0][8:-1]+"};Server="+ server_details.split(';')[1][7:] + ';database=' + str(database)[2:-4] + ';UID=dsadm;PWD=Mx!33826ZXL;utocommit=True;'

        connection_list.append(connnection_string)
        #print('Connection lists'+str(connection_list))

    for database in databasesNoAccess:
        noAccessDblist.append(str(server_details.split(';')[1].split('=')[1])+'.'+str(database[0]))


#log
logFile = open(logPath+'\\'+logFileName, 'a')
logFile.write(str(datetime.datetime.now())+' :Connection string for all the servers and databases initialized  ..'+ '\n\n' )
print(str(datetime.datetime.now())+' :Connection string for all the servers and databases initialized  ..'+ '\n\n' )

if (len(noAccessDblist)) > 0:
    logFile.write(str(datetime.datetime.now())+' :Below are the list of databases the sql id does not have access and hence skipped . Raise request to get access if needed'+ '\n' )
    for noAccessDb in noAccessDblist:
        logFile.write(str(noAccessDb)+ '\n')

    print(str(datetime.datetime.now())+' :Below are the list of databases the sql id does not have access and hence skipped . Raise request to get access if needed'+ '\n' )
    for noAccessDb in noAccessDblist:
        print(str(noAccessDb) + '\n')

# logFile.write('Below are all the connection strings will be used in the load process :'+ '\n' )
# print('Below are all the connection strings will be used in the load process :'+ '\n' )
# logFile.write(str(connection_list))
# print(str(connection_list))
logFile.write('\n\n\n')
print('\n\n\n')
logFile.close()



###################################################################################################################################
# Load the details to object usage detail and the reference lookup tables for all the databased in all the serevrs    #############
###################################################g###############################################################################
##################################################################################################################################

#log
logFile = open(logPath+'\\'+logFileName, 'a')
logFile.write(str(datetime.datetime.now())+' :Load process started for object_usage_details & ReferenceLookup    ..'+ '\n\n' )
print(str(datetime.datetime.now())+' :Load process started for object_usage_details & ReferenceLookup    ..'+ '\n\n' )

#print(referenceLookupFile_sql_str)


for databases in connection_list :
    connobjectUsage = pyodbc.connect(databases)
    connreferenceLookup = pyodbc.connect(databases)

    objectUsageDetailcursor = connobjectUsage.cursor()
    objectUsageDetail_sql_statements = objectUsageDetailcursor.execute(objectUsageDetail_sql_str)


    referenceLookupFilecursor = connreferenceLookup.cursor()
    referenceLookupFile_sql_statements = referenceLookupFilecursor.execute(referenceLookupFile_sql_str)

    #print(type(objectUsageDetail_sql_statements))
    # log
    logFile.write(str(datetime.datetime.now()) + ' :load process started  for the server :' + databases.split(';')[1].split('=')[1] +' and database '+databases.split(';')[2].split('=')[1]  +'\n ')
    print(str(datetime.datetime.now()) + ' :load process started  for the server :' + databases.split(';')[1].split('=')[1] +' and database '+databases.split(';')[2].split('=')[1] + '\n ')

    logFile.write(str(datetime.datetime.now()) + ': Started load for object usage detail '+ '\n ')
    print(str(datetime.datetime.now()) + ':Started load for object usage detail'+ '\n ')
    [ write_cursor.execute(insertObjectUsageDetailsFile_sql_str, insertRows) for insertRows in objectUsageDetailcursor.execute(objectUsageDetail_sql_str)]
    write_conn.commit()

    logFile.write(str(datetime.datetime.now()) + ': Finished load for object usage detail and started load for reference lookup ' + '\n ')
    print(str(datetime.datetime.now()) + ': Finished load for object usage detail and started load for reference lookup' + '\n ')

    [ write_cursor.execute(insertReferenceLookupFile_sql_str, insertRows) for insertRows in referenceLookupFilecursor.execute(referenceLookupFile_sql_str)]
    write_conn.commit()

    logFile.write(str(datetime.datetime.now()) + ': Finished load for  reference lookup ' + '\n ')
    print(str(datetime.datetime.now()) + ': Finished load for  reference lookup ' + '\n ')





    # log
    logFile.write(str(datetime.datetime.now()) + ' :load process finished  for the server :' + databases.split(';')[1].split('=')[1] +' and database '+databases.split(';')[2].split('=')[1]  +'\n\n ')
    print(str(datetime.datetime.now()) + ' :load process finished  for the server :' + databases.split(';')[1].split('=')[1] +' and database '+databases.split(';')[2].split('=')[1]  +'\n\n ')


endTime = datetime.datetime.now()

#log
logFile.write('\n\n\n'+str(datetime.datetime.now()) + ' :Entire load  process ended at  :' + str(endTime) + '\n')
print('\n\n\n'+str(datetime.datetime.now()) + ' :Entire load  process ended at  :' + str(endTime) + '\n')



#print('Total runtime :'+str(endTime - startTime))

#log
logFile.write(str(datetime.datetime.now()) + ':The entire load process took   :' + str(endTime - startTime) +' hours  and the process completed successfully '+ '\n')
print(str(datetime.datetime.now()) + ':The entire load process took   :' + str(endTime - startTime) +' hours  and the process completed successfully '+ '\n')



###################################################################################################################################
#  Mail communication process                                                                                          #############
###################################################g###############################################################################
##################################################################################################################################


HOST    ='mxout.ne.gov'
FROM    = 'abhishek.parida@nebraska.gov'
SUBJECT = "<<<Object Usage Detail run status>>>"
TO      = ['abhishek.parida@nebraska.gov']

text    = "Hi All ,\n  " \
          "The Object Usage detail run process for today completed successfully.\n\n" \
          "Start time      :" + str(startTime) +"\n" \
          "End time        :" + str(endTime) +"\n" \
          "Total Run time  :" + str(endTime - startTime) +' hrs' +"\n\n\n" \
          "Also check the log for more information about the loading process "\
          "Thanks and Regards,\n\nData Architecture & Business Intelligence Team\n\n\n"\
          "Note : Please do not reply .This is an automated mail . Contact the Data Architecture & Business Intelligence team  for more details."


BODY="\r\n".join((
    "From: %s" %FROM,
    "To: %s" %TO,
    "Subject: %s" %SUBJECT ,
    "",
    text
    ))


server = smtplib.SMTP(HOST)
server.sendmail(FROM, TO, BODY)

# log
logFile.write(str(datetime.datetime.now()) + ' :Mail communication done ' + '\n')
print(str(datetime.datetime.now()) + ':Mail communication done ' + '\n')
logFile.close()

