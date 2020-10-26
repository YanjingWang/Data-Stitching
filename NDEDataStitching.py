###############################################################################################################
#-- ==========================================================================================================
#-- Author      :Data Architecture & Business Intelligence Team
# -- Create date:09/15/2020
#-- Description :This process will run daily and stitch the data from all the NDE serers to the DataLake
#-- Version     :1.0
#-- ==========================================================================================================
##############################################################################################################
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#pip install mssql-scripter

############################################################################
##########             Import Statements         ###########################
############################################################################
import pyodbc
import time
import threading
import os
import datetime
import sys
import shutil
############################################################################
####### Path variable declarations##########################################
############################################################################
logPath = r'C:\Test\DataStitching' #Place where the log file will be created
createLookupObjectsFile=r'C:\Test\DataStitching\createLookupObjects.sql' #Sql File from where the initial scan process will run
tableCreationLocation=r'C:\Test\DataStitching\TableCreationScripts'
tableCreationBackUpLocation=r'C:\Test\DataStitching\TableCreationScripts_BackUp'


############################################################################
#### Log Path and Log file creation process#################################
############################################################################
logFileName= 'NDEDataStitchingLog_'+str(datetime.datetime.now().strftime("%d-%b-%Y_%H_%M_%S_%f")+'.txt')
print(str(datetime.datetime.now())+' :Log File Created')
logFile = open(logPath+'\\'+logFileName, 'a')
logFile.write('#--------------------Log Process for data stitching ---------------------------#\n')
logFile.close()






#######################################################################################################
###The funtion will load the requried lookup data which is needed for current days process ############
## The main objective of the function is to load the tabke [DataStitching].[ModifedObjectDetails] with #
## all requried information which will be used in carrying out the data stitching operation .         #
#######################################################################################################
def createLookupObjects():
    try:

        #-------------------Log entry Step-------------------------------------------------------------------#
        logFile = open(logPath + '\\' + logFileName, 'a')
        logFile.write(str(datetime.datetime.now())+' :Starting the createLookupObjects process to load  [DataStitching].[ModifedObjectDetails]   '+'\n' )
        logFile.close()
        print(str(datetime.datetime.now())+' :Starting the createLookupObjects process to load  [DataStitching].[ModifedObjectDetails]    '+'\n')


        createLookupObjects_sql = open(createLookupObjectsFile, 'r')
        createLookupObjects_sql_str = createLookupObjects_sql.read()
        createLookupObjects_sql.close()
        #print(createLookupObjects_sql_str)

        # --DB connection String------------------------------------------------------------------------------------------------#
        db = pyodbc.connect('Driver={SQL Server Native Client 11.0};Server=NDESQL21;Database=NDE_POCDB;Trusted_Connection=yes; autocommit=True')
        cursor = db.cursor()
        cursor.execute(createLookupObjects_sql_str)
        db.commit()

        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile = open(logPath + '\\' + logFileName, 'a')
        logFile.write(str(datetime.datetime.now())+' :CreateLookupObjects process to load  [DataStitching].[ModifedObjectDetails]   finished successfully '+'\n' )
        logFile.close()
        print(str(datetime.datetime.now())+' :CreateLookupObjects process to load  [DataStitching].[ModifedObjectDetails]   finished successfully '+'\n')

    except Exception as ex:
        print(str(datetime.datetime.now())+' :createLookupObjects faild with the Error  '+ str(ex) )

        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile = open(logPath + '\\' + logFileName, 'a')
        logFile.write(str(datetime.datetime.now())+' :CreateLookupObjects process to load  [DataStitching].[ModifedObjectDetails] process faild with the Error  '+ str(ex)+'\n' )
        print(str(datetime.datetime.now())+' :CreateLookupObjects process to load  [DataStitching].[ModifedObjectDetails] process faild with the Error  '+ str(ex)+'\n' )
        logFile.close()
        sys.exit(1)
#---------------------------------------------------------------------------------------------------------------#



####################################################################################################################################################################################
##This function create the list of tables who has parent and child relationsships with the relationship levels.
##The data is loaded to the table [DataStitching].[ParentChildDetails]
##This will be used in the sbsequesnt process to carry out the stitching of the parent child tables.
def createPkFkObjects():
    try:

        # -------------------Log entry Process-------------------------------------------------------------------#
        logFile = open(logPath + '\\' + logFileName, 'a')
        logFile.write(str(datetime.datetime.now()) + ' :The createPkFkObjects process to load  [DataStitching].[ParentChildDetails] started    ' + '\n')
        logFile.close()
        print(str(datetime.datetime.now()) + ' :The createPkFkObjects process to load  [DataStitching].[ParentChildDetails] started   ' + '\n')

        # --DB connection String------------------------------------------------------------------------------------------------#
        db = pyodbc.connect('Driver={SQL Server Native Client 11.0};Server=NDESQL21;Database=NDE_POCDB;Trusted_Connection=yes; autocommit=True')
        cursor = db.cursor()
        createPkFkObjects_proc = 'EXECUTE [DataStitching].[createPkFkObjects]'
        cursor.execute(createPkFkObjects_proc)
        db.commit()


        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile = open(logPath + '\\' + logFileName, 'a')
        logFile.write(str(datetime.datetime.now())+' :The createPkFkObjects process to load  [DataStitching].[ParentChildDetails] completed     '+'\n' )
        logFile.close()
        print(str(datetime.datetime.now())+' :The createPkFkObjects process to load  [DataStitching].[ParentChildDetails] completed   '+'\n')

    except Exception as ex:

        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile = open(logPath + '\\' + logFileName, 'a')
        logFile.write(str(datetime.datetime.now())+' :The createPkFkObjects process to load  [DataStitching].[ParentChildDetails] failed with error   '+ str(ex)+'\n' )
        print(str(datetime.datetime.now())+' :The createPkFkObjects process to load  [DataStitching].[ParentChildDetails] failed with error '+ str(ex)+'\n' )
        logFile.close()
        sys.exit(1)


##Load the table  [DataStitching].[ColumnListDetails] with all the related informations.
##This will be used to create the table definations
def createColumnListsPkFk():
    try:
        print(str(datetime.datetime.now()) + ' :Starting the createColumnLists processs')

        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile = open(logPath + '\\' + logFileName, 'a')
        logFile.write(str(datetime.datetime.now())+' :The createColumnListsPkFk process to load [DataStitching].[ColumnListDetails] started   '+'\n' )
        logFile.close()
        print(str(datetime.datetime.now())+' :The createColumnListsPkFk process to load [DataStitching].[ColumnListDetails] started   '+'\n')



        # --DB connection String------------------------------------------------------------------------------------------------#
        db = pyodbc.connect('Driver={SQL Server Native Client 11.0};Server=NDESQL21;Database=NDE_POCDB;Trusted_Connection=yes; autocommit=True')
        cursor = db.cursor()
        createColumnLists_proc = 'EXECUTE [DataStitching].[ColumnListDetailsPkFk]'
        cursor.execute(createColumnLists_proc)
        db.commit()




        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile = open(logPath + '\\' + logFileName, 'a')
        logFile.write(str(datetime.datetime.now())+' :The createColumnListsPkFk process to load [DataStitching].[ColumnListDetails]  completed   '+'\n' )
        logFile.close()
        print(str(datetime.datetime.now())+' :The createColumnListsPkFk process to load [DataStitching].[ColumnListDetails]  completed  '+'\n')



    except Exception as ex:

        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile = open(logPath + '\\' + logFileName, 'a')
        logFile.write(str(datetime.datetime.now())+'  :The createColumnListsPkFk process to load [DataStitching].[ColumnListDetails] faild with the Error  '+ str(ex)+'\n' )
        print(str(datetime.datetime.now())+'   :The createColumnListsPkFk process to load [DataStitching].[ColumnListDetails] faild with the Error  '+ str(ex)+'\n' )
        logFile.close()
        sys.exit(1)







def createTableScripts():
    try:
        print(str(datetime.datetime.now()) + ' :Starting the createTableScripts processs')

        #get the list of tables and child tables got modifed.
        sqlColumnListDetails = "select TOP 10   * from [DataStitching].[ColumnListDetails] where CONVERT(DATE,Rundate)= (select max(CONVERT(DATE,Rundate)) from  [DataStitching].[ModifedObjectDetails])"
        db = pyodbc.connect('Driver={SQL Server Native Client 11.0};Server=NDESQL21;Database=NDE_POCDB;Trusted_Connection=yes;')
        cursor = db.cursor()
        cursor.execute(sqlColumnListDetails)
        table_details = cursor.fetchall()
        db.close()

        #create all the sql create statement files with parent and child table creation statement in proper order
        for table_detail in table_details:
            sarver_name     = table_detail[0]
            database_name   = table_detail[1]
            schema_name     = table_detail[2]
            parent_table    = table_detail[3]
            all_tables      = table_detail[8]
            new_schema_name = sarver_name+'_'+database_name+'_'+schema_name

            mssql_scripter_cmd = 'mssql-scripter -S ' + sarver_name + '  -d ' + database_name + '   --exclude-use-database --include-objects   ' + all_tables #+ ' >  C:\Test\DataStitching\TableCreationScripts\\' + sarver_name + '_' + database_name + '_' + schema_name + '_' + parent_table + '.sql'
            print(mssql_scripter_cmd)

            mssql_scripter_read=os.popen(mssql_scripter_cmd)

            table_creattion_file=open(tableCreationLocation + '\\' + sarver_name + '_' + database_name + '_' + schema_name + '_' + parent_table + '.sql', 'w')
            table_creattion_file.write(mssql_scripter_read.read())
            #table_creattion_file.write(mssql_scripter_read.read().replace('['+schema_name+']','['+new_schema_name+']'))
            #table_creattion_file.write(mssql_scripter_read.read().replace('GO', ''))
            table_creattion_file.close()

        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile = open(logPath + '\\' + logFileName, 'a')
        logFile.write(str(datetime.datetime.now())+' :The createTableScripts process completed   '+'\n' )
        logFile.close()
        print(str(datetime.datetime.now())+' :The createTableScripts process completed   '+'\n')


    except Exception as ex:

        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile = open(logPath + '\\' + logFileName, 'a')
        logFile.write(str(datetime.datetime.now())+' :createTableScripts faild with the Error  '+ str(ex)+'\n' )
        print(str(datetime.datetime.now())+' :createTableScripts faild with the Error  '+ str(ex)+'\n' )
        logFile.close()
        sys.exit(1)






def createTableScriptsInitial():
    try:
        print(str(datetime.datetime.now()) + ' :Starting the createTableScripts processs')

        #get the list of tables and child tables got modifed.
        sqlColumnListDetails = "select  servername,databasename,schemaname,descendant,MAX(lvl),CONVERT(DATE,Rundate) as rundate \
                               from [DataStitching].[ParentChildDetails] \
                                group by  servername,databasename,schemaname,descendant,CONVERT(DATE,Rundate)\
                                having    CONVERT(DATE,Rundate)= (select max(CONVERT(DATE,Rundate))) and  servername = 'NDESQL01'\
                                order by MAX(lvl)"

        db = pyodbc.connect('Driver={SQL Server Native Client 11.0};Server=NDESQL21;Database=NDE_POCDB;Trusted_Connection=yes;')
        cursor = db.cursor()
        cursor.execute(sqlColumnListDetails)
        table_details = cursor.fetchall()
        db.close()
        seqniceOfFile= 0


        #create all the sql create statement files with parent and child table creation statement in proper order
        for table_detail in table_details:
            sarver_name     = table_detail[0]
            database_name   = table_detail[1]
            schema_name     = table_detail[2]
            parent_table    = table_detail[3]
            lvl             = table_detail[4]
            new_schema_name = sarver_name+'_'+database_name+'_'+schema_name

            mssql_scripter_cmd = 'mssql-scripter -S ' + sarver_name + '  -d ' + database_name + '   --exclude-use-database --include-objects   ' + schema_name+'.'+parent_table #+ ' >  C:\Test\DataStitching\TableCreationScripts\\' + sarver_name + '_' + database_name + '_' + schema_name + '_' + parent_table + '.sql'
            print(mssql_scripter_cmd)

            mssql_scripter_read=os.popen(mssql_scripter_cmd)

            table_creattion_file=open(tableCreationLocation + '\\' + str(seqniceOfFile)
                                      +'___'+sarver_name + '_' + database_name + '_' + schema_name + '.' + parent_table + '.sql', 'w')
            #table_creattion_file.write(mssql_scripter_read.read())
            #table_creattion_file.write(mssql_scripter_read.read().replace('['+schema_name+']','['+new_schema_name+']'))
            table_creattion_file_read =mssql_scripter_read.read()
            table_creattion_file_read_replaceGo = table_creattion_file_read.replace('GO\n', '')
            table_creattion_file_read_replaceSchema=table_creattion_file_read_replaceGo.replace('['+schema_name+']','['+new_schema_name+']')
            table_creattion_file_read_replaceSchemaWithQuotes = table_creattion_file_read_replaceSchema.replace("'" + schema_name + "'", "'" + new_schema_name + "'")
            table_creattion_file.write(table_creattion_file_read_replaceSchemaWithQuotes)
            table_creattion_file.close()
            seqniceOfFile +=1

        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile = open(logPath + '\\' + logFileName, 'a')
        logFile.write(str(datetime.datetime.now())+' :The createTableScripts process completed   '+'\n' )
        logFile.close()
        print(str(datetime.datetime.now())+' :The createTableScripts process completed   '+'\n')


    except Exception as ex:

        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile = open(logPath + '\\' + logFileName, 'a')
        logFile.write(str(datetime.datetime.now())+' :createTableScripts faild with the Error  '+ str(ex)+'\n' )
        print(str(datetime.datetime.now())+' :createTableScripts faild with the Error  '+ str(ex)+'\n' )
        logFile.close()
        sys.exit(1)



#######################################################################################################
############################Helps the initial sorting ################################################
#######################################################################################################
def Sort(fileLists):
    # reverse = None (Sorts in Ascending order)
    # key is set to sort using second element of
    # sublist lambda has been used
    fileLists.sort(key=lambda x: int(x[0]))
    return fileLists


def SortReverse(fileLists):
    # reverse = None (Sorts in Ascending order)
    # key is set to sort using second element of
    # sublist lambda has been used
    fileLists.sort(key=lambda x: int(x[0]),reverse=True)
    return fileLists
#######################################################################################################
def stitchingInitial():
    try:

        print(str(datetime.datetime.now())+' :initial Starting the stitching  processs')


        # Create the schemas in the datalake and the backup database if they are not present.
        # --DB connection String------------------------------------------------------------------------------------------------#
        db = pyodbc.connect('Driver={SQL Server Native Client 11.0};Server=NDESQL21;Database=NDE_POCDB;Trusted_Connection=yes; autocommit=True')
        cursor = db.cursor()
        newSchemas = " select distinct "+ "'create schema '," + \
                                   " [TargetSchema] FROM [DataStitching].[ModifedObjectDetails] where SchemaExitsinTarget ='N' \
                                   and CONVERT(DATE,[RunDate]) =  (select max(CONVERT(DATE,Rundate)) \
                                   from  [DataStitching].[ModifedObjectDetails]) and TargetSchema not in \
                                   (select SCHEMA_NAME from INFORMATION_SCHEMA.SCHEMATA)  and [TargetSchema] not like '%\%'"


        cursor.execute(newSchemas)
        newSchemasLists = cursor.fetchall()

        ###Create the schemas if they are not in the tareget database/
        for newSchema in newSchemasLists:
            print(newSchema[0] +' '+newSchema[1])
            cursor.execute(newSchema[0] +' '+newSchema[1])
            db.commit()


        fileLists = []
        fileListSorted = []
        fileListSortedReverse = []
        for filename in os.listdir(tableCreationLocation):
            fileLists.append(filename.split('___'))

        print(fileLists)
        fileListSorted = Sort(fileLists)
        print(fileListSorted)

        # fileListSortedReverse=SortReverse(fileLists)
        # print(fileListSortedReverse)
        # for tables in fileListSortedReverse:
        #   print('DROP TABLE IF EXISTS ' +tables[1].split('.')[0]+'.'+tables[1].split('.')[1])

        for createStatementList in fileListSorted:
            # print(createStatementList[1])
            # print (tableCreationFiles)
            fileFullpath = tableCreationLocation + '\\' + createStatementList[0] + '___' + \
                           createStatementList[1]

            sqlScript = open(fileFullpath, encoding='utf-8-sig')
            # print(fileFullpath)
            sqlScript_string = sqlScript.read().replace('GO\n', '')
            #print(sqlScript_string)
            print('started creating the table ' + createStatementList[1])
            cursor.execute(sqlScript_string)
            db.commit()


            print('table ' + createStatementList[1] + ' got created ')
            print('Started inserting the data to the table ' + createStatementList[1] )


            #get the target schema name and teh table name from the file name
            targetSchema = createStatementList[1].split('.')[0]
            targtTable   = createStatementList[1].split('.')[1]



            createPkFkObjects_proc = 'EXECUTE [DataStitching].[stitchingInitial] '+targetSchema+' , '+targtTable
            cursor.execute(createPkFkObjects_proc)
            db.commit()
            print(targetSchema+' , '+targtTable+ ' Loaded successfully ')







            # #
            sqlScript.close()


        ##Move the files to the backup location
        files = os.listdir(tableCreationLocation)
        for f in files:
            shutil.move(tableCreationLocation+"\\"+ f, tableCreationBackUpLocation)

        #create tbe back up tables in the back up schema and carry out the back up table operation.
        #drop the existing tables in the target schema if it does not exits create them.
        #create and load the new tables .


        #remove the older back up tables and update the data in the backup lookup
        #if its the new table create the tables in the target and insert records from the soruce
        #if the table exists in the target then take the back up of the table
            #--Drop the tabbls
            #--create table
            #--Insert to the













        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile = open(logPath + '\\' + logFileName, 'a')
        logFile.write(str(datetime.datetime.now())+' :The stitching proces completed   '+'\n' )
        logFile.close()
        print(str(datetime.datetime.now())+' :The stitching proces completed   '+'\n')



    except Exception as ex:

        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile = open(logPath + '\\' + logFileName, 'a')
        logFile.write(str(datetime.datetime.now())+' :The initial stitching process faild with the Error  '+ str(ex)+'\n' )
        print(str(datetime.datetime.now())+' :The initial stitching proces faild with the Error  '+ str(ex)+'\n' )
        logFile.close()
        sys.exit(1)

#####################################################################################################################################################
def main():
    db = pyodbc.connect('Driver={SQL Server Native Client 11.0};Server=NDESQL21;Database=NDE_POCDB;Trusted_Connection=yes; autocommit=True')
    cursor = db.cursor()
    maxDateProcessed ="select case when (max(ProcessDate) = (select CONVERT(date,GETDATE()))) then 'AlreadyFinished' else \
                      'RerunRequired' end from [DataStitching].[DailyProcessStatus] where ProcessStatus='Success'"

    cursor.execute(maxDateProcessed)

    maxDateProcessedDate = cursor.fetchall()[0][0]



    if (maxDateProcessedDate == 'AlreadyFinished' ):
        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile = open(logPath + '\\' + logFileName, 'a')
        logFile.write(str(datetime.datetime.now())+' :The process already ran successfuly today ... exiting out    '+'\n' )
        logFile.close()
        print(str(datetime.datetime.now())+' :The process already ran successfuly today ... exiting out   '+'\n')
        #Exiting out of the process as it already ran.
        exit(0)
    else:



        try:

            print(str(datetime.datetime.now()) + ' :Starting the main processs......')


            ####################Process Start time log capture steps ###################################################
            progressStatement = "insert into [DataStitching].[DailyProcessStatus] select convert(date,GETDATE()) ,\
                            'NDEDataStitching',null,getdate(),null ;"
            #print(progressStatement)
            cursor.execute(progressStatement)
            db.commit()

            ############################################################################################################
            ############################Call various functions stitching functions   ###################################
            #createLookupObjects()
            #createPkFkObjects()
            #createColumnListsPkFk()#for pkfk

            #createTableScripts()

            #createTableScriptsInitial()



            stitchingInitial()
            #stitchingInitialNonPKFK()


            #validationsandCleanup()

            ####################Process Success time log capture steps ###################################################
            progressSuccessStatement = " update [DataStitching].[DailyProcessStatus] \
                                     set ProcessStatus = 'Success',\
                                     PrcessEndTime = GETDATE() \
                                     where  ProcessStatus is null and  PrcessEndTime is null "
            print(str(datetime.datetime.now()) + ' :Ending the main processs......')
            cursor.execute(progressSuccessStatement)
            db.commit()
            db.close()
            #----------------------------------------------------------------------------------------------------------#

        # Call the Create Lookup for PKFK to make the base data ready

        except Exception as ex:
        # -------------------Log entry Process-------------------------------------------------------------------#
        ####################Process Failuer time log capture steps ###################################################
            progressSuccessStatement = " update [DataStitching].[DailyProcessStatus] \
                                     set ProcessStatus = 'Failed',\
                                     PrcessEndTime = GETDATE() \
                                     where  ProcessStatus is null and  PrcessEndTime is null "
            print(progressSuccessStatement)
            cursor.execute(progressSuccessStatement)
            db.commit()
            db.close()

            logFile = open(logPath + '\\' + logFileName, 'a')
            logFile.write(str(datetime.datetime.now()) + ' :main process faild with the Error  ' + str(ex) + '\n')
            print(str(datetime.datetime.now()) + ' :main function failed . Please check the error file.  ' )
            logFile.close()
            sys.exit(1)
########################################################################################################################################################

if __name__ == '__main__':
    start_date = datetime.datetime.now().strftime("%d-%b-%Y_%H:%M:%S.%f")
    print(str(datetime.datetime.now())+' :The Data lake stitching process started ')

    ## Call the main function. This will take care of all the opeartions .
    main()