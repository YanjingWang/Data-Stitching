###############################################################################################################
#-- ==========================================================================================================
#-- Author      :Charlotte Wang
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
logPath_NonPKFK = r'C:\Test\DataStitching' #Place where the log file will be created
createLookupObjectsFile_NonPKFK=r'C:\Test\DataStitching\createLookupObjects_NonPKFK.sql' #Sql File from where the initial scan process will run
tableCreationLocation_NonPKFK=r'C:\Test\DataStitching\TableCreationScripts_NonPKFK'
tableCreationBackUpLocation_NonPKFK=r'C:\Test\DataStitching\TableCreationScripts_NonPKFK_BackUp'


############################################################################
#### Log Path and Log file creation process#################################
############################################################################
logFileName_NonPKFK= 'NDEDataStitchingLog_NonPKFK_'+str(datetime.datetime.now().strftime("%d-%b-%Y_%H_%M_%S_%f")+'.txt')
print(str(datetime.datetime.now())+' :NonPKFK Log File Created')
logFile_NonPKFK = open(logPath_NonPKFK+'\\'+logFileName_NonPKFK, 'a')
logFile_NonPKFK.write('#--------------------_NonPKFK Log Process for data stitching ---------------------------#\n')
logFile_NonPKFK.close()





#---------------------------------------------------------------------------------------------------------------#



##Load the table  [DataStitching].[ColumnListDetails] with all the related informations.
##This will be used to create the table definations
def createColumnListsPkFk_NonPKFK():
    try:
        print(str(datetime.datetime.now()) + ' :Starting the NonPKFK createColumnLists processs')

        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile_NonPKFK = open(logPath_NonPKFK + '\\' + logFileName_NonPKFK, 'a')
        logFile_NonPKFK.write(str(datetime.datetime.now())+' :The NonPKFK createColumnListsPkFk process to load [DataStitching].[NonPKFKColumnListDetails] started   '+'\n' )
        logFile_NonPKFK.close()
        print(str(datetime.datetime.now())+' :The NonPKFK createColumnListsPkFk process to load [DataStitching].[NonPKFKColumnListDetails] started   '+'\n')



        # --DB connection String------------------------------------------------------------------------------------------------#
        db = pyodbc.connect('Driver={SQL Server Native Client 11.0};Server=NDESQL21;Database=NDE_DataLake_PreFinal;UID=11111;PWD=11111;utocommit=True') 
        cursor = db.cursor()
        createColumnLists_proc = 'EXECUTE [DataStitching].[ColumnListDetailsNonPkFk]'
        cursor.execute(createColumnLists_proc)
        db.commit()




        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile_NonPKFK = open(logPath_NonPKFK + '\\' + logFileName_NonPKFK, 'a')
        logFile_NonPKFK.write(str(datetime.datetime.now())+' :The NonPKFK createColumnListsPkFk process to load [DataStitching].[NonPKFKColumnListDetails]  completed   '+'\n' )
        logFile_NonPKFK.close()
        print(str(datetime.datetime.now())+' :The NonPKFK createColumnListsPkFk process to load [DataStitching].[NonPKFKColumnListDetails]  completed  '+'\n')



    except Exception as ex:

        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile_NonPKFK = open(logPath_NonPKFK + '\\' + logFileName_NonPKFK, 'a')
        logFile_NonPKFK.write(str(datetime.datetime.now())+'  :The NonPKFK createColumnListsPkFk process to load [DataStitching].[NonPKFKColumnListDetails] faild with the Error  '+ str(ex)+'\n' )
        print(str(datetime.datetime.now())+'   :The NonPKFK createColumnListsPkFk process to load [DataStitching].[NonPKFKColumnListDetails] faild with the Error  '+ str(ex)+'\n' )
        logFile_NonPKFK.close()
        sys.exit(1)







def createTableScripts_NonPKFK():
    try:
        print(str(datetime.datetime.now()) + ' :Starting the createTableScripts processs')

        #get the list of tables and child tables got modifed.
        sqlColumnListDetails = "select * from [DataStitching].[NonPKFKColumnListDetails] "
        db = pyodbc.connect('Driver={SQL Server Native Client 11.0};Server=NDESQL21;Database=NDE_DataLake_PreFinal;UID=dsadm;PWD=Mx!33826ZXL;utocommit=True') 
        cursor = db.cursor()
        cursor.execute(sqlColumnListDetails)
        table_details = cursor.fetchall()
        db.close()

        #create all the sql create statement files with parent and child table creation statement in proper order
        for table_detail in table_details:
            server_name     = table_detail[0]
            database_name   = table_detail[1]
            schema_name     = table_detail[2]
            NonPKFK_table   = table_detail[3]
            all_tables      = table_detail[2]+"."+table_detail[3]
            new_schema_name = sarver_name+'_'+database_name+'_'+schema_name

            mssql_scripter_cmd = 'mssql-scripter -U dsadm -P Mx!33826ZXL -S ' + server_name + '  -d ' + database_name + '   --exclude-use-database --exclude-triggers --include-objects   ' + "' " +all_tables + " '"#+ ' >  C:\Test\DataStitching\TableCreationScripts\\' + sarver_name + '_' + database_name + '_' + schema_name + '_' + NonPKFK_table + '.sql'
            print(mssql_scripter_cmd)

            mssql_scripter_read=os.popen(mssql_scripter_cmd)

            table_creattion_file=open(tableCreationLocation_NonPKFK + '\\' + server_name + '_' + database_name + '_' + schema_name + '_' + NonPKFK_table + '.sql', 'w')
            table_creattion_file.write(mssql_scripter_read.read())
            #table_creattion_file.write(mssql_scripter_read.read().replace('['+schema_name+']','['+new_schema_name+']'))
            #table_creattion_file.write(mssql_scripter_read.read().replace('GO', ''))
            table_creattion_file.close()

        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile_NonPKFK = open(logPath_NonPKFK + '\\' + logFileName_NonPKFK, 'a')
        logFile_NonPKFK.write(str(datetime.datetime.now())+' :The createTableScripts process completed   '+'\n' )
        logFile_NonPKFK.close()
        print(str(datetime.datetime.now())+' :The createTableScripts process completed   '+'\n')


    except Exception as ex:

        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile_NonPKFK = open(logPath_NonPKFK + '\\' + logFileName_NonPKFK, 'a')
        logFile_NonPKFK.write(str(datetime.datetime.now())+' :createTableScripts faild with the Error  '+ str(ex)+'\n' )
        print(str(datetime.datetime.now())+' :createTableScripts faild with the Error  '+ str(ex)+'\n' )
        logFile_NonPKFK.close()
        sys.exit(1)






def createTableScriptsInitial_NonPKFK():
    try:
        print(str(datetime.datetime.now()) + ' :Starting the NON PKFK createTableScripts processs')

        #get the list of tables and child tables got modifed.
        #sqlColumnListDetails = " SELECT DISTINCT SERVERNAME, DATABASENAME, SCHEMANAME, OBJECTNAME from [DataStitching].[NonPKFKColumnListDetails]  "
        sqlColumnListDetails = " SELECT DISTINCT SERVERNAME, DATABASENAME, SCHEMANAME, OBJECTNAME from [dbo].[NonPKFKColumnListDetails_BK_09252020_Sequence] "
        db = pyodbc.connect('Driver={SQL Server Native Client 11.0};Server=NDESQL21;Database=NDE_DataLake_PreFinal;UID=dsadm;PWD=Mx!33826ZXL;utocommit=True') 
        cursor = db.cursor()
        cursor.execute(sqlColumnListDetails)
        table_details = cursor.fetchall()
        db.close()
        #seqniceOfFile= 0
        seqniceOfFile= 8323


        #create all the sql create statement files with parent and child table creation statement in proper order
        for table_detail in table_details:
            sarver_name     = table_detail[0]
            database_name   = table_detail[1]
            schema_name     = table_detail[2]
            NonPKFK_table    = table_detail[3]
           
            new_schema_name = sarver_name+'_'+database_name+'_'+schema_name

            mssql_scripter_cmd = 'mssql-scripter -U dsadm -P Mx!33826ZXL -S ' + sarver_name + '  -d ' + database_name + '   --exclude-use-database --exclude-triggers --include-objects   ' + "' " + schema_name+'.'+NonPKFK_table + " '" #+ ' >  C:\Test\DataStitching\TableCreationScripts\\' + sarver_name + '_' + database_name + '_' + schema_name + '_' + parent_table + '.sql'
            print(mssql_scripter_cmd)

            mssql_scripter_read=os.popen(mssql_scripter_cmd)

            table_creattion_file=open(tableCreationLocation_NonPKFK + '\\' + str(seqniceOfFile)
                                      +'___'+sarver_name + '_' + database_name + '_' + schema_name + '.' + NonPKFK_table + '.sql', 'w')
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
        logFile_NonPKFK = open(logPath_NonPKFK + '\\' + logFileName_NonPKFK, 'a')
        logFile_NonPKFK.write(str(datetime.datetime.now())+' :The createTableScripts process completed   '+'\n' )
        logFile_NonPKFK.close()
        print(str(datetime.datetime.now())+' :The createTableScripts process completed   '+'\n')


    except Exception as ex:

        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile_NonPKFK = open(logPath_NonPKFK + '\\' + logFileName_NonPKFK, 'a')
        logFile_NonPKFK.write(str(datetime.datetime.now())+' :createTableScripts faild with the Error  '+ str(ex)+'\n' )
        print(str(datetime.datetime.now())+' :createTableScripts faild with the Error  '+ str(ex)+'\n' )
        logFile_NonPKFK.close()
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
def stitchingInitial_NonPKFK():
    try:

        print(str(datetime.datetime.now())+' :initial Starting the stitching  processs')


        # Create the schemas in the datalake and the backup database if they are not present.
        # --DB connection String------------------------------------------------------------------------------------------------#
        db = pyodbc.connect('Driver={SQL Server Native Client 11.0};Server=NDESQL21;Database=NDE_DataLake_PreFinal;UID=dsadm;PWD=Mx!33826ZXL;utocommit=True') 
        cursor = db.cursor()



#         newSchemas = " select distinct "+ "'create schema '," + \
#                                    " [TargetSchema] FROM [DataStitching].[ModifedObjectDetails] where SchemaExitsinTarget ='N' \
#                                    and CONVERT(DATE,[RunDate]) =  (select max(CONVERT(DATE,Rundate)) \
#                                    from  [DataStitching].[ModifedObjectDetails]) and TargetSchema not in \
#                                    (select SCHEMA_NAME from INFORMATION_SCHEMA.SCHEMATA)  and [TargetSchema] not like '%\%' \
#                                    [TargetSchema] not like '%[%' "



#         cursor.execute(newSchemas)
#         newSchemasLists = cursor.fetchall()

#         ###Create the schemas if they are not in the tareget database/
#         for newSchema in newSchemasLists:
#             print(newSchema[0] +' '+newSchema[1])
#             cursor.execute(newSchema[0] +' '+newSchema[1])
#             db.commit()


        fileLists = []
        fileListSorted = []
        fileListSortedReverse = []
        for filename in os.listdir(tableCreationLocation_NonPKFK):
            fileLists.append(filename.split('___'))

        #print(fileLists)
        fileListSorted = Sort(fileLists)
        #print(fileListSorted)

#         for sortedFiles in fileLists:
#             tableDetailsTobeLoaded= 'insert into [DataStitching].[InitialLoadStatus] select '+ sortedFiles[0]+ ',\''+sortedFiles[1].split('.')[0]+'\',\''+sortedFiles[1].split('.')[1]+'\',\''+'N'+'\',\''+'N'+'\','+ 'null'+','+'null'+','+ 'getdate()'
#             print(tableDetailsTobeLoaded)
#             cursor.execute(tableDetailsTobeLoaded)
#             db.commit()
         
        
#          fileListSortedReverse=SortReverse(fileLists)
#         print(fileListSortedReverse)
#         for tables in fileListSortedReverse:
#             print('DROP TABLE IF EXISTS ' +tables[1].split('.')[0]+'.'+tables[1].split('.')[1])

        for createStatementList in fileListSorted:
            # print(createStatementList[1])
            #print (tableCreationFiles)
            #get the target schema name and teh table name from the file name
            targetSchema = createStatementList[1].split('.')[0]
            targtTable   = createStatementList[1].split('.')[1]
            fileFullpath = tableCreationLocation_NonPKFK + '\\' + createStatementList[0] + '___' + \
                           createStatementList[1]

            #print (fileFullpath)

            sqlScript = open(fileFullpath, encoding='utf-8-sig')
            # print(fileFullpath)
            sqlScript_string = sqlScript.read().replace('GO\n', '')
            #print(sqlScript_string)
            print('started creating the table ' + createStatementList[1])
            cursor.execute(sqlScript_string)
            db.commit()

            TableCreationStatusUpdateQuery = "update  [DataStitching].[InitialLoadStatus] set TableCreationStatus='Y' \
                                             where SchemaName='"+targetSchema+"' and ObjectName='"+targtTable+"'"
            cursor.execute(TableCreationStatusUpdateQuery)
            db.commit()
            sqlScript.close()

            #print(TableCreationStatusUpdateQuery)


            print('Successfully creating the table ' + createStatementList[1])

            shutil.move(fileFullpath, tableCreationBackUpLocation_NonPKFK)
            print(' The table '+createStatementList[1]+' moved to the backup location')
            #
            print('table ' + createStatementList[1] + ' got created ')
            print('Started inserting the data to the table ' + createStatementList[1] )
            #
            #

            #
            #
            #
            #
            #
            createPkFkObjects_proc = 'EXECUTE [DataStitching].[NonPKFKstitchingInitial] '+targetSchema+' , '+targtTable


            cursor.execute(createPkFkObjects_proc)
            db.commit()

            TableLoadStatusUpdateQuery = "update  [DataStitching].[InitialLoadStatus] set TableLoadStatus='Y' \
                                             where SchemaName='"+targetSchema+"' and ObjectName='"+targtTable+"'"
            cursor.execute(TableLoadStatusUpdateQuery)
            db.commit()
            print(TableCreationStatusUpdateQuery)


            print(targetSchema+' , '+targtTable+ ' Loaded successfully ')


        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile_NonPKFK = open(logPath_NonPKFK + '\\' + logFileName_NonPKFK, 'a')
        logFile_NonPKFK.write(str(datetime.datetime.now())+' :The stitching proces completed   '+'\n' )
        logFile_NonPKFK.close()
        print(str(datetime.datetime.now())+' :The stitching proces completed   '+'\n')



    except Exception as ex:

        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile_NonPKFK = open(logPath_NonPKFK + '\\' + logFileName_NonPKFK, 'a')
        logFile_NonPKFK.write(str(datetime.datetime.now())+' :The initial stitching process faild with the Error  '+ str(ex)+'\n' )
        print(str(datetime.datetime.now())+' :The initial stitching proces faild with the Error  '+ str(ex)+'\n' )
        logFile_NonPKFK.close()
        sys.exit(1)

#####################################################################################################################################################
def main():
    db = pyodbc.connect('Driver={SQL Server Native Client 11.0};Server=NDESQL21;Database=NDE_DataLake_PreFinal;UID=dsadm;PWD=Mx!33826ZXL;utocommit=True') 
    cursor = db.cursor()
    maxDateProcessed ="select case when (max(ProcessDate) = (select CONVERT(date,GETDATE()))) then 'AlreadyFinished' else \
                      'RerunRequired' end from [DataStitching].[DailyProcessStatus] where ProcessStatus='Success'"

    cursor.execute(maxDateProcessed)

    maxDateProcessedDate = cursor.fetchall()[0][0]



    if (maxDateProcessedDate == 'NonPKFK AlreadyFinished' ):
        #-------------------Log entry Process-------------------------------------------------------------------#
        logFile_NonPKFK = open(logPath_NonPKFK + '\\' + logFileName_NonPKFK, 'a')
        logFile_NonPKFK.write(str(datetime.datetime.now())+' :The process already ran successfuly today ... exiting out    '+'\n' )
        logFile_NonPKFK.close()
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
            #createColumnListsPkFk_NonPKFK()

            #createTableScripts()
            #createTableScripts_NonPKFK()

            #createTableScriptsInitial()
            #createTableScriptsInitial_NonPKFK()




            #stitchingInitial()
            stitchingInitial_NonPKFK()


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

            logFile_NonPKFK = open(logPath_NonPKFK + '\\' + logFileName_NonPKFK, 'a')
            logFile_NonPKFK.write(str(datetime.datetime.now()) + ' :main process faild with the Error  ' + str(ex) + '\n')
            print(str(datetime.datetime.now()) + ' :main function failed . Please check the error file.  ' )
            logFile_NonPKFK.close()
            sys.exit(1)
########################################################################################################################################################

if __name__ == '__main__':
    start_date = datetime.datetime.now().strftime("%d-%b-%Y_%H:%M:%S.%f")
    print(str(datetime.datetime.now())+' :The Data lake stitching process started ')

    ## Call the main function. This will take care of all the opeartions .
    main()
