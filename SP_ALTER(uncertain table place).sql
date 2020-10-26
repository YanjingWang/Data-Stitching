USE [NDE_DataLake]
GO
/****** Object:  StoredProcedure [DataStitching].[DataStitchingLoad_DDL_ALTER]    Script Date: 9/4/2020 10:52:09 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [DataStitching].[DataStitchingLoad_DDL_ALTER]
      -- Add the parameters for the stored procedure here
     @ServerName VARCHAR(500)
      ,@DatabaseName VARCHAR(500)
      ,@SchemaName VARCHAR(500)
      ,@TableName VARCHAR(500)
	  ,@TableCreation nvarchar(max)
AS
BEGIN
      -- SET NOCOUNT ON added to prevent extra result sets from
      -- interfering with SELECT statements.
      SET NOCOUNT ON;

      BEGIN TRY
            BEGIN TRANSACTION;

                  DECLARE
                  @OperationType varchar(100) = 'DDL_ALTER'
                  ,@Status VARCHAR(100) = 'Succeed'
                  ,@Starttime datetime2  = getdate()
                  --,@EndTime datetime2  = getdate()
				  --,@Timestamp datetime2  = format(getdate(),'MM/dd/yyyy,HH:mm:ss')
				  ,@Timestamp VARCHAR(MAX)  = format(getdate(),'MMddyyyyHHmmss')
                  ,@TargetName varchar(1000) = @ServerName + '_' + @DatabaseName + '_' + @SchemaName + '.' + @TableName  
                  ,@SourceName varchar(1000) = @ServerName + '.' + @DatabaseName + '.' + @SchemaName + '.' + @TableName
				  --,@BackUpName varchar(1000) = @ServerName + '.' + @DatabaseName + '.' + @SchemaName + '.' + @TableName + '_format(getdate(),''MM/dd/yyyy,HH:mm:ss'')' 
                  ,@TargetSchema varchar(1000) = @ServerName + '_' + @DatabaseName + '_' + @SchemaName
				  ,@TbCreation varchar(max) = @TableCreation


                  DECLARE @tsql nvarchar(max) =
				  + ' IF NOT EXISTS ('  --1.check scheme in target is exist or not (datalake)
				  + ' SELECT schema_name ' 
				  + ' FROM information_schema.schemata'
				  + ' WHERE schema_name = ''' +@Targetschema + ''' ) '
				  + ' BEGIN'
				  + ' EXEC sp_executesql N''' + 'CREATE SCHEMA ' + @Targetschema 
				  + ''' END'
                  + ' IF (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''' + @TargetSchema + ''' AND TABLE_NAME = ''' +@TableName + '''))' --2.check tb in target exists or not
                  + ' BEGIN'
				  + ' DROP TABLE IF EXISTS ' + @TargetName
				  + ' ' + @TABLECREATION 
                  + ' INSERT INTO ' + @TargetName
                  + ' SELECT * FROM ' + @SourceName
				  + ' USE [NDE_Datalake_Backup]' --CHECK IF SCEMA TERE
				  + ' IF NOT EXISTS ('
				  + ' SELECT schema_name ' 
				  + ' FROM information_schema.schemata'
				  + ' WHERE schema_name = ''' +@Targetschema + ''' ) '
				  + ' BEGIN'
				  + ' EXEC sp_executesql N''' + 'CREATE SCHEMA ' + @Targetschema -- 4.schema doesn't exist, create schema
				  + ''' END'
				  + ' SELECT * INTO ' + @Servername + '_' + @Databasename + '_' + @Schemaname + '.' + @TableName + '_'+ @Timestamp
				  + ' FROM ' + @Servername + '.' + @Databasename + '.' + @Schemaname + '.' + @TableName
				  + ' USE [NDE_Datalake]'
                  + ' PRINT(' + '1' +')'
				  + ' END'
				  + ' ELSE'
				  + ' BEGIN'
				  + ' ' + @TABLECREATION 
                  + ' INSERT INTO ' + @TargetName
                  + ' SELECT * FROM ' + @SourceName
				  + ' USE [NDE_Datalake_Backup]' --CHECK IF SCEMA TERE
				  + ' IF NOT EXISTS ('
				  + ' SELECT schema_name ' 
				  + ' FROM information_schema.schemata'
				  + ' WHERE schema_name = ''' +@Targetschema + ''' ) '
				  + ' BEGIN'
				  + ' EXEC sp_executesql N''' + 'CREATE SCHEMA ' + @Targetschema -- 4.schema doesn't exist, create schema
				  + ''' END'
				  + ' SELECT * INTO ' + @Servername + '_' + @Databasename + '_' + @Schemaname + '.' + @TableName + '_'+ @Timestamp
				  + ' FROM ' + @Servername + '.' + @Databasename + '.' + @Schemaname + '.' + @TableName
				  + ' USE [NDE_Datalake]'
                  + ' PRINT(' + '2' +')'
				  + ' END'




                  DECLARE @tsql_print varchar(max) = 
				  + ' IF NOT EXISTS ('  --1.check scheme in target is exist or not (datalake)
				  + ' SELECT schema_name ' 
				  + ' FROM information_schema.schemata'
				  + ' WHERE schema_name = ''' +@Targetschema + ''' ) '
				  + ' BEGIN'
				  + ' EXEC sp_executesql N''' + 'CREATE SCHEMA ' + @Targetschema 
				  + ''' END'
                  + ' IF (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''' + @TargetSchema + ''' AND TABLE_NAME = ''' +@TableName + '''))' --2.check tb in target exists or not
                  + ' BEGIN'
				  + ' DROP TABLE IF EXISTS ' + @TargetName
				  + ' ' + @TABLECREATION 
                  + ' INSERT INTO ' + @TargetName
                  + ' SELECT * FROM ' + @SourceName
				  + ' USE [NDE_Datalake_Backup]' --CHECK IF SCEMA TERE
				  + ' IF NOT EXISTS ('
				  + ' SELECT schema_name ' 
				  + ' FROM information_schema.schemata'
				  + ' WHERE schema_name = ''' +@Targetschema + ''' ) '
				  + ' BEGIN'
				  + ' EXEC sp_executesql N''' + 'CREATE SCHEMA ' + @Targetschema -- 4.schema doesn't exist, create schema
				  + ''' END'
				  + ' SELECT * INTO ' + @Servername + '_' + @Databasename + '_' + @Schemaname + '.' + @TableName + '_'+ @Timestamp
				  + ' FROM ' + @Servername + '.' + @Databasename + '.' + @Schemaname + '.' + @TableName
				  + ' USE [NDE_Datalake]'
                  + ' PRINT(' + '1' +')'
				  + ' END'
				  + ' ELSE'
				  + ' BEGIN'
				  + ' ' + @TABLECREATION 
                  + ' INSERT INTO ' + @TargetName
                  + ' SELECT * FROM ' + @SourceName
				  + ' USE [NDE_Datalake_Backup]' --CHECK IF SCEMA TERE
				  + ' IF NOT EXISTS ('
				  + ' SELECT schema_name ' 
				  + ' FROM information_schema.schemata'
				  + ' WHERE schema_name = ''' +@Targetschema + ''' ) '
				  + ' BEGIN'
				  + ' EXEC sp_executesql N''' + 'CREATE SCHEMA ' + @Targetschema -- 4.schema doesn't exist, create schema
				  + ''' END'
				  + ' SELECT * INTO ' + @Servername + '_' + @Databasename + '_' + @Schemaname + '.' + @TableName + '_'+ @Timestamp
				  + ' FROM ' + @Servername + '.' + @Databasename + '.' + @Schemaname + '.' + @TableName
				  + ' USE [NDE_Datalake]'
                  + ' PRINT(' + '2' +')'
				  + ' END'
				  PRINT @tsql_print;
                  EXECUTE sp_executesql @tsql;
				  --
				  declare @EndTime datetime2  = getdate()
                  --
                  DECLARE @V_SuccessfulTB varchar(1000) = @TargetName
                  DECLARE @ActualCount varchar(200)
                  Declare @TBName varchar(200)
                  Set @TBName = @TargetName
                  Declare @RowCount int
                  declare @SQL as varchar(max)
                  set @SQL = 'SELECT Count(*) FROM ' + @TBName
                  declare @MyCount table(MyRowCount int)
                  insert @MyCount
                  exec (@SQL)
                  set @ActualCount = (select * from @MyCount)
				  --
                  DECLARE @TargetCount varchar(200)
                  Declare @STBName varchar(200)
                  Set @STBName = @SourceName
                  Declare @SRowCount int
                  declare @SSQL as varchar(max)
                  set @SSQL = 'SELECT Count(*) FROM ' + @STBName
                  declare @MySCount table(MySRowCount int)
                  insert @MySCount
                  exec (@SSQL)
                  set @TargetCount = (select * from @MySCount)

                  INSERT INTO [DataStitching].[ExecutionLog3] 
                  (
				  [SourceServer],
				  [SourceDatabase],
				  [SourceSchema],
                  [SchemaName],
                  [TableName],
                  [Status], --Succeed/Failed
                  [OperationType],
                  [TableRows],
                  [SourceCount],
                  [StartTime],
                  [EndTime],
                  --[ErrorDetails],
                  [Rundate]
                        )
                  VALUES
                  (
				  @ServerName,
				  @DatabaseName,
				  @SchemaName,
                  @TargetSchema,
                  @TableName,
                  'SUCCEED',
                  @OperationType,
                  @TargetCount,
                  @ActualCount,
                  @Starttime,
                  @EndTime,
                  GETDATE()
                  )

				  DECLARE @id INT 
				  SET @id = 0 
				  UPDATE [DataStitching].[DataStitching_Backup]  
				  SET @id = [SeqenceCount] = @id + 1
				  declare @BackupName varchar(max)
				  select  @BackupName = max(name) from [NDESQL21].[NDE_Datalake_Backup].sys.tables where name like '%'+@TableName+'%'
				  declare @RealBackupName varchar(max) 
				  set @RealBackupName = @Targetschema + '.' + @BackupName
				   INSERT INTO [DataStitching].[DataStitching_Backup] 
                  (
				  [OriginalTableName],
				  [BackUptables],
				  [CreatedTimestamp],
                  [SeqenceCount],
                  [DeleteFlag]
                        )
                  VALUES
                  (
				  @SourceName,
				  @RealBackupName,
				  Getdate(),
                  @id,
				  'N'
                  )
            COMMIT;

      END TRY
      BEGIN CATCH
            --ROLLBACK;
            --THROW;
            DECLARE @V_FailedTB varchar(500) = @TargetName
            DECLARE @V_Error_Message VARCHAR(MAX)
            DECLARE @V_Error_Number INT
            DECLARE @V_Error_Line INT

            SELECT @V_Error_Message = ERROR_MESSAGE() 
            SELECT @V_Error_Number = ERROR_NUMBER()   
            SELECT @V_Error_Line =ERROR_LINE() 
            INSERT INTO [DataStitching].[ExecutionLog3] 
                  (
				  [SourceServer],
				  [SourceDatabase],
				  [SourceSchema],
                  [SchemaName],
                  [TableName],
                  [Status], --Succeed/Failed
                  [OperationType],
                  [TableRows],
                  [SourceCount],
                  [StartTime],
                  [EndTime],
                  [ErrorDetails],
                  [Rundate]
                        )
                  VALUES
                  (
				  @ServerName,
				  @DatabaseName,
				  @SchemaName,
                  @TargetSchema,
                  @TableName,
                  'Failed',
                  @OperationType,
                  @TargetCount,
                  @ActualCount,
                  @Starttime,
                  @EndTime,
                  @V_Error_Message,
                  GETDATE()
                  )
      
    IF (@@TRANCOUNT > 0)
        ROLLBACK TRANSACTION    
-- Transaction uncommittable
    IF (XACT_STATE()) = -1
      ROLLBACK TRANSACTION
	IF XACT_STATE() <> 0 
	  ROLLBACK TRANSACTION
-- Transaction committable
    IF (XACT_STATE()) = 1
      COMMIT TRANSACTION
      END CATCH

   
END


