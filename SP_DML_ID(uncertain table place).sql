USE [NDE_DataLake]
GO
/****** Object:  StoredProcedure [DataStitching].[DataStitchingLoad_DML_ID]    Script Date: 9/3/2020 4:54:13 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [DataStitching].[DataStitchingLoad_DML_ID]
      -- Add the parameters for the stored procedure here
     @ServerName VARCHAR(500)
      ,@DatabaseName VARCHAR(500)
      ,@SchemaName VARCHAR(500)
      ,@TableName VARCHAR(500)
	  ,@TableCreation VARCHAR(MAX) 
AS
BEGIN
      -- SET NOCOUNT ON added to prevent extra result sets from
      -- interfering with SELECT statements.
	  -- EXEC [DataStitching].[DataStitchingLoad_DML] @ServerName = 'NDESQL04',@DatabaseName = 'STG_MART_NDE', @SchemaName = 'dbo', @TableName = 'TCS_CURRENT_CERTIFICATES'
	  --c
      SET NOCOUNT ON;

      BEGIN TRY
            BEGIN TRANSACTION;

                  DECLARE
                  @OperationType varchar(100) = 'DML'
                  ,@Status VARCHAR(100) = 'Succeed'
                  ,@Starttime datetime2  = getdate()
                  --,@EndTime datetime2  = getdate()
				  --,@Timestamp datetime2  = format(getdate(),'MM/dd/yyyy,HH:mm:ss')
				  ,@Timestamp VARCHAR(MAX)  = format(getdate(),'MMddyyyyHHmmss')
                  ,@TargetName varchar(1000) = @ServerName + '_' + @DatabaseName + '_' + @SchemaName + '.' + @TableName  
                  ,@SourceName varchar(1000) = @ServerName + '.' + @DatabaseName + '.' + @SchemaName + '.' + @TableName
				  --,@BackUpName varchar(1000) = @ServerName + '.' + @DatabaseName + '.' + @SchemaName + '.' + @TableName + '_format(getdate(),''MM/dd/yyyy,HH:mm:ss'')' 
                  ,@TargetSchema varchar(1000) = @ServerName + '_' + @DatabaseName + '_' + @SchemaName
				  ,@TbCREATION VARCHAR(MAX) = @TableCreation

				DROP TABLE IF EXISTS ##Columnlist
				CREATE TABLE [##Columnlist] (
				ServerName varchar(500)
				,TableCatalog varchar(500)
				,TableSchema varchar(100)
				,TableName varchar(500)
				,ColumnName varchar(500))
			DECLARE @Columnlist NVARCHAR(MAX) = 
			+' INSERT INTO [##Columnlist] ('
			+' ServerName'
			+' ,TableCatalog'
			+' ,TableSchema'
			+' ,TableName'
			+' ,ColumnName)'
			+ ' SELECT ''' + @Servername+'''as Servername,'''+ @Databasename +''' as Databasename,'+ 's.name AS schema_name,t.name AS table_Name ,c.name AS column_Name'
			+ ' FROM ' + @Servername + '.' + @Databasename  + '.sys.tables t'
			+ ' JOIN ' + @Servername + '.' + @Databasename + '.sys.schemas s ON t.schema_id = s.schema_id'
			+ ' JOIN ' + @Servername + '.' + @Databasename + '.sys.columns c ON t.object_id = c.object_id'
			+ ' WHERE s.name = '''+ @Schemaname
			+ ''' and t.name = ''' + @tablename
			+ ''' ORDER BY s.name, t.name, c.name'

			DECLARE @Columnlist_print VARCHAR(MAX) = 
			+' INSERT INTO [##Columnlist] ('
			+' ServerName'
			+' ,TableCatalog'
			+' ,TableSchema'
			+' ,TableName'
			+' ,ColumnName)'
			+ ' SELECT ''' + @Servername+'''as Servername,'''+ @Databasename +''' as Databasename,'+ 's.name AS schema_name,t.name AS table_Name ,c.name AS column_Name'
			+ ' FROM ' + @Servername + '.' + @Databasename  + '.sys.tables t'
			+ ' JOIN ' + @Servername + '.' + @Databasename + '.sys.schemas s ON t.schema_id = s.schema_id'
			+ ' JOIN ' + @Servername + '.' + @Databasename + '.sys.columns c ON t.object_id = c.object_id'
			+ ' WHERE s.name = '''+ @Schemaname
			+ ''' and t.name = ''' + @tablename
			+ ''' ORDER BY s.name, t.name, c.name'

                  --PRINT @Columnlist_print;
                  EXECUTE sp_executesql @Columnlist;
				  --DECLARE @ColumnName TABLE (column_list nvarchar(max))
				  --INSERT INTO @ColumnName
				  --SELECT ColumnName FROM ##Columnlist;

				  DECLARE @ALLColumnList varchar(max)
				  SET @ALLColumnList = (
  				  SELECT STUFF((SELECT ',[' + ColumnName + ']' + CHAR(10)
                  FROM [##Columnlist] x1
                  WHERE x1.TableName = x2.TableName
                  FOR XML PATH ('')), 1, 1,'') AS MergeOnColumns
                  FROM [##Columnlist] x2
                  GROUP BY TableName);

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
                  + ' TRUNCATE TABLE ' + @TargetName
				  + ' SET IDENTITY_INSERT ' + @TargetName + ' ON;' --3. exist: truncate and then insert into then backup
                  + ' INSERT INTO ' + @TargetName + ' (' + @ALLColumnList + ')'
                  + ' SELECT ' + @ALLColumnList + ' FROM ' + @SourceName
				  + ' SET IDENTITY_INSERT ' + @TargetName + ' OFF;'
				  + ' USE NDE_DataLake_Backup'  --4. check schema is in backup db or not
				  + ' IF NOT EXISTS ('
				  + ' SELECT schema_name ' 
				  + ' FROM information_schema.schemata'
				  + ' WHERE schema_name = ''' +@Targetschema + ''' ) '
				  + ' BEGIN'
				  + ' EXEC sp_executesql N''' + 'CREATE SCHEMA ' + @Targetschema -- 4.schema doesn't exist, create schema
				  + ''' END'
				  --+ ' SET IDENTITY_INSERT ' + @TargetName + ' ON;'
				  + ' SELECT * INTO ' + @Servername + '_' + @Databasename + '_' + @Schemaname + '.' + @TableName + '_'+ @Timestamp --5.backup to backup db
				  + ' FROM ' + @Servername + '.' + @Databasename + '.' + @Schemaname + '.' + @TableName
				  --+ ' SET IDENTITY_INSERT ' + @TargetName + ' OFF;'
				  + ' USE NDE_Datalake'
                  + ' PRINT(' + '1' +')'
                  + ' END'
                  + ' ELSE'
                  + ' BEGIN'
				  + ' ' + @TbCREATION  --6. if table doesn't in target, create tb in target by using cmd then insert then backup
                  + ' SET IDENTITY_INSERT ' + @TargetName + ' ON;'
                  + ' INSERT INTO ' + @TargetName + ' (' + @ALLColumnList + ')'
                  + ' SELECT ' + @ALLColumnList + ' FROM ' + @SourceName
				  + ' SET IDENTITY_INSERT ' + @TargetName + ' OFF;'
				  + ' USE NDE_DataLake_Backup'
				  + ' IF NOT EXISTS ('
				  + ' SELECT schema_name '
				  + ' FROM information_schema.schemata'
				  + ' WHERE schema_name = ''' +@Targetschema + ''' ) '
				  + ' BEGIN'
				  + ' EXEC sp_executesql N''' + 'CREATE SCHEMA ' + @Targetschema 
				  + ''' END'
				  --+ ' SET IDENTITY_INSERT ' + @TargetName + ' ON;'
				  + ' SELECT * INTO ' + @Servername + '_' + @Databasename + '_' + @Schemaname + '.' + @TableName + '_'+ @Timestamp
				  + ' FROM ' + @Servername + '.' + @Databasename + '.' + @Schemaname + '.' + @TableName
				  --+ ' SET IDENTITY_INSERT ' + @TargetName + ' OFF;'
				  + ' USE NDE_Datalake' 
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
                  + ' TRUNCATE TABLE ' + @TargetName
				  + ' SET IDENTITY_INSERT ' + @TargetName + ' ON;' --3. exist: truncate and then insert into then backup
                  + ' INSERT INTO ' + @TargetName + ' (' + @ALLColumnList + ')'
                  + ' SELECT ' + @ALLColumnList + ' FROM ' + @SourceName
				  + ' SET IDENTITY_INSERT ' + @TargetName + ' OFF;'
				  + ' USE NDE_DataLake_Backup'  --4. check schema is in backup db or not
				  + ' IF NOT EXISTS ('
				  + ' SELECT schema_name ' 
				  + ' FROM information_schema.schemata'
				  + ' WHERE schema_name = ''' +@Targetschema + ''' ) '
				  + ' BEGIN'
				  + ' EXEC sp_executesql N''' + 'CREATE SCHEMA ' + @Targetschema -- 4.schema doesn't exist, create schema
				  + ''' END'
				  --+ ' SET IDENTITY_INSERT ' + @TargetName + ' ON;'
				  + ' SELECT * INTO ' + @Servername + '_' + @Databasename + '_' + @Schemaname + '.' + @TableName + '_'+ @Timestamp --5.backup to backup db
				  + ' FROM ' + @Servername + '.' + @Databasename + '.' + @Schemaname + '.' + @TableName
				  --+ ' SET IDENTITY_INSERT ' + @TargetName + ' OFF;'
				  + ' USE NDE_Datalake'
                  + ' PRINT(' + '1' +')'
                  + ' END'
                  + ' ELSE'
                  + ' BEGIN'
				  + ' ' + @TbCREATION  --6. if table doesn't in target, create tb in target by using cmd then insert then backup
                  + ' SET IDENTITY_INSERT ' + @TargetName + ' ON;'
                  + ' INSERT INTO ' + @TargetName + ' (' + @ALLColumnList + ')'
                  + ' SELECT ' + @ALLColumnList + ' FROM ' + @SourceName
				  + ' SET IDENTITY_INSERT ' + @TargetName + ' OFF;'
				  + ' USE NDE_DataLake_Backup'
				  + ' IF NOT EXISTS ('
				  + ' SELECT schema_name '
				  + ' FROM information_schema.schemata'
				  + ' WHERE schema_name = ''' +@Targetschema + ''' ) '
				  + ' BEGIN'
				  + ' EXEC sp_executesql N''' + 'CREATE SCHEMA ' + @Targetschema 
				  + ''' END'
				  --+ ' SET IDENTITY_INSERT ' + @TargetName + ' ON;'
				  + ' SELECT * INTO ' + @Servername + '_' + @Databasename + '_' + @Schemaname + '.' + @TableName + '_'+ @Timestamp
				  + ' FROM ' + @Servername + '.' + @Databasename + '.' + @Schemaname + '.' + @TableName
				  --+ ' SET IDENTITY_INSERT ' + @TargetName + ' OFF;'
				  + ' USE NDE_Datalake' 
                  + ' PRINT(' + '2' +')'
                  + ' END'
                  PRINT @tsql_print;
                  EXECUTE sp_executesql @tsql;
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
