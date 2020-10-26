USE [NDE_DataLake]
GO
/****** Object:  StoredProcedure [DataStitching].[DataStitching_DDL_DROP]    Script Date: 9/3/2020 5:37:18 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [DataStitching].[DataStitchingLoad_DDL_DROP]
      -- Add the parameters for the stored procedure here
     @ServerName VARCHAR(500)
      ,@DatabaseName VARCHAR(500)
      ,@SchemaName VARCHAR(500)
      ,@TableName VARCHAR(500)
AS
BEGIN
      -- SET NOCOUNT ON added to prevent extra result sets from
      -- interfering with SELECT statements.
      SET NOCOUNT ON;

      BEGIN TRY
            BEGIN TRANSACTION;

                  DECLARE
                  @OperationType varchar(100) = 'DDL_DROP'
                  ,@Status VARCHAR(100) = 'Succeed'
                  ,@Starttime datetime2  = getdate()
                  --,@EndTime datetime2  = getdate()
				  ,@Timestamp VARCHAR(MAX)  = format(getdate(),'MMddyyyyHHmmss')
                  ,@TargetName varchar(1000) = @ServerName + '_' + @DatabaseName + '_' + @SchemaName + '.' + @TableName  
                  ,@SourceName varchar(1000) = @ServerName + '.' + @DatabaseName + '.' + @SchemaName + '.' + @TableName
				  --,@BackUpName varchar(1000) = @ServerName + '.' + @DatabaseName + '.' + @SchemaName + '.' + @TableName + '_format(getdate(),''MMddyyyyHHmmss'')' 
                  ,@TargetSchema varchar(1000) = @ServerName + '_' + @DatabaseName + '_' + @SchemaName;


		
			DECLARE @tsql nvarchar(max) = 
			+ ' USE [NDE_DataLake_Backup]'
			+ ' SELECT * INTO ' + @Servername + '_' + @Databasename + '_' + @Schemaname + '.' + @TableName + '_'+ @Timestamp
			+ ' FROM ' + @Servername + '.' + @Databasename + '.' + @Schemaname + '.' + @TableName
			+ ' USE [NDE_Datalake]'
			+ ' DROP TABLE IF EXISTS ' + @TargetName
			+ ' PRINT(3)'

			DECLARE @tsql_print varchar(max) = 
			+ ' USE [NDE_DataLake_Backup]'
			+ ' SELECT * INTO ' + @Servername + '_' + @Databasename + '_' + @Schemaname + '.' + @TableName + '_'+ @Timestamp
			+ ' FROM ' + @Servername + '.' + @Databasename + '.' + @Schemaname + '.' + @TableName
			+ ' USE [NDE_Datalake]'
			+ ' DROP TABLE IF EXISTS ' + @TargetName
			+ ' PRINT(3)'

                  PRINT @tsql_print;
                  EXECUTE sp_executesql @tsql;

				  declare @EndTime datetime2  = getdate()
				   
				  INSERT INTO [DataStitching].[ExecutionLog3] 
                  (
				  [SourceServer],
				  [SourceDatabase],
				  [SourceSchema],
                  [SchemaName],
                  [TableName],
                  [Status], --Succeed/Failed
                  [OperationType],
                  --[TableRows],
                  --[SourceCount],
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
                  --0,
                  --@ActualCount,
                  @Starttime,
                  @EndTime,
                  GETDATE()
                  )
				  --print 'summery table insert?'
				  DECLARE @id INT 
				  SET @id = 0 
				  declare @BackupName varchar(max)
				  select  @BackupName = max(name) from [NDESQL21].[NDE_Datalake_Backup].sys.tables where name like '%'+@TableName+'%'

				  UPDATE [DataStitching].[DataStitching_Backup]  
				  SET @id = [SeqenceCount] = @id + 1
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
                  --[TableRows],
                  --[SourceCount],
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
                  --0,
                  --@ActualCount,
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