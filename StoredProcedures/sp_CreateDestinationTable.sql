CREATE PROCEDURE [sp_CreateDestinationTable]
	@SourceTableId		INT,
	@DestinationId		INT,
	@DropExisting		BIT
AS
BEGIN
	
	DECLARE	@Sql	NVARCHAR(MAX) = ''

	DECLARE @TableName			SYSNAME
	DECLARE @SourceDatabase		SYSNAME
	DECLARE @SourceTableString	SYSNAME
	DECLARE @DestinationDB		SYSNAME
	DECLARE @TempString			NVARCHAR(MAX)

	DECLARE @ErrorMessage NVARCHAR(4000)

	SELECT @TableName = [TableName] FROM [SourceTables] WITH (NOLOCK) 
		WHERE ([TableId] = @SourceTableId)

	SELECT @DestinationDB = [DatabaseName] FROM [DataDestinations] WITH (NOLOCK) 
		WHERE ([DataDestinationId] = @DestinationId)

	SELECT 
		@TableName = [TableName],
		@SourceDatabase = '[' + [ServerName] + '].[' + [DatabaseName] + ']',
		@SourceTableString = '[' + [ServerName] + '].[' + [DatabaseName] + ']..[' + [TableName] + ']' 
	FROM [SourceTables] ST WITH (NOLOCK) 
	INNER JOIN [DataSources] DS WITH (NOLOCK) ON (ST.[DataSourceId] = DS.[DataSourceId]) 
	WHERE (ST.[TableId] = @SourceTableId)

	IF (PATINDEX('%.%', @TableName) > 0)
	BEGIN
		SET @SourceTableString = REPLACE(@SourceTableString, ']..[', '].[')
	END
	ELSE
		BEGIN
			SET @SourceTableString = REPLACE(@SourceTableString, ']..[', '].dbo.[')
		END

	--- Check if table exists
	SET @Sql = 'SELECT @TempString = [TABLE_NAME] FROM [' + @DestinationDB + '].[INFORMATION_SCHEMA].[TABLES] WHERE ([TABLE_NAME]=''' + @TableName + ''')'
	EXEC sp_ExecuteSQL 
		@Statement = @Sql,
		@Params = N'@TempString  NVARCHAR(MAX) OUTPUT',
		@TempString = @TempString OUTPUT

	IF ((@TempString IS NOT NULL) AND (@DropExisting = 0))
	BEGIN
		RETURN 0
	END
	ELSE
		IF ((@TempString IS NOT NULL) AND (@DropExisting = 1))
		BEGIN
			--- If table exists and we have been asked to drop it, drop it now
			SET @Sql = 'DROP TABLE [' + @DestinationDB + '].[' + @TableName + ']'
			EXEC sp_ExecuteSQL 
				@Statement = @Sql
		END

	--- Determine source table schema
	IF (OBJECT_ID('tempdb..#RemoteTableSchema') IS NOT NULL)
	BEGIN
		DROP TABLE #RemoteTableSchema
	END

	CREATE TABLE #RemoteTableSchema (
		ORDINAL_POSITION			int,
		COLUMN_NAME					sysname,
		DATA_TYPE					nvarchar(256),
		CHARACTER_MAXIMUM_LENGTH	int
	)

	SET @Sql = 'INSERT INTO #RemoteTableSchema (ORDINAL_POSITION,COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH) SELECT ORDINAL_POSITION,COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH FROM ' 
					+ @SourceDatabase + '.[INFORMATION_SCHEMA].[COLUMNS] WHERE ([TABLE_NAME] = ''' + @TableName + ''') ORDER BY ORDINAL_POSITION ASC'
	EXEC sp_ExecuteSQL 
		@Statement = @Sql

	IF ((SELECT COUNT(1) FROM #RemoteTableSchema) < 1)
	BEGIN
		--- No columns?
		
		SET @ErrorMessage = N'Error retrieving table schema from remote database.'
		RAISERROR (@ErrorMessage, 16, 1)

		RETURN 0
	END


	--- Create the local table schema
	DECLARE @Loop		INT
	DECLARE @LoopLimit	INT
	DECLARE @ColName	sysname
	DECLARE @DataType	nvarchar(256)
	DECLARE @ColLen		int

	SELECT @Loop = MIN(ORDINAL_POSITION), @LoopLimit = MAX(ORDINAL_POSITION) FROM #RemoteTableSchema

	IF ((@Loop > 0) AND (@LoopLimit > 0) AND (@LoopLimit > @Loop))
	BEGIN
		SET @Sql = 'CREATE TABLE [' + @DestinationDB + '].dbo.[' + @TableName + '] ('
		WHILE (@Loop <= @LoopLimit)
		BEGIN

			IF (EXISTS(SELECT 1 FROM #RemoteTableSchema WHERE ([ORDINAL_POSITION] = @Loop)))
			BEGIN
			
				IF ((@Loop > 1) AND (@Loop <= @LoopLimit))
				BEGIN
					SET @Sql = @Sql + ','
				END

				SELECT 
					@ColName = [COLUMN_NAME],
					@DataType = [DATA_TYPE],
					@ColLen = [CHARACTER_MAXIMUM_LENGTH]
				FROM #RemoteTableSchema WHERE ([ORDINAL_POSITION] = @Loop)

				SET @Sql = @Sql + 
							'[' + @ColName + '] [' + @DataType + ']'
				IF (@ColLen IS NOT NULL)
				BEGIN
					SET @Sql = @Sql + '(' + CONVERT(nvarchar, @ColLen) + ')'
				END
			END
		
			SET @Loop = @Loop + 1
		END
		SET @Sql = @Sql + ')'

		--- Debug
		--PRINT @Sql

		BEGIN TRY
			--- Create the table
			EXEC sp_ExecuteSql 
				@Statement = @Sql

			--- Enter details into DestinationTables table
			INSERT INTO [DestinationTables] VALUES(
				@DestinationId, 
				@TableName,
				1,	--- SyncInsert
				1,	--- SyncUpdate
				0,	--- SyncDelete
				0,	--- ConflictHonorDestination
				1,	--- IsEnabled
				NULL,
				NULL
			)

			DECLARE @DestinationTableId		INT
			SELECT @DestinationTableId = [TableId] FROM [DestinationTables] 
				WHERE ([DataDestinationId] = @DestinationId) AND ([TableName] = @TableName)

			INSERT INTO [TableSyncMap] VALUES (
				@SourceTableId,
				@DestinationTableId,
				1	--- IsEnabled
			)
		END TRY
		BEGIN CATCH
			SELECT @ErrorMessage = ERROR_MESSAGE()
			SET @ErrorMessage = N'Error creating destination table: '+@ErrorMessage

			RAISERROR (@ErrorMessage, 16, 1)
		END CATCH
	END

END
