CREATE PROCEDURE [sp_SyncObject]
	@DataSourceId			INT,
	@SourceServerName		SYSNAME,
	@SourceServerLogin		SYSNAME,
	@SourceDatabase			SYSNAME,
	@SourceTableId			INT,
	@SourceTableName		SYSNAME,
	@InsertKeyColumn		SYSNAME,			
	@InsertKeyColumnNULLFunction	NVARCHAR(255),	
	@InsertTimestampColumn		SYSNAME,			
	@UpdateKeyColumn		SYSNAME,			
	@UpdateKeyColumnNULLFunction	NVARCHAR(255),	
	@UpdateTimestampColumn		SYSNAME,
	@SourceInserted			BIT,
	@SourceUpdated			BIT,
	@SourceDeleted			BIT,
	@OnConflictHonorSource		BIT,
	@DestinationId			INT,
	@DestinationDatabase		SYSNAME,
	@DestinationTableId		INT,
	@DestinationTableName		SYSNAME,
	@DestinationInserted		BIT,
	@DestinationUpdated		BIT,
	@DestinationDeleted		BIT,
	@OnConflictHonorDestination	BIT,
	@DestinationInsertKeyLastValue	NVARCHAR(MAX),
	@DestinationUpdateKeyLastValue	NVARCHAR(MAX)
AS
BEGIN
	
	DECLARE		@DynamicSQL			NVARCHAR(MAX)
	DECLARE		@ErrorMessage			NVARCHAR(4000)
	DECLARE		@Message			NVARCHAR(MAX)

	DECLARE		@TempTableName			SYSNAME	= NULL
	DECLARE		@FQSourceTableName		SYSNAME = @SourceTableName
	DECLARE		@FQDestinationTableName		SYSNAME = @DestinationTableName
	DECLARE		@NeedInserted			BIT = 1
	DECLARE		@NeedUpdated			BIT = 1
	DECLARE		@RemoveRemoteDeleted		BIT = 1
	DECLARE		@ConflictHonor			TINYINT = 1

	DECLARE		@IgnoreAllKeys			BIT	= 0

	DECLARE		@InsertKeyStartValue		NVARCHAR(MAX) = NULL
	DECLARE		@UpdateKeyStartValue		NVARCHAR(MAX) = NULL
	DECLARE		@InsertKeyEndValue		NVARCHAR(MAX) = NULL
	DECLARE		@UpdateKeyEndValue		NVARCHAR(MAX) = NULL

	DECLARE		@InsertKeyValueDestMax		NVARCHAR(MAX) = NULL
	DECLARE		@UpdateKeyValueDestMax		NVARCHAR(MAX) = NULL
	DECLARE		@InsertTimeValueDestMax		NVARCHAR(MAX) = NULL
	DECLARE		@UpdateTimeValueDestMax		NVARCHAR(MAX) = NULL

	DECLARE		@InsertTimestampValue		DATETIME = '2000-01-01 00:00:00'
	DECLARE		@UpdateTimestampValue		DATETIME = '2000-01-01 00:00:00'

	DECLARE		@RunLoopInsertKeyStartValue	NVARCHAR(MAX) = NULL
	DECLARE		@RunLoopInsertKeyEndValue	NVARCHAR(MAX) = NULL
	DECLARE		@RunLoopInsertTimestampValue	DATETIME = '2000-01-01 00:00:00'

	DECLARE		@RunLoopUpdateKeyStartValue	NVARCHAR(MAX) = NULL
	DECLARE		@RunLoopUpdateKeyEndValue	NVARCHAR(MAX) = NULL
	DECLARE		@RunLoopUpdateTimestampValue	DATETIME = '2000-01-01 00:00:00'

	DECLARE		@UpdateStatement		NVARCHAR(MAX) = NULL

	DECLARE		@BatchCount			BIGINT = 0
	DECLARE		@FlagSyncEnd			BIT = 0

	DECLARE		@RETURNVALUE			INT = 0
	DECLARE		@ERR_OK				INT = 0
	DECLARE		@ERR_WARN			INT = -1
	DECLARE		@ERR_ERROR			INT = -2


	SET NOCOUNT ON

	BEGIN TRY
		

		--- Resolve source/destination flags
		IF ((@OnConflictHonorSource = 1) AND (@OnConflictHonorDestination = 0))
		BEGIN
			SET @ConflictHonor = 1
		END
		ELSE IF ((@OnConflictHonorSource = 0) AND (@OnConflictHonorDestination = 1))
			BEGIN
				SET @ConflictHonor = 2
			END
			ELSE IF (@OnConflictHonorSource = @OnConflictHonorDestination)
			BEGIN
				SET @ConflictHonor = 3
			END

		SET @NeedInserted = dbo.fnScalarResolveFlags(@SourceInserted, @DestinationInserted, @ConflictHonor)
		SET @NeedUpdated = dbo.fnScalarResolveFlags(@SourceUpdated, @DestinationUpdated, @ConflictHonor)
		SET @RemoveRemoteDeleted = dbo.fnScalarResolveFlags(@SourceDeleted, @DestinationDeleted, @ConflictHonor)
		
		--- Establish Linked servers if required
		IF (NOT EXISTS (SELECT 1 FROM sys.servers WHERE ([name] = @SourceServerName)))
		BEGIN
			BEGIN TRY
				EXEC master.dbo.sp_addlinkedserver @server = @SourceServerName, @srvproduct = N'SQL Server'
				EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname = @SourceServerName, @locallogin = @SourceServerLogin, @useself = N'True'
			END TRY
			BEGIN CATCH
				SELECT @ErrorMessage = ERROR_MESSAGE()
				SET @ErrorMessage = N'Error creating linked server: '+@ErrorMessage

				GOTO ERROREXIT
			END CATCH
		END

		--- Qualify table names
		IF (PATINDEX('%.%', @SourceTableName) > 0)
		BEGIN
			SET @FQSourceTableName = '[' + @SourceServerName + '].[' + @SourceDatabase + '].[' + @SourceTableName + ']'
		END
		ELSE
			BEGIN
				SET @FQSourceTableName = '[' + @SourceServerName + '].[' + @SourceDatabase + '].dbo.[' + @SourceTableName + ']'
			END
		IF (PATINDEX('%.%', @DestinationTableName) > 0)
		BEGIN
			SET @FQDestinationTableName = '[' + @DestinationDatabase + '].[' + @DestinationTableName + ']'
			SET @TempTableName = '[' + @DestinationDatabase + '].[' + 'tmp_' + @DestinationTableName + ']'
		END
		ELSE
			BEGIN
				SET @FQDestinationTableName = '[' + @DestinationDatabase + '].dbo.[' + @DestinationTableName + ']'
				SET @TempTableName = '[' + @DestinationDatabase + '].dbo.[' + 'tmp_' + @DestinationTableName + ']'
			END

		--- Create local copy of the remote table if required
		EXEC sp_CreateDestinationTable @SourceTableId, @DestinationId, 0
		
		--- If no keys are defined, we ignore all keys and do a trunc/load
		IF ((ISNULL(@InsertKeyColumn, '') = '') OR (ISNULL(@UpdateKeyColumn, '') = ''))
		BEGIN
			SET @IgnoreAllKeys = 1
		END

		IF (@IgnoreAllKeys = 1)
		BEGIN
			
			SET @DynamicSQL = 'INSERT INTO ' + @FQDestinationTableName + ' SELECT * FROM ' + @FQSourceTableName + ' WITH (NOLOCK)'

			EXEC sp_ExecuteSQL 
				@Statement = @DynamicSQL

			--- There is no need to update stats since there are no keys defined for which we can retrieve the MAX values

			--- Exit
			GOTO NORMALEXIT
		END
		
		--- Create cache table for the table we are about to sync up
		IF (OBJECT_ID(@TempTableName) IS NOT NULL)
		BEGIN
			SET @DynamicSQL = 'DROP TABLE ' + @TempTableName
			EXEC sp_ExecuteSQL 
				@Statement = @DynamicSQL
		END
		SET @DynamicSQL = 'SELECT TOP 0 * INTO ' + @TempTableName + ' FROM ' + @FQSourceTableName
		EXEC sp_ExecuteSQL 
			@Statement = @DynamicSQL

		IF (OBJECT_ID('tempdb..#UpdateTableSchema') IS NOT NULL)
		BEGIN
			DROP TABLE #UpdateTableSchema
		END

		CREATE TABLE #UpdateTableSchema (
			ORDINAL_POSITION			int,
			COLUMN_NAME					sysname
		)

		SET @DynamicSQL = 'INSERT INTO #UpdateTableSchema (ORDINAL_POSITION,COLUMN_NAME) SELECT ORDINAL_POSITION, COLUMN_NAME FROM [' + @DestinationDatabase + '].[INFORMATION_SCHEMA].[COLUMNS] WHERE ([TABLE_NAME] = ''' + @DestinationTableName + ''') ORDER BY ORDINAL_POSITION ASC'
	
		EXEC sp_ExecuteSQL 
			@Statement = @DynamicSQL

		SET @UpdateStatement = 'UPDATE PT SET '
		
		DECLARE @Loop		INT
		DECLARE @LoopLimit	INT
		DECLARE @ColName	sysname
		
		SELECT @Loop = MIN(ORDINAL_POSITION), @LoopLimit = MAX(ORDINAL_POSITION) FROM #UpdateTableSchema
		IF ((@Loop > 0) AND (@LoopLimit > 0) AND (@LoopLimit > @Loop))
		BEGIN
			WHILE (@Loop <= @LoopLimit)
			BEGIN
				IF (EXISTS(SELECT 1 FROM #UpdateTableSchema WHERE ([ORDINAL_POSITION] = @Loop)))
				BEGIN
			
					IF ((@Loop > 1) AND (@Loop <= @LoopLimit))
					BEGIN
						SET @UpdateStatement = @UpdateStatement + ','
					END

					SELECT 
						@ColName = [COLUMN_NAME]
					FROM #UpdateTableSchema WHERE ([ORDINAL_POSITION] = @Loop)

					SET @UpdateStatement = @UpdateStatement + 
								'PT.[' + @ColName + '] = TT.[' + @ColName + ']'
				END
		
				SET @Loop = @Loop + 1
			END
		END

		SET @UpdateStatement = @UpdateStatement + ' FROM ' + @FQDestinationTableName + ' PT ' + 
								'INNER JOIN ' + @TempTableName + ' TT ON (TT.[' + @UpdateKeyColumn + '] = PT.[' + @UpdateKeyColumn + ']) ' +
								'WHERE ((PT.[' + @InsertKeyColumn + ']=TT.[' + @InsertKeyColumn + ']) AND (PT.[' + @UpdateKeyColumn + '] < TT.[' + @UpdateKeyColumn + ']))'

		--- Get the values for different keys from source table
		SET @DynamicSQL = 
			'SELECT '
			+ '@InsertKeyStartValue = MIN([' + @InsertKeyColumn + ']), '
			+ '@InsertKeyEndValue = MAX([' + @InsertKeyColumn + ']), '
			+ '@UpdateKeyStartValue = MIN([' + @UpdateKeyColumn + ']), '
			+ '@UpdateKeyEndValue = MAX([' + @UpdateKeyColumn + ']), ' 
			+ ' FROM ' + @FQSourceTableName + ' WITH (NOLOCK)'

		EXEC sp_ExecuteSQL 
			@Statement = @DynamicSQL, 
			@Params = N'@InsertKeyStartValue NVARCHAR(MAX) OUTPUT, @InsertKeyEndValue NVARCHAR(MAX) OUTPUT, @UpdateKeyStartValue NVARCHAR(MAX) OUTPUT, @UpdateKeyEndValue NVARCHAR(MAX) OUTPUT', 
			@InsertKeyStartValue = @InsertKeyStartValue OUTPUT, 
			@InsertKeyEndValue = @InsertKeyEndValue OUTPUT,
			@UpdateKeyStartValue = @UpdateKeyStartValue OUTPUT,
			@UpdateKeyEndValue = @UpdateKeyEndValue OUTPUT

		--- If there are any values, get the timestamps
		IF (ISNULL(@InsertKeyStartValue, '') <> '')
		BEGIN
			SET @DynamicSQL = 
				'SELECT '
				+ '@InsertTimestampValue = [' + @InsertTimestampColumn + '] ' 
				+ ' FROM ' + @FQSourceTableName + ' WITH (NOLOCK) ' 
				+ ' WHERE ([' + @InsertKeyColumn + '] = ''' + @InsertKeyStartValue + ''') '

			EXEC sp_ExecuteSQL 
				@Statement = @DynamicSQL, 
				@Params = N'@InsertTimestampValue DATETIME OUTPUT', 
				@InsertTimestampValue = @InsertTimestampValue OUTPUT
		END

		IF (ISNULL(@UpdateKeyStartValue, '') <> '')
		BEGIN
			SET @DynamicSQL = 
				'SELECT '
				+ '@UpdateTimestampValue = [' + @UpdateTimestampColumn + '] ' 
				+ ' FROM ' + @FQSourceTableName + ' WITH (NOLOCK) ' 
				+ ' WHERE ([' + @UpdateKeyColumn + '] = ''' + @UpdateKeyStartValue + ''') '

			EXEC sp_ExecuteSQL 
				@Statement = @DynamicSQL, 
				@Params = N'@UpdateTimestampValue DATETIME OUTPUT', 
				@UpdateTimestampValue = @UpdateTimestampValue OUTPUT
		END

		--- Get the values for different keys from destination table
		SET @DynamicSQL = 
			'SELECT '
			+ '@InsertKeyValueDestMax = MAX([' + @InsertKeyColumn + ']), '
			+ '@UpdateKeyValueDestMax = MAX([' + @UpdateKeyColumn + ']) '

		IF (ISNULL(@InsertTimestampColumn, '') <> '')
		BEGIN
			SET @DynamicSQL = @DynamicSQL + ', @InsertTimeValueDestMax = MAX([' + @InsertTimestampColumn + ']) '
		END
		ELSE
			BEGIN
				SET @DynamicSQL = @DynamicSQL + ', @InsertTimeValueDestMax = NULL '
			END

		IF (ISNULL(@InsertTimestampColumn, '') <> '')
		BEGIN
			SET @DynamicSQL = @DynamicSQL + ', @UpdateTimeValueDestMax = MAX([' + @UpdateTimestampColumn + ']) '
		END
		ELSE
			BEGIN
				SET @DynamicSQL = @DynamicSQL + ', @UpdateTimeValueDestMax = NULL '
			END
		SET @DynamicSQL = @DynamicSQL + ' FROM ' + @FQDestinationTableName + ' WITH (NOLOCK)'

		EXEC sp_ExecuteSQL 
			@Statement = @DynamicSQL, 
			@Params = N'@InsertKeyValueDestMax NVARCHAR(MAX) OUTPUT, @UpdateKeyValueDestMax NVARCHAR(MAX) OUTPUT, @InsertTimeValueDestMax NVARCHAR(MAX) OUTPUT, @UpdateTimeValueDestMax NVARCHAR(MAX) OUTPUT', 
			@InsertKeyValueDestMax = @InsertKeyValueDestMax OUTPUT, 
			@UpdateKeyValueDestMax = @UpdateKeyValueDestMax OUTPUT,
			@InsertTimeValueDestMax = @InsertTimeValueDestMax OUTPUT,
			@UpdateTimeValueDestMax = @UpdateTimeValueDestMax OUTPUT
		
		
		SET @FlagSyncEnd = 0
		
		IF (ISNULL(@InsertKeyStartValue, '') > ISNULL(@InsertKeyValueDestMax, ''))
		BEGIN
			SET @RunLoopInsertKeyStartValue = ISNULL(@InsertKeyStartValue, '')
		END
		ELSE
			BEGIN
				SET @RunLoopInsertKeyStartValue = ISNULL(@InsertKeyValueDestMax, '')
			END
		
		IF (ISNULL(@UpdateKeyStartValue, '') > ISNULL(@UpdateKeyValueDestMax, ''))
		BEGIN
			SET @RunLoopUpdateKeyStartValue = ISNULL(@UpdateKeyStartValue, '')
		END
		ELSE
			BEGIN
				SET @RunLoopUpdateKeyStartValue = ISNULL(@UpdateKeyValueDestMax, '')
			END

		WHILE (@FlagSyncEnd <> 1)
		BEGIN
			
			--- Calculate range for this batch
			SET @DynamicSQL = 
				'SELECT '
					+ '@RunLoopInsertKeyEndValue = MIN([' + @InsertKeyColumn + ']) '
				+ ' FROM ' + @FQDestinationTableName + ' WITH (NOLOCK) ' 
				+ ' WHERE ([' + @InsertKeyColumn + '] > ''' + @RunLoopInsertKeyStartValue + ''')'

			EXEC sp_ExecuteSQL 
				@Statement = @DynamicSQL, 
				@Params = N'@RunLoopInsertKeyEndValue NVARCHAR(MAX) OUTPUT', 
				@RunLoopInsertKeyEndValue = @RunLoopInsertKeyEndValue OUTPUT

			SET @DynamicSQL = 
				'SELECT '
					+ '@RunLoopUpdateKeyEndValue = MIN([' + @UpdateKeyColumn + ']) '
				+ ' FROM ' + @FQDestinationTableName + ' WITH (NOLOCK) ' 
				+ ' WHERE ([' + @UpdateKeyColumn + '] > ''' + @RunLoopUpdateKeyStartValue + ''')'

			EXEC sp_ExecuteSQL 
				@Statement = @DynamicSQL, 
				@Params = N'@RunLoopUpdateKeyEndValue NVARCHAR(MAX) OUTPUT',
				@RunLoopUpdateKeyEndValue = @RunLoopUpdateKeyEndValue OUTPUT
			
			IF (ISNULL(@RunLoopInsertKeyEndValue, '') = '')
			BEGIN
				SET @RunLoopInsertKeyEndValue = @InsertKeyEndValue
			END

			IF (ISNULL(@RunLoopUpdateKeyEndValue, '') = '')
			BEGIN
				SET @RunLoopUpdateKeyEndValue = @UpdateKeyEndValue
			END

			IF ((ISNULL(@RunLoopInsertKeyStartValue, '') = '') OR (ISNULL(@RunLoopInsertKeyEndValue, '') = ''))
			BEGIN
				SET @FlagSyncEnd = 1
			END

			IF ((ISNULL(@RunLoopUpdateKeyStartValue, '') = '') OR (ISNULL(@RunLoopUpdateKeyEndValue, '') = ''))
			BEGIN
				SET @FlagSyncEnd = 1
			END

			IF (@FlagSyncEnd = 1)
			BEGIN
				BREAK
			END

			--- Get the batch data range into the temp table for inserts
			IF (@NeedInserted = 1)
			BEGIN
				SET @DynamicSQL = 'TRUNCATE TABLE ' + @TempTableName
				EXEC sp_ExecuteSQL 
					@Statement = @DynamicSQL


				SET @DynamicSQL = 'INSERT INTO ' + @TempTableName + ' SELECT * FROM ' + @FQSourceTableName + ' WITH (NOLOCK) ' + 
									'WHERE ([' + @InsertKeyColumn + '] BETWEEN ''' + @RunLoopInsertKeyStartValue + ''' AND ''' + @RunLoopInsertKeyEndValue + ''')'
			
				EXEC sp_ExecuteSQL 
					@Statement = @DynamicSQL


				--- Row count
				SET @BatchCount = 0
				SET @DynamicSQL = 'SELECT @BatchCount=COUNT(1) FROM ' + @TempTableName
			
				EXEC sp_ExecuteSQL 
					@Statement = @DynamicSQL, 
					@Params = N'@BatchCount BIGINT OUTPUT',
					@BatchCount = @BatchCount OUTPUT

				SET @BatchCount = ISNULL(@BatchCount, 0)
				IF (@BatchCount > 0)
				BEGIN
					--- Insert into destination table
					SET @DynamicSQL = 'INSERT INTO ' + @FQDestinationTableName + ' SELECT * FROM ' + @TempTableName + ' WITH (NOLOCK) '

					EXEC sp_ExecuteSQL 
						@Statement = @DynamicSQL

					-- Get max Update key value
					SET @DynamicSQL = 'SELECT @InsertKeyValueDestMax=MAX([' + @UpdateKeyColumn + ']) FROM ' + @FQDestinationTableName + ' WITH (NOLOCK)'
					EXEC sp_ExecuteSQL 
						@Statement = @DynamicSQL, 
						@Params = N'@InsertKeyValueDestMax NVARCHAR(MAX) OUTPUT',
						@InsertKeyValueDestMax = @InsertKeyValueDestMax OUTPUT

					SET @DynamicSQL = 'UPDATE [DestinationTables] SET [InsertKeyLastValue]= ''' + @InsertKeyValueDestMax + ''' WHERE ([DestinationTableId]=' + @DestinationTableId + ')'
					EXEC sp_ExecuteSQL 
						@Statement = @DynamicSQL
				END
			END

			IF (@NeedUpdated = 1)
			BEGIN
				SET @DynamicSQL = 'TRUNCATE TABLE ' + @TempTableName
				EXEC sp_ExecuteSQL 
					@Statement = @DynamicSQL


				SET @DynamicSQL = 'INSERT INTO ' + @TempTableName + ' SELECT * FROM ' + @FQSourceTableName + ' WITH (NOLOCK) ' + 
									'WHERE ([' + @UpdateKeyColumn + '] BETWEEN ''' + @RunLoopUpdateKeyStartValue + ''' AND ''' + @RunLoopUpdateKeyEndValue + ''')'
			
				EXEC sp_ExecuteSQL 
					@Statement = @DynamicSQL


				--- Row count
				SET @BatchCount = 0
				SET @DynamicSQL = 'SELECT @BatchCount=COUNT(1) FROM ' + @TempTableName
			
				EXEC sp_ExecuteSQL 
					@Statement = @DynamicSQL, 
					@Params = N'@BatchCount BIGINT OUTPUT',
					@BatchCount = @BatchCount OUTPUT

				SET @BatchCount = ISNULL(@BatchCount, 0)
				IF (@BatchCount > 0)
				BEGIN
					--- Insert into destination table
					SET @DynamicSQL = @UpdateStatement

					EXEC sp_ExecuteSQL 
						@Statement = @DynamicSQL

					-- Get max Update key value
					SET @DynamicSQL = 'SELECT @UpdateKeyValueDestMax=MAX([' + @UpdateKeyColumn + ']) FROM ' + @FQDestinationTableName + ' WITH (NOLOCK)'
					EXEC sp_ExecuteSQL 
						@Statement = @DynamicSQL, 
						@Params = N'@UpdateKeyValueDestMax NVARCHAR(MAX) OUTPUT',
						@UpdateKeyValueDestMax = @UpdateKeyValueDestMax OUTPUT

					SET @DynamicSQL = 'UPDATE [DestinationTables] SET [UpdateKeyLastValue]= ''' + @UpdateKeyValueDestMax + ''' WHERE ([DestinationTableId]=' + @DestinationTableId + ')'
					EXEC sp_ExecuteSQL 
						@Statement = @DynamicSQL
				END
			END

			SET @RunLoopInsertKeyStartValue = @RunLoopInsertKeyEndValue
			SET @RunLoopInsertKeyEndValue = NULL

			SET @RunLoopUpdateKeyStartValue = @RunLoopUpdateKeyEndValue
			SET @RunLoopUpdateKeyEndValue = NULL

		END -- End While Loop

	END TRY
	BEGIN CATCH
		SELECT @ErrorMessage = ERROR_MESSAGE() + ' at Line ' + CONVERT(NVARCHAR, ERROR_LINE())
		GOTO ERROREXIT
	END CATCH

	GOTO NORMALEXIT


ERROREXIT:
	SET @RETURNVALUE = @ERR_ERROR

NORMALEXIT:

	IF (OBJECT_ID('tempdb..#UpdateTableSchema') IS NOT NULL)
	BEGIN
		DROP TABLE #UpdateTableSchema
	END

	IF ((@TempTableName IS NOT NULL) AND (OBJECT_ID(@TempTableName) IS NOT NULL))
	BEGIN
		SET @DynamicSQL = 'DROP TABLE ' + @TempTableName
		EXEC sp_ExecuteSQL 
			@Statement = @DynamicSQL
	END

	SET NOCOUNT OFF

	IF (@RETURNVALUE = @ERR_ERROR)
	BEGIN
		RAISERROR (@ErrorMessage, 16, 1)
	END

	RETURN @RETURNVALUE

END
