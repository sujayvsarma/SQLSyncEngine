CREATE PROCEDURE [sp_RunForAllDataSources]
AS
BEGIN
	
	CREATE TABLE #SyncSources(
		SyncSourceID				INT		IDENTITY(1, 1),
		DataSourceId				INT,
		SourceServerName			SYSNAME,
		SourceServerLogin			SYSNAME,
		SourceDatabase				SYSNAME,
		SourceTableId				INT,
		SourceTableName				SYSNAME,
		InsertKeyColumn				SYSNAME		NULL,
		InsertKeyColumnNULLFunction		NVARCHAR(255)	NULL,
		InsertTimestampColumn			SYSNAME		NULL,
		UpdateKeyColumn				SYSNAME		NULL,
		UpdateKeyColumnNULLFunction		NVARCHAR(255)	NULL,
		UpdateTimestampColumn			SYSNAME		NULL,
		SourceInserted				BIT,
		SourceUpdated				BIT,
		SourceDeleted				BIT,
		OnConflictHonorSource			BIT,
		DestinationId				INT,
		DestinationDatabase			SYSNAME,
		DestinationTableId			INT,
		DestinationTableName			SYSNAME,
		DestinationInserted			BIT,
		DestinationUpdated			BIT,
		DestinationDeleted			BIT,
		OnConflictHonorDestination		BIT,
		DestinationInsertKeyLastValue		NVARCHAR(MAX),
		DestinationUpdateKeyLastValue		NVARCHAR(MAX)
	)

	DECLARE @MinDataSourceId			INT = -1
	DECLARE @MaxDataSourceId			INT = -1
	DECLARE @LoopSourceId				INT

	DECLARE @DataSourceId				INT,
		@SourceServerName			SYSNAME,
		@SourceServerLogin			SYSNAME,
		@SourceDatabase				SYSNAME,
		@SourceTableId				INT,
		@SourceTableName			SYSNAME,
		@InsertKeyColumn			SYSNAME,			
		@InsertKeyColumnNULLFunction		NVARCHAR(255),	
		@InsertTimestampColumn			SYSNAME,			
		@UpdateKeyColumn			SYSNAME,			
		@UpdateKeyColumnNULLFunction		NVARCHAR(255),	
		@UpdateTimestampColumn			SYSNAME,			
		@SourceInserted				BIT,
		@SourceUpdated				BIT,
		@SourceDeleted				BIT,
		@OnConflictHonorSource			BIT,
		@DestinationId				INT,
		@DestinationDatabase			SYSNAME,
		@DestinationTableId			INT,
		@DestinationTableName			SYSNAME,
		@DestinationInserted			BIT,
		@DestinationUpdated			BIT,
		@DestinationDeleted			BIT,
		@OnConflictHonorDestination		BIT,
		@DestinationInsertKeyLastValue		NVARCHAR(MAX),
		@DestinationUpdateKeyLastValue		NVARCHAR(MAX)

	SELECT @MinDataSourceId = MIN([DataSourceId]), @MaxDataSourceId = MAX([DataSourceId]) FROM [DataSources] WITH (NOLOCK)

	IF ((@MinDataSourceId > 0) AND (@MaxDataSourceId > 0))
	BEGIN
		SET @LoopSourceId = @MinDataSourceId
		WHILE (@LoopSourceId <= @MaxDataSourceId)
		BEGIN
			IF (EXISTS(SELECT 1 FROM [DataSources] WITH (NOLOCK) WHERE ([DataSourceId]=@LoopSourceId)))
			BEGIN
				INSERT INTO #SyncSources 
					EXEC sp_GetSyncObjects @LoopSourceId
			END

			SET @LoopSourceId = @LoopSourceId + 1
		END
	END
	ELSE
		BEGIN
			--- No sources defined
			RETURN 0
		END

	-- Debug
	--SELECT * FROM #SyncSources

	-- Run the jobs
	SELECT @MinDataSourceId = MIN([SyncSourceID]), @MaxDataSourceId = MAX([SyncSourceID]) FROM [#SyncSources] WITH (NOLOCK)

	SET @LoopSourceId = @MinDataSourceId
	WHILE (@LoopSourceId <= @MaxDataSourceId)
	BEGIN
		IF (EXISTS(SELECT 1 FROM #SyncSources WITH (NOLOCK) WHERE ([SyncSourceID]=@LoopSourceId)))
			BEGIN
				
				SELECT 
					@DataSourceId = DataSourceId,	
					@SourceServerName = SourceServerName,	
					@SourceServerLogin = SourceServerLogin,
					@SourceDatabase = SourceDatabase,			
					@SourceTableId = SourceTableId,		
					@SourceTableName = SourceTableName,	
					@InsertKeyColumn = InsertKeyColumn,
					@InsertKeyColumnNULLFunction = InsertKeyColumnNULLFunction,
					@InsertTimestampColumn = InsertTimestampColumn,
					@UpdateKeyColumn = UpdateKeyColumn,
					@UpdateKeyColumnNULLFunction = UpdateKeyColumnNULLFunction,
					@UpdateTimestampColumn = UpdateTimestampColumn,
					@SourceInserted = SourceInserted,			
					@SourceUpdated = SourceUpdated,		
					@SourceDeleted = SourceDeleted,
					@OnConflictHonorSource = OnConflictHonorSource,
					@DestinationId = DestinationId,	
					@DestinationDatabase = DestinationDatabase,	
					@DestinationTableId = DestinationTableId,
					@DestinationTableName = DestinationTableName,	
					@DestinationInserted = DestinationInserted,	
					@DestinationUpdated = DestinationUpdated,	
					@DestinationDeleted = DestinationDeleted,
					@OnConflictHonorDestination = OnConflictHonorDestination,
					@DestinationInsertKeyLastValue = DestinationInsertKeyLastValue,
					@DestinationUpdateKeyLastValue = DestinationUpdateKeyLastValue
				FROM #SyncSources WITH (NOLOCK) 
				WHERE ([SyncSourceID]=@LoopSourceId)

				-- Debug
				SELECT * FROM #SyncSources WITH (NOLOCK) WHERE ([SyncSourceID]=@LoopSourceId)

				EXEC [sp_SyncObject] 
						@DataSourceId,	
						@SourceServerName,	
						@SourceServerLogin,
						@SourceDatabase,			
						@SourceTableId,		
						@SourceTableName,	
						@InsertKeyColumn,
						@InsertKeyColumnNULLFunction,
						@InsertTimestampColumn,
						@UpdateKeyColumn,
						@UpdateKeyColumnNULLFunction,
						@UpdateTimestampColumn,
						@SourceInserted,			
						@SourceUpdated,		
						@SourceDeleted,
						@OnConflictHonorSource,
						@DestinationId,	
						@DestinationDatabase,	
						@DestinationTableId,
						@DestinationTableName,	
						@DestinationInserted,	
						@DestinationUpdated,	
						@DestinationDeleted,
						@OnConflictHonorDestination,
						@DestinationInsertKeyLastValue,
						@DestinationUpdateKeyLastValue
			END

		SET @LoopSourceId = @LoopSourceId + 1
	END


	RETURN 0
END