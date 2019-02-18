CREATE PROCEDURE [sp_GetSyncObjects]
	@SyncDataSourceId		INT
AS
BEGIN
	
	SELECT 
		DS.DataSourceId, DS.ServerName, DS.LinkedServerAccountName, DS.DatabaseName As SourceDatabaseName, 
		ST.TableId As SourceTableId, ST.TableName As SourceTableName, 
		ST.InsertKeyColumn, ST.InsertKeyColumnNULLFunction, ST.InsertTimestampColumn,
		ST.UpdateKeyColumn, ST.UpdateKeyColumnNULLFunction, ST.UpdateTimestampColumn, 
		ST.SyncInsert As SourceSyncInsert, ST.SyncUpdate As SourceSyncUpdate, ST.SyncDelete As SourceSyncDelete, ST.ConflictHonorSource, 
		DT.DataDestinationId, DD.DatabaseName As DestinationDatabaseName, DT.TableId As DestinationTableId, DT.TableName As DestinationTableName, 
		DT.SyncInsert As DestinationSyncInsert, DT.SyncUpdate As DestinationSyncUpdate, DT.SyncDelete As DestinationSyncDelete, DT.ConflictHonorDestination, 
		DT.InsertKeyLastValue As DestinationInsertKeyLastValue, DT.UpdateKeyLastValue As DestinationUpdateKeyLastValue
	FROM DataSources DS WITH (NOLOCK) 
	INNER JOIN SourceTables ST WITH (NOLOCK) ON ((ST.DataSourceId = DS.DataSourceId) AND (ST.IsEnabled = 1))
	INNER JOIN TableSyncMap TSM WITH (NOLOCK) ON ((TSM.IsEnabled = 1) AND (TSM.SourceTableId = ST.TableId)) 
	INNER JOIN DestinationTables DT WITH (NOLOCK) ON ((DT.IsEnabled = 1) AND (DT.TableId = TSM.DestinationTableId)) 
	INNER JOIN DataDestinations DD WITH (NOLOCK) ON ((DD.IsEnabled = 1) AND (DD.DataDestinationId = DT.DataDestinationId))
	WHERE ((DS.IsEnabled = 1) AND (DS.DataSourceId = @SyncDataSourceId))

END
