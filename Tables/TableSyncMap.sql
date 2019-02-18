CREATE TABLE [TableSyncMap]
(
	[TableSyncMapId]			INT				IDENTITY(1,1)		NOT NULL	PRIMARY KEY,
	[SourceTableId]				INT									NOT NULL,
	[DestinationTableId]		INT									NOT NULL,
	[IsEnabled]					BIT
)
