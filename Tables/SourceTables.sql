CREATE TABLE [SourceTables]
(
	[TableId]						INT				IDENTITY(1,1)		NOT NULL	PRIMARY KEY,
	[DataSourceId]					INT									NOT NULL,
	[TableName]						SYSNAME								NOT NULL,
	[SyncInsert]					BIT									NOT NULL,
	[SyncUpdate]					BIT									NOT NULL,
	[SyncDelete]					BIT									NOT NULL,
	[ConflictHonorSource]			TINYINT								NOT NULL,
	[InsertKeyColumn]				SYSNAME								NULL,
	[InsertKeyColumnNULLFunction]	NVARCHAR(255)						NULL,
	[InsertTimestampColumn]			SYSNAME								NULL,
	[UpdateKeyColumn]				SYSNAME								NULL,
	[UpdateKeyColumnNULLFunction]	NVARCHAR(255)						NULL,
	[UpdateTimestampColumn]			SYSNAME								NULL,
	[IsEnabled]						BIT									NOT NULL
)
