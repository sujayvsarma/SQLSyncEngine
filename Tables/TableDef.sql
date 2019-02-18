CREATE TABLE [TableDef]
(
	[TableDefId]				INT				IDENTITY(1,1)		NOT NULL	PRIMARY KEY,
	[TableId]					INT									NOT NULL,
	[IsSource]					BIT									NOT NULL,
	[IsDestination]				BIT									NOT NULL,
	[ColumnName]				SYSNAME								NOT NULL,
	[SyncInsert]				BIT									NOT NULL,
	[SyncUpdate]				BIT									NOT NULL,
	[IsEnabled]					BIT									NOT NULL
)
