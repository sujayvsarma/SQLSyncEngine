CREATE TABLE [DestinationTables]
(
	[TableId]				INT				IDENTITY(1,1)		NOT NULL	PRIMARY KEY,
	[DataDestinationId]			INT							NOT NULL,
	[TableName]				SYSNAME							NOT NULL,
	[SyncInsert]				BIT							NOT NULL,
	[SyncUpdate]				BIT							NOT NULL,
	[SyncDelete]				BIT							NOT NULL,
	[ConflictHonorDestination]		TINYINT							NOT NULL,
	[IsEnabled]				BIT							NOT NULL,
	[InsertKeyLastValue]			NVARCHAR(255)						NULL,
	[UpdateKeyLastValue]			NVARCHAR(255)						NULL
)
