CREATE TABLE [DataDestinations]
(
	[DataDestinationId]			INT				IDENTITY(1,1)		NOT NULL	PRIMARY KEY,
	[DatabaseName]				SYSNAME								NOT NULL,
	[IsEnabled]					BIT									NOT NULL
)
