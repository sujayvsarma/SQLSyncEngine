CREATE TABLE [DataSources]
(
	[DataSourceId]				INT				IDENTITY(1,1)		NOT NULL	PRIMARY KEY,
	[ServerName]				SYSNAME								NOT NULL,
	[DatabaseName]				SYSNAME								NOT NULL,
	[LinkedServerAccountName]	SYSNAME								NULL,
	[IsEnabled]					BIT									NOT NULL
)
