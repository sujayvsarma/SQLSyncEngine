CREATE PROCEDURE [sp_CreateLinkedServer]
	@ServerName	SYSNAME,
	@LoginAccount	SYSNAME	= NULL
AS
BEGIN
	SET NOCOUNT ON

	IF (NOT EXISTS(SELECT 1 FROM sys.servers WHERE ([name] = @ServerName)))
	BEGIN 
		BEGIN TRY
			EXEC master.dbo.sp_addlinkedserver @server = @ServerName, @srvproduct=N'SQL Server'

			IF (@LoginAccount IS NOT NULL)
			BEGIN
				EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname = @ServerName, @locallogin = @LoginAccount, @useself = N'True'
			END
			ELSE
				BEGIN
					EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname=@ServerName, @useself=N'True', @locallogin=NULL, @rmtuser=NULL, @rmtpassword=NULL
				END
		END TRY
		BEGIN CATCH
			DECLARE @ErrorMessage NVARCHAR(4000)
			SELECT @ErrorMessage = ERROR_MESSAGE()
			SET @ErrorMessage = N'Error creating linked server: '+@ErrorMessage

			RAISERROR (@ErrorMessage, 16, 1)
		END CATCH
	END

	SET NOCOUNT OFF

	RETURN 0
END