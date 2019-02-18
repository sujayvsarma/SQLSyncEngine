CREATE FUNCTION [fnScalarResolveFlags]
(
	@SourceFlag			BIT,
	@DestinationFlag	BIT,
	@ConflictFlag		TINYINT
)
RETURNS BIT
AS
BEGIN
	IF (@SourceFlag = @DestinationFlag)
	BEGIN
		RETURN @SourceFlag
	END
	ELSE
		BEGIN
			IF (@ConflictFlag = 1)
			BEGIN
				-- respect source
				RETURN @SourceFlag
			END
			ELSE IF (@ConflictFlag = 2)
				BEGIN
					-- respect destination
					RETURN @DestinationFlag
				END
		END

	RETURN @SourceFlag
END
