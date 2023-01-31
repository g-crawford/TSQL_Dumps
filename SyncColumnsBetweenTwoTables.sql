CREATE PROCEDURE [Sync].[TableColumns] (
	 @SourceTableDatabase	AS VARCHAR(MAX)
	,@SourceTableName		AS VARCHAR(MAX)
	,@TargetTableDatabase	AS VARCHAR(MAX)
	,@TargetTableName		AS VARCHAR(MAX)

)

AS

-----------------------------------------------------------------------------------------------------------------------
DECLARE @QualifiedSourceTableName	AS VARCHAR(MAX) = (@SourceTableDatabase + '.' + @SourceTableName)
DECLARE @QualifiedTargetTableName	AS VARCHAR(MAX) = (@TargetTableDatabase + '.' + @TargetTableName)
-----------------------------------------------------------------------------------------------------------------------
-- Query construction variables
-----------------------------------------------------------------------------------------------------------------------
DECLARE @Select	AS VARCHAR(MAX) = '
	SELECT 
	c.name										as ColumnName
	,UPPER(t.name)								as DataType
	,CASE
		WHEN t.name IN (''varchar'', ''nvarchar'') THEN
			CASE 
				WHEN c.max_length = -1 THEN ''MAX''
				ELSE CAST((c.max_length / 2) AS VARCHAR(MAX))
			END
		ELSE NULL
	END AS MaxCharacterLength
	,c.is_nullable								as IsNullable
	,CAST(c.precision AS VARCHAR(MAX))			as Precision
	,CAST(c.scale AS VARCHAR(MAX))			as [Scale]
'
DECLARE @Into AS VARCHAR(4) = 'INTO' 
DECLARE @TempTableName AS VARCHAR(MAX)
DECLARE @From AS VARCHAR(4) = 'FROM'
DECLARE @FromTable AS VARCHAR(16) = 'sys.columns as c'
DECLARE @Join AS VARCHAR(54) = 'JOIN sys.types as t ON c.user_type_id = t.user_type_id' 
DECLARE @Where AS VARCHAR(32) = 'WHERE c.object_id = Object_id('''

DECLARE @Query AS VARCHAR(MAX)

-----------------------------------------------------------------------------------------------------------------------
-- Drop existing Temp tables
-----------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ##SourceTableColumns
DROP TABLE IF EXISTS ##TargetTableColumns
DROP TABLE IF EXISTS #MismatchedColumns
DROP TABLE IF EXISTS #LoopingTable

-----------------------------------------------------------------------------------------------------------------------
-- Construct Query and populate both Temp Tables
-----------------------------------------------------------------------------------------------------------------------
SET @TempTableName = '##SourceTableColumns'
-----------------------------------------------------------------------------------------------------------------------
SET @Query  = (
	@Select
	+ ' ' + @Into + ' ' + @TempTableName
	+ ' ' + @From + ' ' + @SourceTableDatabase + '.'  + @FromTable
	+ ' ' + @Join
	+ ' ' + @Where + @QualifiedSourceTableName + ''')'
)

EXEC (@Query)

-----------------------------------------------------------------------------------------------------------------------
SET @TempTableName = '##TargetTableColumns'
-----------------------------------------------------------------------------------------------------------------------
SET @Query  = (
	@Select
	+ ' ' + @Into + ' ' + @TempTableName
	+ ' ' + @From + ' ' + @TargetTableDatabase + '.'  + @FromTable
	+ ' ' + @Join
	+ ' ' + @Where + @QualifiedTargetTableName + ''')'
)

EXEC (@Query)

-----------------------------------------------------------------------------------------------------------------------
-- Identify Mismatched Columns and Construct Query for each column
-----------------------------------------------------------------------------------------------------------------------
SELECT
ROW_NUMBER() OVER (ORDER BY mc.ColumnName) AS RowID
,mc.ColumnName
,IIF(tt.ColumnName IS NULL, 0, 1) as ExistingColumn
,(
	IIF(tt.ColumnName IS NOT NULL, 'ALTER TABLE' + ' ' + @QualifiedTargetTableName, '')
	+ ' ' + IIF(tt.ColumnName IS NOT NULL, 'MODIFY COLUMN', '')
	+ ' ' + st.ColumnName
	+ ' ' + st.DataType
	+ ' ' + IIF(st.DataType IN ('VARCHAR', 'NVARCHAR', 'DECIMAL'), 
				'(' + IIF(st.DataType = 'DECIMAL', st.[Precision] + ', ' + st.[Scale], st.MaxCharacterLength) + ')',
				''
			)
	+ ' ' + IIF(st.IsNullable = 1, 'NULL', 'NOT NULL')
) AS Query

INTO #MismatchedColumns
FROM (
	SELECT * FROM ##SourceTableColumns

	EXCEPT 

	SELECT * FROM ##TargetTableColumns
) AS mc

LEFT JOIN ##SourceTableColumns as st
ON st.ColumnName = mc.ColumnName

LEFT JOIN ##TargetTableColumns as tt
ON tt.ColumnName = mc.ColumnName

DECLARE @TotalMismatchedCount AS INT = (SELECT COUNT(*) FROM #MismatchedColumns)

PRINT ('Located ' + CAST(@TotalMismatchedCount AS VARCHAR(MAX)) + ' mismatched columns.')


IF @TotalMismatchedCount > 0

BEGIN

	-------------------------------------------------------------------------------------------------------------------
	-- Add New columns (since multiple columns can be added at once, saving time of processing)
	-------------------------------------------------------------------------------------------------------------------

	DECLARE @NewColumnCount AS INT = (SELECT COUNT(*) FROM #MismatchedColumns WHERE ExistingColumn = 0)

	IF @NewColumnCount > 0

	BEGIN

		PRINT ('Adding ' + CAST(@NewColumnCount AS VARCHAR(MAX)) + ' new column(s)...')

		SET @Query = (
			'ALTER TABLE ' + @QualifiedTargetTableName
			+ ' ' + 'ADD' 
			+ ' ' + (
				SELECT
				STRING_AGG(mc.Query, ', ') as Query

				FROM #MismatchedColumns as mc

				WHERE 1=1
				AND mc.ExistingColumn = 0

				GROUP BY mc.ExistingColumn
			)
		)

		EXEC (@Query)

	END

	IF @NewColumnCount = 0

	BEGIN

		PRINT 'No new columns to add.'

	END

	-------------------------------------------------------------------------------------------------------------------
	-- Modify existing columns
	-------------------------------------------------------------------------------------------------------------------
	-- Create a loop table (so that #MismatchedColumns remains intact for the final select showing what changed)
	-- Loop through each mismatched column in loop table and execute its query to sync tables
	-------------------------------------------------------------------------------------------------------------------

	DECLARE @ModifiedColumnCount AS INT = (SELECT COUNT(*) FROM #MismatchedColumns WHERE ExistingColumn = 1) 

	IF @ModifiedColumnCount > 0

	BEGIN

		SELECT * INTO #LoopingTable FROM #MismatchedColumns WHERE ExistingColumn = 1

		DECLARE @Id AS INT

		DECLARE @Count AS INT = 1

		WHILE EXISTS (SELECT * FROM #LoopingTable)
		BEGIN

			PRINT ('Modifying row ' + CAST(@Count AS VARCHAR(MAX)) + ' of ' +  CAST(@ModifiedColumnCount AS VARCHAR(MAX)))

			SELECT TOP 1 @Id = RowID FROM #LoopingTable
			SET @Query = (SELECT Query FROM #LoopingTable WHERE RowID = @Count)

			PRINT ('Executing Query: ' + @Query)

			EXEC (@Query)

			DELETE #LoopingTable WHERE RowID = @Count

			SET @Count = @Count + 1

		END

	END

	IF @ModifiedColumnCount = 0

	BEGIN

		PRINT 'No new columns to add.'

	END


END

-----------------------------------------------------------------------------------------------------------------------
-- Finally, select all mismatched columns to show what was changed and how
-----------------------------------------------------------------------------------------------------------------------
SELECT
mc.*

FROM #MismatchedColumns AS mc

ORDER BY mc.RowID desc
