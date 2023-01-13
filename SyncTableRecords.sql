CREATE PROCEDURE [Sync].[TableRecords] (
	 @SourceTableDatabase		VARCHAR(MAX)
	,@SourceTableName			VARCHAR(MAX)
	,@SourceTablePKColumnName	VARCHAR(MAX)
	,@TargetTableDatabase		VARCHAR(MAX)
	,@TargetTableName			VARCHAR(MAX)
	,@TargetTablePKColumnName	VARCHAR(MAX)
)

AS

PRINT 'Initializing...'

--------------------------------------------------------------------------------------------------------------
DECLARE @QualifiedSourceTableName AS VARCHAR(MAX) = CONCAT(@SourceTableDatabase, '.', @SourceTableName)
DECLARE @QualifiedTargetTableName AS VARCHAR(MAX) = CONCAT(@TargetTableDatabase, '.', @TargetTableName)
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ##SourceTableColumns
DROP TABLE IF EXISTS ##TargetTableColumns
DROP TABLE IF EXISTS #SharedColumns
DROP TABLE IF EXISTS ##UpdatedPopulation
--------------------------------------------------------------------------------------------------------------
-- Query variables
--------------------------------------------------------------------------------------------------------------

DECLARE @Select	AS VARCHAR(MAX) = 'SELECT c.name as ColumnName'
DECLARE @Into AS VARCHAR(MAX) = 'INTO'
DECLARE @TempTableName AS VARCHAR(MAX)
DECLARE @From AS VARCHAR(MAX) = 'FROM'
DECLARE @FromTable AS VARCHAR(16) = 'sys.columns as c'
DECLARE @Where AS VARCHAR(32) = 'WHERE c.object_id = Object_id('

DECLARE @Query AS VARCHAR(MAX)

--------------------------------------------------------------------------------------------------------------
-- Source Table Columns
--------------------------------------------------------------------------------------------------------------
PRINT 'Obtaining source table columns...'

SET @TempTableName = '##SourceTableColumns'

SET @Query = (
	@Select
	+ ' ' + @Into + ' ' + @TempTableName
	+ ' ' + @From + ' ' + @SourceTableDatabase + '.' + @FromTable
	+ ' ' + @Where + '''' + @QualifiedSourceTableName + '''' + ')'
)


EXEC (@Query)

--------------------------------------------------------------------------------------------------------------
-- Target Table Columns
--------------------------------------------------------------------------------------------------------------
PRINT 'Obtaining target table columns...'

SET @TempTableName = '##TargetTableColumns'

SET @Query = (
	@Select
	+ ' ' + @Into + ' ' + @TempTableName
	+ ' ' + @From + ' ' + @TargetTableDatabase + '.' + @FromTable
	+ ' ' + @Where + '''' + @QualifiedTargetTableName + '''' + ')'
)


EXEC (@Query)

--------------------------------------------------------------------------------------------------------------
-- Find Shared Table Columns between tables
--------------------------------------------------------------------------------------------------------------
PRINT 'Obtaining shared table columns...'

SELECT 
st.ColumnName

INTO #SharedColumns 
FROM ##SourceTableColumns AS st
JOIN ##TargetTableColumns AS tt
ON tt.ColumnName = st.ColumnName

PRINT 'Stpromg shared columns in variable...'

DECLARE @SharedColumns AS VARCHAR(MAX) = (SELECT STRING_AGG(ColumnName, ', ') FROM #SharedColumns)

--------------------------------------------------------------------------------------------------------------
-- Dynamically construct columns in query so EXCEPT statement always has the same columns for both tables 
-- Bonus! They are always in the same order, too
--------------------------------------------------------------------------------------------------------------
PRINT 'Obtaining updated population...'

SET @TempTableName = '##UpdatedPopulation'

SET @Query = (
	'SELECT u.*'
	+ ' INTO ' + @TempTableName
	+ ' FROM ('
	+ 'SELECT ' + @SharedColumns + ' FROM ' + @QualifiedSourceTableName
	+ ' EXCEPT '
	+ 'SELECT ' + @SharedColumns + ' FROM ' + @QualifiedTargetTableName
	+ ') as u'
)

EXEC (@Query)

DECLARE @TotalUpdatedPopulation AS INT = (SELECT COUNT(*) FROM ##UpdatedPopulation)

PRINT 'Located ' + CAST(@TotalUpdatedPopulation AS VARCHAR(MAX)) + ' updated records.'

IF @TotalUpdatedPopulation > 0
BEGIN

	----------------------------------------------------------------------------------------------------------
	-- Dynamic construction of MERGE statement
	----------------------------------------------------------------------------------------------------------

	DECLARE @UpdateQuery AS VARCHAR(MAX) = (
		SELECT 
		STRING_AGG(CONCAT('t.', ColumnName, ' = ', 's.', ColumnName), ', ') 
		FROM #SharedColumns
		WHERE ColumnName != @TargetTablePKColumnName
	)

	DECLARE @SourceInsertQuery AS VARCHAR(MAX) = (
		SELECT 
		STRING_AGG('s.' + ColumnName, ', ') 
		FROM #SharedColumns
	)


	SET @Query = (
		  ' SET IDENTITY_INSERT ' + @QualifiedTargetTableName + ' ON '
		+ ' MERGE ' + @QualifiedTargetTableName + ' AS t'
		+ ' USING ' + @QualifiedSourceTableName + ' AS s'
		+ ' ON t.' + @TargetTablePKColumnName + ' = s.' + @SourceTablePKColumnName
		+ ' WHEN MATCHED THEN UPDATE SET ' 
		+	@UpdateQuery
		+ ' WHEN NOT MATCHED BY TARGET THEN '
		+ ' INSERT (' + @SharedColumns + ')'
		+ ' VALUES (' + @SourceInsertQuery + ')'
		+ ' WHEN NOT MATCHED BY SOURCE THEN DELETE;'
		+ ' SET IDENTITY_INSERT ' + @QualifiedTargetTableName + ' OFF'
	)

	PRINT 'Syncing table records...'
	EXEC (@Query)

END

--------------------------------------------------------------------------------------------------------------
-- Finally, select updated population to show all rows that were updated
--------------------------------------------------------------------------------------------------------------

SET @Query = (
	'SELECT IIF(tt.' + @TargetTablePKColumnName + ' IS NULL, 0, 1) AS NewRecordFlag, u.*'
	+ ' FROM ##UpdatedPopulation as u'
	+ ' LEFT JOIN ' + @QualifiedTargetTableName + ' as tt'
	+ ' ON tt.' + @TargetTablePKColumnName + ' = u.' + @TargetTablePKColumnName
)

EXEC (@Query)