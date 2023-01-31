CREATE PROCEDURE [Get].TablePrimaryKey (
	 @TargetTableDatabase VARCHAR(MAX)
	,@TargetTableSchema VARCHAR(MAX)
	,@TargetTableName VARCHAR(MAX)
	,@PrimaryKey VARCHAR(MAX) OUTPUT
)

AS

DECLARE @QualifiedTableName AS VARCHAR(MAX) = (
	@TargetTableDatabase + '.' + @TargetTableSchema + '.[' + @TargetTableName + ']'
)

DECLARE @Query AS NVARCHAR(1000) = (
	'SELECT'
	+ ' @PK = c.name'
	+ ' FROM ' + @TargetTableDatabase + '.sys.columns AS c'
	+ ' JOIN ' + @TargetTableDatabase + '.sys.indexes AS i'
	+ ' ON i.object_id = c.object_id'
	+ ' AND i.index_id = c.column_id'
	+ ' WHERE 1=1'
	+ ' AND c.object_id = OBJECT_ID(''' + @QualifiedTableName + ''')'
	+ ' AND i.is_primary_key = 1'
)

EXECUTE sp_executesql 
@Query
,N'@PK NVARCHAR(1000) OUTPUT'
,@PK = @PrimaryKey OUTPUT