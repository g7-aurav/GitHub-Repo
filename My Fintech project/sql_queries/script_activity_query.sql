-- Use this query in script activity under ForEach

SELECT COUNT(*) AS tgt_count
FROM OPENROWSET(
    BULK 'bronze/@{item().SchemaName}/@{item().TableName}/*.parquet',
    DATA_SOURCE = 'FintechDataExternal',
    FORMAT = 'PARQUET'
) AS rows;