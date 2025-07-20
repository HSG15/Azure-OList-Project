-- This is auto-generated code
SELECT *
FROM
    OPENROWSET(
        BULK 'https://hsgolistecommstorage.dfs.core.windows.net/olist-data/silver/olist_dataset/',
        FORMAT = 'PARQUET'
    ) AS result1