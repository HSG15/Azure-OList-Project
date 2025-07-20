CREATE SCHEMA gold

CREATE VIEW gold.final
as 
SELECT * FROM
    OPENROWSET(
        BULK 'https://hsgolistecommstorage.dfs.core.windows.net/olist-data/silver/olist_dataset/',
        FORMAT = 'PARQUET'
    ) AS result1

-- Now all columns should be visible in the olist-db > Views > gold.final > Columns

SELECT COUNT(*) FROM gold.final; --118434
SELECT * FROM gold.final;

select count(*)
from gold.final
where order_status = 'delivered'