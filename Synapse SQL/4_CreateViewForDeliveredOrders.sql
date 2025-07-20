create view gold.final_delivered_orders
AS
SELECT * FROM 
    OPENROWSET(
        BULK 'https://hsgolistecommstorage.dfs.core.windows.net/olist-data/silver/olist_dataset/',
        FORMAT = 'PARQUET'
    ) AS result2
WHERE order_status = 'delivered'

SELECT * FROM gold.final_delivered_orders;