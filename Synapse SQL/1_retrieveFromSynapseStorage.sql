-- This is auto-generated code
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://hsgolistsynapsestorage.dfs.core.windows.net/hsgsynapsefilesystem/custForProject.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0'
    ) AS [result]
