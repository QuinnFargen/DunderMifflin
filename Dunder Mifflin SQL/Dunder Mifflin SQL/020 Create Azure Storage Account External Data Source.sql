
-- Helpful resources
-- https://learn.microsoft.com/en-us/sql/relational-databases/import-export/examples-of-bulk-access-to-data-in-azure-blob-storage?view=sql-server-ver16
-- https://www.sqlshack.com/use-bulk-insert-import-data-locally-azure/

CREATE MASTER KEY 
ENCRYPTION BY PASSWORD='Av3ry$3cur3PassW0rd';

CREATE DATABASE SCOPED CREDENTIAL azurecred  
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'secret';

CREATE EXTERNAL DATA SOURCE dundermifflinsa
WITH 
(
    TYPE = BLOB_STORAGE,
    LOCATION = 'https://[Azure Storage Account].blob.core.windows.net',
    CREDENTIAL = azurecred
);
