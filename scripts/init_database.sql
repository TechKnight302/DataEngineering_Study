--- STEP 1 — Run in master Database Connection
    
/*
=============================================================
Create DataWarehouse Database (Azure SQL Safe)
=============================================================
*/

IF DB_ID('DataWarehouse') IS NULL
BEGIN
    CREATE DATABASE DataWarehouse;
END
    
-- STEP 2 — Open NEW Connection to DataWarehouse

Database: DataWarehouse
/*
=============================================================
Create Medallion Architecture Schemas
=============================================================
*/

-- Bronze Schema (Raw Data)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');

-- Silver Schema (Cleaned Data)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');

-- Gold Schema (Business Layer)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');

