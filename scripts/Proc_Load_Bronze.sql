/*
===============================================================================
Procedure Name : bronze.load_bronze
Author         : Prathik Shetty
Created Date   : 05-Mar-2026
Layer          : Bronze (Raw Data Ingestion)
Environment    : SQL Server Development Environment
===============================================================================

DESCRIPTION
-----------
This stored procedure performs a FULL REFRESH load of source CRM and ERP
datasets into the Bronze layer tables of the data warehouse.

The Bronze layer represents the raw ingestion layer where data is loaded
directly from external source files with minimal transformation.

For each source table, the procedure performs the following steps:

1. Remove existing data using TRUNCATE TABLE
2. Load fresh data from CSV files using BULK INSERT
3. Capture the number of rows loaded using @@ROWCOUNT
4. Log load execution details into bronze.load_log
5. Print execution progress messages for monitoring


DATA SOURCES
------------
CRM Source Files
- cust_info.csv
- prd_info.csv
- sales_details.csv

ERP Source Files
- CUST_AZ12.csv
- LOC_A101.csv
- PX_CAT_G1V2.csv


LOAD STRATEGY
-------------
Load Type  : Full Refresh
Method     : TRUNCATE + BULK INSERT
File Type  : CSV
Delimiter  : Comma (,)
Header Row : Skipped (FIRSTROW = 2)


LOGGING
-------
Execution details for each table load are recorded in:

    bronze.load_log

Captured information includes:
- Table Name
- Load Start Time
- Load End Time
- Rows Loaded
- Load Status (SUCCESS / FAILED)
- Error Message (if applicable)


ERROR HANDLING
--------------
The procedure uses TRY...CATCH for error handling and XACT_ABORT to ensure
automatic rollback on failure.

If any step fails:
- The entire transaction is rolled back
- Error details are logged
- The error is re-thrown to the calling process


TRANSACTION MANAGEMENT
----------------------
All table loads are executed within a single transaction to ensure
atomicity and data consistency.

BEGIN TRANSACTION
    -> Load all bronze tables
COMMIT TRANSACTION

If an error occurs:
ROLLBACK TRANSACTION


PERFORMANCE CONSIDERATIONS
--------------------------
- TABLOCK hint improves bulk insert performance
- FIRSTROW = 2 skips CSV header row
- TRUNCATE TABLE is used instead of DELETE for faster data removal


DEPENDENCIES
------------
1. Bronze schema tables must already exist
2. CSV files must be available at the specified file paths
3. SQL Server service account must have read access to source files
4. bronze.load_log table must exist


WARNINGS
--------
⚠ This procedure permanently deletes existing data in Bronze tables.
⚠ Do not run in production without proper validation.
⚠ Ensure source files are validated before execution.


EXECUTION
---------
To execute the procedure:

    EXEC bronze.load_bronze;


===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @start_time DATETIME;
    DECLARE @rows_loaded INT;

    BEGIN TRY

        BEGIN TRANSACTION;

        PRINT '====================================================';
        PRINT 'Starting Bronze Layer Full Refresh Load';
        PRINT 'Start Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
        PRINT '====================================================';


        ------------------------------------------------------------
        -- SECTION 1: CRM SOURCE LOAD
        ------------------------------------------------------------

        ------------------------------------------------------------
        -- crm_cust_info
        ------------------------------------------------------------

        SET @start_time = GETDATE();
        PRINT 'Loading crm_cust_info...';

        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\prath\Desktop\Z-All\Study\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.CSV'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @rows_loaded = @@ROWCOUNT;

        INSERT INTO bronze.load_log
        VALUES ('crm_cust_info', @start_time, GETDATE(), @rows_loaded, 'SUCCESS', NULL);

        PRINT 'Rows Loaded (crm_cust_info): ' + CAST(@rows_loaded AS VARCHAR);



        ------------------------------------------------------------
        -- crm_prd_info
        ------------------------------------------------------------

        SET @start_time = GETDATE();
        PRINT 'Loading crm_prd_info...';

        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\prath\Desktop\Z-All\Study\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.CSV'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @rows_loaded = @@ROWCOUNT;

        INSERT INTO bronze.load_log
        VALUES ('crm_prd_info', @start_time, GETDATE(), @rows_loaded, 'SUCCESS', NULL);

        PRINT 'Rows Loaded (crm_prd_info): ' + CAST(@rows_loaded AS VARCHAR);



        ------------------------------------------------------------
        -- crm_sales_details
        ------------------------------------------------------------

        SET @start_time = GETDATE();
        PRINT 'Loading crm_sales_details...';

        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\prath\Desktop\Z-All\Study\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.CSV'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @rows_loaded = @@ROWCOUNT;

        INSERT INTO bronze.load_log
        VALUES ('crm_sales_details', @start_time, GETDATE(), @rows_loaded, 'SUCCESS', NULL);

        PRINT 'Rows Loaded (crm_sales_details): ' + CAST(@rows_loaded AS VARCHAR);



        ------------------------------------------------------------
        -- SECTION 2: ERP SOURCE LOAD
        ------------------------------------------------------------

        ------------------------------------------------------------
        -- erp_CUST_AZ12
        ------------------------------------------------------------

        SET @start_time = GETDATE();
        PRINT 'Loading erp_CUST_AZ12...';

        TRUNCATE TABLE bronze.erp_CUST_AZ12;

        BULK INSERT bronze.erp_CUST_AZ12
        FROM 'C:\Users\prath\Desktop\Z-All\Study\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.CSV'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @rows_loaded = @@ROWCOUNT;

        INSERT INTO bronze.load_log
        VALUES ('erp_CUST_AZ12', @start_time, GETDATE(), @rows_loaded, 'SUCCESS', NULL);

        PRINT 'Rows Loaded (erp_CUST_AZ12): ' + CAST(@rows_loaded AS VARCHAR);



        ------------------------------------------------------------
        -- erp_LOC_A101
        ------------------------------------------------------------

        SET @start_time = GETDATE();
        PRINT 'Loading erp_LOC_A101...';

        TRUNCATE TABLE bronze.erp_LOC_A101;

        BULK INSERT bronze.erp_LOC_A101
        FROM 'C:\Users\prath\Desktop\Z-All\Study\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.CSV'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @rows_loaded = @@ROWCOUNT;

        INSERT INTO bronze.load_log
        VALUES ('erp_LOC_A101', @start_time, GETDATE(), @rows_loaded, 'SUCCESS', NULL);

        PRINT 'Rows Loaded (erp_LOC_A101): ' + CAST(@rows_loaded AS VARCHAR);



        ------------------------------------------------------------
        -- erp_PX_CAT_G1V2
        ------------------------------------------------------------

        SET @start_time = GETDATE();
        PRINT 'Loading erp_PX_CAT_G1V2...';

        TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

        BULK INSERT bronze.erp_PX_CAT_G1V2
        FROM 'C:\Users\prath\Desktop\Z-All\Study\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.CSV'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @rows_loaded = @@ROWCOUNT;

        INSERT INTO bronze.load_log
        VALUES ('erp_PX_CAT_G1V2', @start_time, GETDATE(), @rows_loaded, 'SUCCESS', NULL);

        PRINT 'Rows Loaded (erp_PX_CAT_G1V2): ' + CAST(@rows_loaded AS VARCHAR);



        ------------------------------------------------------------
        -- COMMIT
        ------------------------------------------------------------

        COMMIT TRANSACTION;

        PRINT '====================================================';
        PRINT 'Bronze Load Completed Successfully';
        PRINT 'End Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
        PRINT '====================================================';

    END TRY

    BEGIN CATCH

        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        INSERT INTO bronze.load_log
        VALUES ('BRONZE_LOAD_PROCESS', GETDATE(), GETDATE(), 0, 'FAILED', ERROR_MESSAGE());

        PRINT '====================================================';
        PRINT 'ERROR OCCURRED DURING BRONZE LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR);
        PRINT '====================================================';

        THROW;

    END CATCH

END;


exec bronze.load_bronze;
