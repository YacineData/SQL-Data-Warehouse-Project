/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/



CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @StartTime DATETIME , @EndTime DATETIME, @BatchStartTime DATETIME , @BatchEndTime DATETIME
	BEGIN TRY
		SET @BatchStartTime = GETDATE();
		PRINT '==================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==================================================';

		PRINT '--------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------------------';

		SET @StartTime = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',' ,
			TABLOCK 
		);
		SET @EndTime = GETDATE();
		PRINT CONCAT('>> Load Duration: ' , DATEDIFF(second, @StartTime , @EndTime) , ' Seconds')
		PRINT '>> -----------'


		SET @StartTime = GETDATE()
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;


		PRINT '>> Inserting Data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',' ,
			TABLOCK 
		);
		SET @EndTime = GETDATE();
		PRINT CONCAT('>> Load Duration: ' , DATEDIFF(second, @StartTime , @EndTime) , ' Seconds')
		PRINT '>> -----------'


		SET @StartTime = GETDATE()
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;


		PRINT '>> Inserting Data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',' ,
			TABLOCK 
		);
		SET @EndTime = GETDATE();
		PRINT CONCAT('>> Load Duration: ' , DATEDIFF(second, @StartTime , @EndTime) , ' Seconds')
		PRINT '>> -----------'



		PRINT '--------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------------------------------';



		SET @StartTime = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;


		PRINT '>> Inserting Data into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',' ,
			TABLOCK 
		);
		SET @EndTime = GETDATE();
		PRINT CONCAT('>> Load Duration: ' , DATEDIFF(second, @StartTime , @EndTime) , ' Seconds')
		PRINT '>> -----------'


		SET @StartTime = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;


		PRINT '>> Inserting Data into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',' ,
			TABLOCK 
		);
		SET @EndTime = GETDATE();
		PRINT CONCAT('>> Load Duration: ' , DATEDIFF(second, @StartTime , @EndTime) , ' Seconds')
		PRINT '>> -----------'


		SET @StartTime = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;


		PRINT '>> Inserting Data into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',' ,
			TABLOCK 
		);
		SET @EndTime = GETDATE();
		PRINT CONCAT('>> Load Duration: ' , DATEDIFF(second, @StartTime , @EndTime) , ' Seconds')
		PRINT '>> -----------'
		
		SET @BatchEndTime = GETDATE();
		PRINT '================================================='
		PRINT 'Loading Bronze Layer is Completed'
		PRINT CONCAT('>> Whole Batch Load Duration: ' , DATEDIFF(second, @BatchStartTime, @BatchEndTime) , ' Seconds')
		PRINT '================================================='

	END TRY 
	BEGIN CATCH 
		PRINT '================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT CONCAT('Error Message ' , ERROR_MESSAGE());
		PRINT CONCAT('Error Number' , ERROR_NUMBER());
		PRINT CONCAT('Error Message' , ERROR_STATE());
		PRINT '================================================='
	END CATCH
END 
