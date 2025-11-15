/*
=================================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=================================================================================================

Purpose:
  This ETL (Extract , Transform , Load) Stored Procedure will populate the "Silver" Layer/Schema tables from the "Bronze" Layer
  By :
    -Truncating "Silver" Layer Tables
    -Inserting Clean And Standardized Data From Bronze into the Silver Layer

Usage:
    EXEC silver.load_silver;
=================================================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @StartTime DATETIME , @EndTime DATETIME , @BatchStartTime DATETIME , @BatchEndTime DATETIME 
	BEGIN TRY 
		SET @BatchStartTime = GETDATE()
		PRINT '=============================================================================';
		PRINT 'Loading The Silver Layer';
		PRINT '-----------------------------------------------------------------------------';
		

		PRINT '-----------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------------------------------------';

		SET @StartTime = GETDATE() 
		PRINT '>> Truncating Table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info
		PRINT '>> Inserting Date Into: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
			)
				SELECT	
					cst_id ,
					cst_key,
					TRIM(cst_firstname) cst_firstname ,
					TRIM(cst_lastname) cst_lastname,
					CASE 
						WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' 
						WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
						ELSE 'n/a' 
					END AS cst_marital_status, -- Normalize marital status values to readable format
					CASE 
						WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male' 
						WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
						ELSE 'n/a'
					END AS cst_gndr, -- Normalize gender values to readable format
					cst_create_date
				FROM (
				SELECT 
					*,
					ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) flag 
				FROM bronze.crm_cust_info
				WHERE cst_id IS NOT NULL
				)t
				WHERE flag = 1  -- Select the most recent record per customer; -- Select the most recent record per customer
		SET @EndTime = GETDATE() 
		PRINT CONCAT('>> Load Duration: ' , DATEDIFF(second,@StartTime , @EndTime) ,' seconds')
		PRINT '-------------------'


		SET @StartTime = GETDATE() 
		PRINT '>> Truncating Table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info 
		PRINT '>> Inserting Date Into: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info
		(
			prd_id,
			prd_cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1,5),'-','_') prd_cat_id, --Extract Category ID
			SUBSTRING(prd_key,7,LEN(prd_key))  prd_key,-- Extract Product Key
			prd_nm,
			COALESCE(prd_cost, 0) prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line , -- Map product line condes to descriptive values
			prd_start_dt,
			DATEADD(DAY,-1 ,LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) ) prd_end_dt -- Calculate End Date as one day before the next start date
		FROM bronze.crm_prd_info;
		SET @EndTime = GETDATE()
		PRINT CONCAT('>> Load Duration: ' , DATEDIFF(second,@StartTime , @EndTime) ,' seconds')
		PRINT '-------------------'

		SET @StartTime = GETDATE()
		PRINT '>> Truncating Table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details
		PRINT '>> Inserting Date Into: silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8  THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8  THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8  THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <0 OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales ,-- Recalculate/Derive sales if original value is missing or incorrect	
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <0 --OR sls_price != sls_sales / ABS(sls_quantity)
					THEN sls_sales / NULLIF(sls_quantity,0)
				ELSE sls_price -- Derive price if original value is missing	
			END AS sls_price
		FROM bronze.crm_sales_details;
		SET @EndTime = GETDATE()
		PRINT CONCAT('>> Load Duration: ' , DATEDIFF(second,@StartTime , @EndTime) , ' seconds')
		PRINT '-------------------'


		PRINT '-----------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables'
		PRINT '-----------------------------------------------------------------------------';

		SET @StartTime = GETDATE()
		PRINT '>> Truncating Table: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT '>> Inserting Date Into: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT 
			CASE WHEN cid LIKE 'NAS%' THEN 	SUBSTRING(cid,4,LEN(cid)) -- Removing 'NAS' prefix if present
				 ELSE cid
			END AS cid	,
			CASE 
				WHEN bdate > GETDATE() THEN NULL 
				ELSE bdate 
			END AS bdate , -- Setting BirthDates that are out of range to NULL
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
				WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'
				ELSE 'n/a' 
			END gen --Normalize gender values and handle missing values	
		FROM bronze.erp_cust_az12;
		SET @EndTime = GETDATE()
		PRINT CONCAT('>> Load Duration: ' , DATEDIFF(second,@StartTime , @EndTime) , ' seconds')
		PRINT '-------------------'
       

	    SET @StartTime = GETDATE()
		PRINT '>> Truncating Table: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT '>> Inserting Date Into: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101
		(
		cid,
		cntry
		)
		SELECT 
		REPLACE(cid ,'-' , '') cid ,
		CASE 
			WHEN cntry IN ('DE' , 'Germany' ) THEN 'Germany'
			WHEN cntry IN ('USA' ,'US' , 'United States' ) THEN 'United States'
			WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
			ELSE TRIM(cntry)
		END cntry --Normalize and Handle missing Country Codes 
		FROM bronze.erp_loc_a101;
		SET @EndTime = GETDATE()
		PRINT CONCAT('>> Load Duration: ' , DATEDIFF(second,@StartTime , @EndTime) , ' seconds')
		PRINT '-------------------'

		SET @StartTime = GETDATE()
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT '>> Inserting Date Into: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @EndTime = GETDATE()
		PRINT CONCAT('>> Load Duration: ' , DATEDIFF(second,@StartTime , @EndTime) , ' seconds')
		PRINT '-------------------'
		SET @BatchEndTime = GETDATE()
		PRINT 'Silver Layer is Loaded Successfully'
		PRINT CONCAT('Load Duration for the Silver Layer: ' , DATEDIFF(second,@BatchStartTime ,@BatchEndTime),' seconds')
		PRINT '================================================================================================='


	END TRY
	BEGIN CATCH
		PRINT '=================================================================================================';
		PRINT CONCAT('Error Message' , ERROR_MESSAGE());
		PRINT CONCAT('Error Message' , ERROR_NUMBER());
		PRINT CONCAT('Error Message' , ERROR_STATE());
		PRINT '=================================================================================================';
	END CATCH
END 
