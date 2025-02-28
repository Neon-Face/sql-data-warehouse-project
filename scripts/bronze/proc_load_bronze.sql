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


DELIMITER $$

CREATE PROCEDURE bronze_load_bronze()
BEGIN
	DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;
    DECLARE error_message TEXT;

	-- Handling errors
	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
	BEGIN
		GET DIAGNOSTICS CONDITION 1 error_message = MESSAGE_TEXT;
		SELECT '==========================================' AS message;
        SELECT 'ERROR OCCURRED DURING LOADING BRONZE LAYER' AS message;
        SELECT CONCAT('Error Message: ', error_message) AS message;
        SELECT '==========================================' AS message;
    END;

	-- START TIME
	SET batch_start_time = NOW()

	-- SET LOG
	    -- 日志信息
    SELECT '================================================' AS message;
    SELECT 'Loading Bronze Layer' AS message;
    SELECT '================================================' AS message;
    SELECT '------------------------------------------------' AS message;
    SELECT 'Loading CRM Tables' AS message;
    SELECT '------------------------------------------------' AS message;

	-- bronze.crm_cust_info
	SET start_time = NOW()
	SELECT '>> Truncating Table: bronze.crm_cust_info' AS message;
	TRUNCATE TABLE bronze.crm_cust_info;
	SELECT '>> Inserting Data Into: bronze.crm_cust_info' AS message;
	LOAD DATA INFILE '/Users/lucas/Documents/Obsidian/FUTURE/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
	INTO TABLE bronze.crm_cust_info
	FIELDS TERMINATED BY ','
	IGNORE 1 ROWS;
	SET end_time = NOW();
	SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS message;
    SELECT '>> -------------' AS message;

	-- 处理 bronze.crm_prd_info
    SET start_time = NOW();
    SELECT '>> Truncating Table: bronze.crm_prd_info' AS message;
    TRUNCATE TABLE bronze.crm_prd_info;
    SELECT '>> Inserting Data Into: bronze.crm_prd_info' AS message;
    LOAD DATA INFILE '/Users/lucas/Documents/Obsidian/FUTURE/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
    INTO TABLE bronze.crm_prd_info
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS message;
    SELECT '>> -------------' AS message;

    -- 处理 bronze.crm_sales_details
    SET start_time = NOW();
    SELECT '>> Truncating Table: bronze.crm_sales_details' AS message;
    TRUNCATE TABLE bronze.crm_sales_details;
    SELECT '>> Inserting Data Into: bronze.crm_sales_details' AS message;
    LOAD DATA INFILE '/Users/lucas/Documents/Obsidian/FUTURE/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
    INTO TABLE bronze.crm_sales_details
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS message;
    SELECT '>> -------------' AS message;

    SELECT '------------------------------------------------' AS message;
    SELECT 'Loading ERP Tables' AS message;
    SELECT '------------------------------------------------' AS message;

    -- 处理 bronze.erp_loc_a101
    SET start_time = NOW();
    SELECT '>> Truncating Table: bronze.erp_loc_a101' AS message;
    TRUNCATE TABLE bronze.erp_loc_a101;
    SELECT '>> Inserting Data Into: bronze.erp_loc_a101' AS message;
    LOAD DATA INFILE '/Users/lucas/Documents/Obsidian/FUTURE/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
    INTO TABLE bronze.erp_loc_a101
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS message;
    SELECT '>> -------------' AS message;

    -- 处理 bronze.erp_cust_az12
    SET start_time = NOW();
    SELECT '>> Truncating Table: bronze.erp_cust_az12' AS message;
    TRUNCATE TABLE bronze.erp_cust_az12;
    SELECT '>> Inserting Data Into: bronze.erp_cust_az12' AS message;
    LOAD DATA INFILE '/Users/lucas/Documents/Obsidian/FUTURE/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
    INTO TABLE bronze.erp_cust_az12
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS message;
    SELECT '>> -------------' AS message;

    -- 处理 bronze.erp_px_cat_g1v2
    SET start_time = NOW();
    SELECT '>> Truncating Table: bronze.erp_px_cat_g1v2' AS message;
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    SELECT '>> Inserting Data Into: bronze.erp_px_cat_g1v2' AS message;
    LOAD DATA INFILE '/Users/lucas/Documents/Obsidian/FUTURE/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
    INTO TABLE bronze.erp_px_cat_g1v2
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS message;
    SELECT '>> -------------' AS message;

    -- 记录总执行时间
    SET batch_end_time = NOW();
    SELECT '==========================================' AS message;
    SELECT 'Loading Bronze Layer is Completed' AS message;
    SELECT CONCAT('   - Total Load Duration: ', TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' seconds') AS message;
    SELECT '==========================================' AS message;

END $$
