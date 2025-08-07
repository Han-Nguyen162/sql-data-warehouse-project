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
Create or alter procedure bronze.load_bronze AS 
begin
	declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	begin try
		Set @batch_start_time = GETDATE();
		print'===========================================';
		print'Loading Bronze Layer';
		print'===========================================';
	
		print '------------------------------------------';
		print 'Loading CRM Tables';
		print '------------------------------------------';

		Set @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_cust_infor';
		Truncate table bronze.crm_cust_infor;

		print '>> Insert Data Into: bronze.crm_cust_infor';
		bulk insert bronze.crm_cust_infor
		from "C:\Users\hanng\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv"
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock 
		);
		Set @end_time = GETDATE();
		print '>> Load Duration: ' +Cast (Datediff(second, @start_time, @end_time) As Nvarchar) + 'seconds';
		print'>> --------------';

		Set @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_prd_infor';
		Truncate table bronze.crm_prd_infor;

		print '>> Insert Data Into: bronze.crm_prd_infor';
		bulk insert bronze.crm_prd_infor
		from "C:\Users\hanng\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv"
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock 
		);
		Set @end_time = GETDATE();
		print '>> Load Duration: ' +Cast (Datediff(second, @start_time, @end_time) As Nvarchar) + 'seconds';
		print'>> --------------';

		Set @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_sales_details';
		Truncate table bronze.crm_sales_details;

		print '>> Insert Data Into: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from "C:\Users\hanng\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv"
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock 
		);
		Set @end_time = GETDATE();
		print '>> Load Duration: ' +Cast (Datediff(second, @start_time, @end_time) As Nvarchar) + 'seconds';
		print'>> --------------';

		print '------------------------------------------';
		print 'Loading ERP Tables';
		print '------------------------------------------';

		Set @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_cust_az12';
		Truncate table bronze.erp_cust_az12;

		print '>> Insert Data Into: bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from "C:\Users\hanng\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv"
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock 
		);
		Set @end_time = GETDATE();
		print '>> Load Duration: ' +Cast (Datediff(second, @start_time, @end_time) As Nvarchar) + 'seconds';
		print'>> --------------';

		Set @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_loc_a101';
		Truncate table bronze.erp_loc_a101;

		print '>> Insert Data Into: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from "C:\Users\hanng\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv"
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock 
		);
		Set @end_time = GETDATE();
		print '>> Load Duration: ' +Cast (Datediff(second, @start_time, @end_time) As Nvarchar) + 'seconds';
		print'>> --------------';

		Set @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_px_cat_g1v2';
		Truncate table bronze.erp_px_cat_g1v2;

		print '>> Insert Data Into: bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from "C:\Users\hanng\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv"
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock 
		);
		Set @end_time = GETDATE();
		print '>> Load Duration: ' +Cast (Datediff(second, @start_time, @end_time) As Nvarchar) + 'seconds';
		print'>> --------------';
	
	Set @batch_end_time = GETDATE();
	print '=================================='
	print 'Loading Bronze Layer is completed';
	print '>> Total Load Duration: ' +Cast (Datediff(second, @batch_start_time, @batch_end_time) As Nvarchar) + 'seconds';
	print '=================================='

	end try
	begin catch
		print '=================================='
		print 'Error occured during loading bronze layer'
		print 'Error Message' + Error_Message();
		print 'Error Message' + Cast (Error_Number() AS NVARCHAR);
		print 'Error Message' + Cast (Error_Number() AS NVARCHAR);
		print '=================================='
	end catch 
END;
