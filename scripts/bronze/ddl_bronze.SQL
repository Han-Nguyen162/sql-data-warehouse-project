/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
Use DataWarehouse;

if object_id ('bronze.crm_cust_infor', 'U') is not null
	drop table bronze.crm_cust_infor;

Create Table bronze.crm_cust_infor (
	cst_id	INT,
	cst_key	NVarchar(50),
	cst_firstname NVarchar(50),
	cst_lastname	NVarchar(50),
	cst_marital_status NVarchar(50),	
	cst_gndr NVarchar(50),
	cst_create_date date
);

if object_id ('bronze.crm_prd_infor', 'U') is not null
	drop table bronze.crm_prd_infor;

Create Table bronze.crm_prd_infor (
	prd_id	Int,
	prd_key	NVarchar(50),
	prd_nm	NVarchar(50),
	prd_cost NVarchar(50),
	prd_line NVarchar(50),
	prd_start_dt date,
	prd_end_dt Date
);

if object_id ('bronze.crm_sales_details', 'U') is not null
	drop table bronze.crm_sales_details;

Create Table bronze.crm_sales_details (
	sls_ord_num	Nvarchar(50),
	sls_prd_key	NVarchar(50),
	sls_cust_id	int,
	sls_order_dt int,
	sls_ship_dt	int,
	sls_due_dt	int,
	sls_sales	int,
	sls_quantity int,	
	sls_price Int
)

if object_id ('bronze.erp_cust_az12', 'U') is not null
	drop table bronze.erp_cust_az12;

Create Table bronze.erp_cust_az12 (
	CID	nvarchar(50),
	BDATE date,
	GEN nvarchar(50)
)

if object_id ('bronze.erp_loc_a101', 'U') is not null
	drop table bronze.erp_loc_a101;

Create table bronze.erp_loc_a101 (
	CID nvarchar(50),	
	CNTRY nvarchar(50)
)

if object_id ('bronze.erp_px_cat_g1v2', 'U') is not null
	drop table bronze.erp_px_cat_g1v2;

Create table bronze.erp_px_cat_g1v2 (
	ID nvarchar(50),	
	CAT	nvarchar(50),
	SUBCAT nvarchar(50),
	MAINTENANCE nvarchar(50)
)

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
