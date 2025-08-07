/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
Use DataWarehouse;

if object_id ('silver.crm_cust_infor', 'U') is not null
	drop table silver.crm_cust_infor;
Go

Create Table silver.crm_cust_infor (
	cst_id	INT,
	cst_key	NVarchar(50),
	cst_firstname NVarchar(50),
	cst_lastname	NVarchar(50),
	cst_marital_status NVarchar(50),	
	cst_gndr NVarchar(50),
	cst_create_date date,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
Go

if object_id ('silver.crm_prd_infor', 'U') is not null
	drop table silver.crm_prd_infor;

Create Table silver.crm_prd_infor (
	prd_id	Int,
	cat_id NVarchar(50),
	prd_key	NVarchar(50),
	prd_nm	NVarchar(50),
	prd_cost NVarchar(50),
	prd_line NVarchar(50),
	prd_start_dt date,
	prd_end_dt Date,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
Go

if object_id ('silver.crm_sales_details', 'U') is not null
	drop table silver.crm_sales_details;

Create Table silver.crm_sales_details (
	sls_ord_num	Nvarchar(50),
	sls_prd_key	NVarchar(50),
	sls_cust_id	int,
	sls_order_dt Date,
	sls_ship_dt	Date,
	sls_due_dt	Date,
	sls_sales	int,
	sls_quantity int,	
	sls_price Int,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
Go

if object_id ('silver.erp_cust_az12', 'U') is not null
	drop table silver.erp_cust_az12;

Create Table silver.erp_cust_az12 (
	CID	nvarchar(50),
	BDATE date,
	GEN nvarchar(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
Go

if object_id ('silver.erp_loc_a101', 'U') is not null
	drop table silver.erp_loc_a101;

Create table silver.erp_loc_a101 (
	CID nvarchar(50),	
	CNTRY nvarchar(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
Go

if object_id ('silver.erp_px_cat_g1v2', 'U') is not null
	drop table silver.erp_px_cat_g1v2;

Create table silver.erp_px_cat_g1v2 (
	ID nvarchar(50),	
	CAT	nvarchar(50),
	SUBCAT nvarchar(50),
	MAINTENANCE nvarchar(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
Go
