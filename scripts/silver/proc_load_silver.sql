/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
Create or alter procedure silver.load_silver AS 
  begin
	  declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	  begin try
		  Set @batch_start_time = GETDATE();
		  print'===========================================';
		  print'Loading silver Layer';
		  print'===========================================';
	
    		  print '------------------------------------------';
    		  print 'Loading CRM Tables';
    		  print '------------------------------------------';

                  -- Loading silver.crm_cust_infor
            		Set @start_time = GETDATE();
            		print '>> Truncating Table: silver.crm_cust_infor';
            		Truncate table silver.crm_cust_infor;
                print '>> Insert Data Into: silver.crm_cust_infor';
            		  Insert into silver.crm_cust_infor (
                    	cst_id,
                    	cst_key,
                    	cst_firstname,
                    	cst_lastname,
                    	cst_marital_status,
                    	cst_gndr,
                    	cst_create_date
                    )
                  Select 
                      cst_id,
                      cst_key,
                      trim(cst_firstname) as cst_firstname,
                      trim(cst_lastname) as cst_lastname,
                      case 
                      	when Upper(trim(cst_marital_status)) = 'M' then 'Married'
                      	when Upper(trim(cst_marital_status)) = 'S' then 'Single'
                      	else 'N/A'
                      end cst_marital_status, 
                      case 
                      	when Upper(trim(cst_gndr)) = 'M' then 'Male'
                      	when Upper(trim(cst_gndr)) = 'F' then 'Female'
                      	else 'N/A'
                      end cst_gndr,
                      cst_create_date
                          from
                          	(Select 
                          	*,
                          	Row_NUMBER() Over (Partition by cst_id order by cst_create_date DESC) as flag_last
                          	from bronze.crm_cust_infor where cst_id is not null) As ranked_customers
                          where flag_last = 1;            
            		Set @end_time = GETDATE();
            		print '>> Load Duration: ' +Cast (Datediff(second, @start_time, @end_time) As Nvarchar) + 'seconds';
            		print'>> --------------';
              

                  -- Loading silver.crm_cprd_infor
          		
