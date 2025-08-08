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
					Set @end_time = GETDATE();
					print '>> Load Duration: ' +Cast (Datediff(second, @start_time, @end_time) As Nvarchar) + 'seconds';
					print'>> --------------';
					
					Set @start_time = GETDATE();
					print '>> Truncating Table: silver.crm_prd_infor';
					Truncate table silver.crm_prd_infor;
					
					print '>> Insert Data Into: silver.crm_prd_infor';
					
							Insert into silver.crm_prd_infor (
								prd_id,
								cat_id,
								prd_key,
								prd_nm,
								prd_cost,
								prd_line,
								prd_start_dt,
								prd_end_dt
									)
							Select 
								prd_id,
								Replace(Substring(prd_key,1,5), '-','_') as cat_id,
								substring(prd_key,7, len(prd_key)) as prd_key,
								prd_nm,
								Isnull(prd_cost,0) as prd_cost,
								case
									when Upper(trim(prd_line)) = 'M' then 'Mountain'
									when Upper(trim(prd_line)) = 'R' then 'Road'
									when Upper(trim(prd_line)) = 'S' then 'Other Sales'
									when Upper(trim(prd_line)) = 'T' then 'Touring'
									else 'N/A'
								end as prd_line,
								cast (prd_start_dt As Date) as prd_start_dt, 
								dateadd(day,-1, Lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) AS prd_end_dt
							from bronze.crm_prd_infor;
					
					Set @end_time = GETDATE();
					print '>> Load Duration: ' +Cast (Datediff(second, @start_time, @end_time) As Nvarchar) + 'seconds';
					print'>> --------------';

 			-- Loading crm_sales_details
					Set @start_time = GETDATE();
					print '>> Truncating Table: silver.crm_sales_details';
					Truncate table silver.crm_sales_details;
					
					print '>> Insert Data Into: silver.crm_sales_details';
								 Insert into silver.crm_sales_details (
								sls_ord_num,
								sls_prd_key,
								sls_cust_id,
								sls_order_dt,
								sls_ship_dt,
								sls_due_dt,
								sls_sales,
								sls_quantity,
								sls_price ) 
								
								Select 
									sls_ord_num,
									sls_prd_key,
									sls_cust_id,
									
									case when sls_order_dt = 0 
										or len(sls_order_dt) != 8 then Null
										else cast(cast(sls_order_dt as Varchar) as Date)
									end as sls_order_dt,
									
									case when sls_ship_dt = 0 
										or len(sls_ship_dt) != 8 then Null
										else cast(cast(sls_ship_dt as Varchar) as Date)
									end as sls_ship_dt,
									
									case when sls_due_dt = 0 
										or len(sls_due_dt) != 8 then Null
										else cast(cast(sls_due_dt as Varchar) as Date)
									end as sls_due_dt,
									
									case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
										then sls_quantity * ABS(sls_price)
										else sls_sales
									end as sls_sales,
									sls_quantity,
									case when sls_price is null or sls_price <= 0 
												then sls_sales / Nullif(sls_quantity,0)
										else sls_price
									end as sls_price
								from bronze.crm_sales_details

					Set @end_time = GETDATE();
					print '>> Load Duration: ' +Cast (Datediff(second, @start_time, @end_time) As Nvarchar) + 'seconds';
					print'>> --------------';
			
	-- Loading erp_cust_az12

					Set @start_time = GETDATE();
					print '>> Truncating Table: silver.erp_cust_az12';
					Truncate table silver.erp_cust_az12;
					
					print '>> Insert Data Into: silver.erp_cust_az12';
					
					Insert into silver.erp_cust_az12 ( cid, BDATE, Gen)
					Select
						cid,
						case when cid like 'NAS%' then substring(CID,4, len(cid))
						else cid
						end as cid,	
						case when BDATE > Getdate() then Null
						else BDATE
						END AS BDATE,
						case when upper(trim(Gen)) In ('F','Female') then 'Female'
						     when upper(trim(Gen)) In ('M','Male')  then 'Male'
							 else 'N/A'
						end as Gen
					from bronze.erp_cust_az12;  

					Set @end_time = GETDATE();
					print '>> Load Duration: ' +Cast (Datediff(second, @start_time, @end_time) As Nvarchar) + 'seconds';
					print'>> --------------';
				

	-- Loading erp_loc_a101
					Set @start_time = GETDATE();
					print '>> Truncating Table: silver.erp_loc_a101';
					Truncate table silver.erp_loc_a101;
					print '>> Insert Data Into: silver.erp_loc_a101';
					
					Insert into silver.erp_loc_a101 (CID, CNTRY)
						select 
						replace(CID, '-','') as CID,
						case when upper(trim(CNTRY)) in ('USA','United States','US') then 'United States'
							 when upper(trim(CNTRY)) = 'DE' then 'Germany'
							 when upper(trim(CNTRY))= '' or CNTRY is null then 'N/A'
							 else (trim(CNTRY))
						end as CNTRY
					from bronze.erp_loc_a101
						
					Set @end_time = GETDATE();
					print '>> Load Duration: ' +Cast (Datediff(second, @start_time, @end_time) As Nvarchar) + 'seconds';
					print'>> --------------';

	-- Loading erp_px_cat_g1v2

					Set @start_time = GETDATE();
					print '>> Truncating Table: silver.erp_px_cat_g1v2';
					Truncate table silver.erp_px_cat_g1v2;
					
					print '>> Insert Data Into: silver.erp_px_cat_g1v2';
					
					Insert into silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
					Select 
					id,
					cat,
					subcat,
					maintenance from bronze.erp_px_cat_g1v2
					
					Set @end_time = GETDATE();
					print '>> Load Duration: ' +Cast (Datediff(second, @start_time, @end_time) As Nvarchar) + 'seconds';
					print'>> --------------';

	
		Set @batch_end_time = GETDATE();
		print '=================================='
		print 'Loading silver Layer is completed';
		print '>> Total Load Duration: ' +Cast (Datediff(second, @batch_start_time, @batch_end_time) As Nvarchar) + 'seconds';
		print '=================================='
	
		end try
		begin catch
			print '=================================='
			print 'Error occured during loading silver layer'
			print 'Error Message' + Error_Message();
			print 'Error Message' + Cast (Error_Number() AS NVARCHAR);
			print 'Error Message' + Cast (Error_Number() AS NVARCHAR);
			print '=================================='
		end catch 
	END;
