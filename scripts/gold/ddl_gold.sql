/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
  
CREATE VIEW gold.dim_customers as
Select 
	Row_number() over (order by cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as customer_firstname,
	ci.cst_lastname as customer_lastname,
	ci.cst_marital_status as marital_status,
	case when ci.cst_gndr != 'N/A' then ci.cst_gndr -- CRM is the master for gender
	else coalesce(ca.GEN, 'N/A') 
	end as new_gen,
	ci.cst_create_date as create_date,
	ca.BDATE as birthdate,
	la.CNTRY as country
from silver.crm_cust_infor ci
left join silver.erp_cust_az12 ca
on		  ci.cst_key = ca.CID 
right join silver.erp_loc_a101 la
on		   ci.cst_key = la.CID

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products as
Select 
	Row_number() over (order by TY.prd_start_dt, TY.prd_key) as product_key,
	TY.prd_id as product_id,
	TY.prd_key as product_number,
	TY.prd_nm as product_name,
	TY.cat_id as category_id,
	TI.CAT as category,
	TI.SUBCAT as sub_category,
	TI.MAINTENANCE,
	TY.prd_cost as cost,
	TY.prd_line as product_line,
	TY.prd_start_dt as start_date
from silver.crm_prd_infor TY
left join silver.erp_px_cat_g1v2 TI
on TY.cat_id = TI.ID
where TY.prd_end_dt is null


-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

Create view gold.fact_sales as
select 
	DE.sls_ord_num as order_number,
	DA.product_key,
	DI.customer_key,
	DE.sls_order_dt as order_date,
	DE.sls_ship_dt as shipping_date,
	DE.sls_due_dt as due_date,
	DE.sls_sales as sales_amount,
	DE.sls_quantity as quantity,
	DE.sls_price as price
from silver.crm_sales_details DE
left join gold.dim_products DA
on DE.sls_prd_key = DA.product_number
left join gold.dim_customers DI
on DE.sls_cust_id = DI.customer_id
