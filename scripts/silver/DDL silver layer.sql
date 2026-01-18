/*
================================================================================
Script Name : silver_layer_tables.sql
Layer       : Silver

Description :
This script creates the Silver layer tables of the SQL Data Warehouse by dropping
existing objects and recreating them with a refined schema derived from the
Bronze layer.

The Silver layer represents cleaned and standardized data. Compared to the
Bronze layer, this layer introduces basic structural improvements such as:
- Standardized data types (e.g., date conversions)
- Enriched attributes required for downstream analytics
- Technical metadata columns for data warehouse tracking

Each table includes a dwh_create_date column to record the load timestamp,
supporting data lineage and audit requirements.

No business-level aggregations are performed in this layer. The data remains
granular and is prepared for consumption by the Gold layer.

Source :
- Transformed data from Bronze layer tables

Usage Notes :
- Tables are recreated to support repeatable development and testing
- Designed to be populated via Silver-layer load procedures
================================================================================
*/

if object_id('silver.crm_cust_info','u') is not null
	drop table silver.crm_cust_info;
create table silver.crm_cust_info(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date,
	dwh_create_date datetime2 default getdate() 
);
go

if object_id('silver.crm_prd_info','u') is not null
	drop table silver.crm_prd_info;
create table silver.crm_prd_info(
	prd_id int,
	cat_id nvarchar(50),
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date,
	dwh_create_date datetime2 default getdate()
);
go

if object_id('silver.crm_sales_details','u') is not null
	drop table silver.crm_sales_details;
create table silver.crm_sales_details(
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt date,
	sls_ship_dt date,
	sls_due_dt date,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date datetime2 default getdate()
);
go

if object_id('silver.erp_cust_az12','u') is not null
	drop table silver.erp_cust_az12;
create table silver.erp_cust_az12(
	cid nvarchar(50),
	bdate date,
	gen nvarchar(50),
	dwh_create_date datetime2 default getdate()
);
go

if object_id('silver.erp_loc_a101','u') is not null
	drop table silver.erp_loc_a101;
create table silver.erp_loc_a101(
	cid nvarchar(50),
	cntry nvarchar(50),
	dwh_create_date datetime2 default getdate()
);
go

if object_id('silver.erp_px_cat_g1v2','u') is not null
	drop table silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2(
	id nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintenance nvarchar(50),
	dwh_create_date datetime2 default getdate()
);
