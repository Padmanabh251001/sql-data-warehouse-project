/*
================================================================================
Script Name : DDl.bronze.sql
Layer       : Bronze
Description :
This script initializes the Bronze layer of the SQL Data Warehouse by dropping
and recreating raw staging tables for CRM and ERP source systems.

The tables are populated from CSV-based source extracts and are designed to
closely reflect the original source schema. No data cleansing, validation,
deduplication, or business logic is applied in this layer.

The Bronze layer serves as the system-of-record for ingested data, enabling
traceability, auditability, and reliable downstream transformations in the
Silver and Gold layers.

Source Systems :
- CRM (Customer, Product, Sales data)
- ERP (Customer, Location, Product Classification data)

Usage Notes :
- Intended for full reload scenarios
- Safe to re-run as tables are dropped and recreated
- Downstream layers depend on these tables for transformations
================================================================================
*/
if object_id('bronze.crm_cust_info','u') is not null
	drop table bronze.crm_cust_info;
create table bronze.crm_cust_info(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date
);
go

if object_id('bronze.crm_prd_info','u') is not null
	drop table bronze.crm_prd_info;
create table bronze.crm_prd_info(
	prd_id int,
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date
);
go

if object_id('bronze.crm_sales_details','u') is not null
	drop table bronze.crm_sales_details;
create table bronze.crm_sales_details(
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int
);
go

if object_id('bronze.erp_cust_az12','u') is not null
	drop table bronze.erp_cust_az12;
create table bronze.erp_cust_az12(
	cid nvarchar(50),
	bdate date,
	gen nvarchar(50)
);
go

if object_id('bronze.erp_loc_a101','u') is not null
	drop table bronze.erp_loc_a101;
create table bronze.erp_loc_a101(
	cid nvarchar(50),
	cntry nvarchar(50)
);
go

if object_id('bronze.erp_px_cat_g1v2','u') is not null
	drop table bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2(
	id nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintenance nvarchar(50)
);
