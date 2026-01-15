/*
================================================================================
Procedure Name : proc_load_bronze.sql
Layer          : Bronze
Type           : Data Load (Full Refresh)

Description :
This stored procedure loads raw data into the Bronze layer of the SQL Data
Warehouse from CRM and ERP source systems using CSV files.

The procedure performs a full refresh by truncating existing Bronze tables
and bulk inserting data from source files. It captures execution timing for
each table load as well as the overall load duration, providing basic runtime
monitoring through printed log messages.

No data transformations, validations, or business rules are applied. The data
is ingested exactly as received from the source systems to maintain data
traceability and support downstream Silver and Gold layer transformations.

Source Systems :
- CRM: Customer, Product, and Sales data
- ERP: Customer, Location, and Product Classification data

Key Features :
- Uses BULK INSERT for efficient data loading
- Truncate-and-load strategy (full reload)
- Execution time logging per table and per run
- TRY-CATCH error handling for

create or alter procedure bronze.load_bronze as
begin
	begin try
	declare @start_time datetime, @end_time datetime, @start_full_time datetime, @end_full_time datetime;
		print '=======================================================================================';
		print 'Loading into bronze layer';
		print '=======================================================================================';

		print '---------------------------------------------------------------------------------------';
		print 'Loading CRM data into tables';
		print '---------------------------------------------------------------------------------------';

		set @start_full_time = GETDATE();
		set @start_time = GETDATE();
		print '>>Truncating table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;
		print '>>Loading data into : bronze.crm_cust_info'; 
		bulk insert bronze.crm_cust_info
		from 'D:\Data Engineering course\Data Warehouse project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE();
		print '>> Time required: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print '---------------------';

		set @start_time = GETDATE();
		print '>>Truncating table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;
		print '>>Loading data into : bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'D:\Data Engineering course\Data Warehouse project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE();
		print '>> Time required: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print '---------------------';

		set @start_time = GETDATE();
		print '>>Truncating table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
		print '>>Loading data into : bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'D:\Data Engineering course\Data Warehouse project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE();
		print '>> Time required: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print '---------------------';

		print '---------------------------------------------------------------------------------------';
		print 'Loading ERP data into tables';
		print '---------------------------------------------------------------------------------------';

		set @start_time = GETDATE();
		print '>>Truncating table: bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;
		print '>>Loading data into : bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'D:\Data Engineering course\Data Warehouse project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE();
		print '>> Time required: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print '---------------------';

		set @start_time = GETDATE();
		print '>>Truncating table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;
		print '>>Loading data into : bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'D:\Data Engineering course\Data Warehouse project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE();
		print '>> Time required: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print '---------------------';

		set @start_time = GETDATE();
		print '>>Truncating table: bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
		print '>>Loading data into : bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'D:\Data Engineering course\Data Warehouse project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE();
		set @end_full_time = GETDATE();
		print '>> Time required: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print '---------------------';
		print '>> Full time required: ' + cast(datediff(second,@start_full_time,@end_full_time) as nvarchar) + ' seconds';
	end try
	begin catch
	print '========================================================================================';
	print 'An error occured during loading bronze layer';
	print 'error message' + error_message();
	print 'error message' + cast(error_number() as nvarchar);
	print 'error message' + cast(error_state() as nvarchar);
	print '========================================================================================';
	end catch
end
