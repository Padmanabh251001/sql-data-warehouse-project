/*
================================================================================
Procedure Name : silver.load_silver
Layer          : Silver
Type           : Data Transformation & Load (Full Refresh)

Description :
This stored procedure populates the Silver layer of the SQL Data Warehouse by
transforming and standardizing data from the Bronze layer.

The procedure applies basic data quality rules such as trimming text values,
standardizing codes, handling nulls and invalid values, correcting dates,
resolving duplicates, and deriving cleaned attributes required for analytics.
The transformed data is then loaded into Silver tables using a truncate-and-load
(full refresh) strategy.

Execution time is logged for each table load as well as for the full procedure
run to support monitoring and performance analysis.

Source & Target :
- Source : Bronze layer tables (raw CRM and ERP data)
- Target : Silver layer tables (cleaned and standardized data)

Key Transformations :
- Deduplication using latest record logic
- Standardization of gender, marital status, country, and product attributes
- Date format validation and correction
- Data quality fixes for sales amounts and pricing
- Basic key normalization for cross-system consistency

Key Features :
- Truncate-and-load approach for repeatable processing
- Step-wise logging with execution time tracking
- TRY-CATCH error handling for controlled failures
- Prepares data for business modeling in the Gold layer

Usage Notes :
- Designed for batch execution after Bronze layer load
- No aggregations or business KPIs are created in this layer
- Downstream Gold layer depends on successful execution
================================================================================
*/

create or alter procedure silver.load_silver as 
begin
	begin try
		declare @start_time datetime, @end_time datetime, @full_start_time datetime, @full_end_time datetime;
		print '=======================================================================================';
		print 'Loading into silver layer';
		print '=======================================================================================';

		print '---------------------------------------------------------------------------------------';
		print 'Loading CRM data into tables';
		print '---------------------------------------------------------------------------------------';

		set @full_start_time=GETDATE();
		set @start_time=GETDATE();
		print '>> Truncating table: silver.crm_cust_info';
		truncate table silver.crm_cust_info;
		print '>> Loading into: silver.crm_cust_info';
		insert into silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

		select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,
		case when upper(trim(cst_marital_status)) = 'M' then 'Married'
			 when upper(trim(cst_marital_status)) = 'S' then 'Single'
			 else 'N/A'
		end cst_marital_status,
		case when upper(trim(cst_gndr)) = 'M' then 'Male'
			 when upper(trim(cst_gndr)) = 'F' then 'Female'
			 else 'N/A'
		end cst_gndr,
		cst_create_date
		from(
			select 
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date,
			row_number() over(partition by cst_id order by cst_create_date desc) as flag_rank
			from bronze.crm_cust_info
			where cst_id is not null
		)t where flag_rank=1
		set @end_time=GETDATE();
		print '>> Time required: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print '---------------------';

		set @start_time=GETDATE();
		print '>> Truncating table: silver.crm_prd_info';
		truncate table silver.crm_prd_info;
		print '>> Loading into: silver.crm_prd_info';
		insert into silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt)
		select 
		prd_id,
		replace(substring(prd_key,1,5),'-','_') as cat_id,
		substring(prd_key,7,len(prd_key)) as prd_key,
		prd_nm,
		isnull(prd_cost,0) as prd_cost,
		case trim(prd_line)
			when 'R' then 'Road'
			when 'S' then 'Other sales'
			when 'M' then 'Mountain'
			when 'T' then 'Touring'
			else 'N/A'
		end prd_line,
		cast(prd_start_dt as date) as prd_start_date,
		cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)- 1 as date) as prd_end_dt
		from bronze.crm_prd_info
		set @end_time=GETDATE();
		print '>> Time required: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print '---------------------';

		set @start_time=GETDATE();
		print '>> Truncating table: silver.crm_sales_details';
		truncate table silver.crm_sales_details;
		print '>> Loading into: silver.crm_sales_details';
		insert into silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		select
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case when len(sls_order_dt) != 8 or sls_order_dt = 0 then null
				 else cast(cast(sls_ship_dt as varchar) as date)
			end sls_order_dt,
			case when len(sls_ship_dt) != 8 or sls_ship_dt = 0 then null
				 else cast(cast(sls_ship_dt as varchar) as date)
			end sls_ship_dt,
			case when len(sls_due_dt) != 8 or sls_due_dt = 0 then null
				 else cast(cast(sls_due_dt as varchar) as date)
			end sls_due_dt,
			case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
					then sls_quantity * abs(sls_price)
				 else sls_sales
			end sls_sales,
			sls_quantity,
			case when sls_price is null or sls_price <= 0
					then sls_sales / nullif(sls_quantity,0)
				 else sls_price
			end sls_price
		from bronze.crm_sales_details
		set @end_time=GETDATE();
		print '>> Time required: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print '---------------------';

		print '---------------------------------------------------------------------------------------';
		print 'Loading ERP data into tables';
		print '---------------------------------------------------------------------------------------';

		set @start_time=GETDATE();
		print '>> Truncating table: silver.erp_cust_az12';
		truncate table silver.erp_cust_az12;
		print '>> Loading into: silver.erp_cust_az12';
		insert into silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		select 
			case when cid like 'NAS%' then substring(cid,4,len(cid))
				 else cid
			end cid,
			case when bdate > GETDATE() then null
				 else bdate
			end bdate,
			case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
				 when upper(trim(gen)) in ('M','MALE') then 'Male'
				 else 'N/A'
			end gen
		from bronze.erp_cust_az12
		set @end_time=GETDATE();
		print '>> Time required: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print '---------------------';

		set @start_time=GETDATE();
		print '>> Truncating table: silver.erp_loc_a101';
		truncate table silver.erp_loc_a101;
		print '>> Loading into: silver.erp_loc_a101';
		insert into silver.erp_loc_a101(
			cid,
			cntry
		)
		select 
			REPLACE(cid, '-','') as cid,
			case when upper(trim(cntry)) = 'DE' then 'Germany'
				 when upper(trim(cntry)) in ('US','USA') then 'United States'
				 when trim(cntry) = '' or cntry is  null then 'N/A'
			else cntry
		end cntry
		from bronze.erp_loc_a101
		set @end_time=GETDATE();
		print '>> Time required: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print '---------------------';

		set @start_time=GETDATE();
		print '>> Truncating table: silver.erp_px_cat_g1v2';
		truncate table silver.erp_px_cat_g1v2;
		print '>> Loading into: silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)
		select
			id,
			cat,
			subcat,
			maintenance
		from bronze.erp_px_cat_g1v2
		set @end_time=GETDATE();
		set @full_end_time = GETDATE();
		print '>> Time required: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print '---------------------';
		print '>> Full time required: ' + cast(datediff(second,@full_start_time,@full_end_time) as nvarchar) + ' seconds';
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
