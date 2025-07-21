create or alter procedure silver.load_silver as

begin
	declare @start_time datetime, @end_time datetime;
	declare @start_procedure_time datetime, @end_procedure_time datetime;
	BEGIN TRY
			set @start_procedure_time = GETDATE();

	print '>> truncate and insert in table silver.crm_cust_info'
	truncate table silver.crm_cust_info
	insert into silver.crm_cust_info  (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)

	select 
		cst_id,
		cst_key,
		trim(cst_firstname),
		trim(cst_lastname),
		case when upper(trim(cst_marital_status))='S' then 'Single'
			 when upper(trim(cst_marital_status))='M' then 'Married'
			 else 'n/a'
		end cst_marital_status, -- normalize martial status values to readable format

		case when upper(trim(cst_gndr))='F' then 'Female'
			 when upper(trim(cst_gndr))='M' then 'Male'
			 else 'n/a'
		end cst_gndr, -- normalize gender values to readable format
		cst_create_date

	from (
			select 
				*,
				ROW_NUMBER() over  (partition by cst_id order by cst_create_date desc) as flag_last
			from bronze.crm_cust_info
			where cst_id is not null
		 )t 
		 where flag_last=1 -- select the most recent record per cst_id


	-------------------------------------------------------------------------

	print '>> truncate and insert in table silver.crm_prd_info'
	truncate table silver.crm_prd_info
	insert into silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT
		prd_id,
		REPLACE( SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id, -- extract category ID
		SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,			-- extract product key
		prd_nm,
		ISNULL(prd_cost, 0) as prd_cost,
		case UPPER(TRIM(prd_line))
			when 'M' then 'Mountain'
			when 'R' then 'Road'
			when 'S' then 'other Sales'
			when 'T' then 'Touring'
			else 'n/a'
		end prd_line, -- map product line codes to descriptve values 
		cast (prd_start_dt as date) prd_start_dt,
		--prd_end_dt`
		cast (LEAD(prd_start_dt) OVER (PARTITION BY prd_key order by prd_start_dt)-1 as date) as prd_end_dt
	FROM bronze.crm_prd_info

	----------------------------------------------------------------------------------

	print '>> truncate and insert in table silver.crm_sales_details'
	truncate table silver.crm_sales_details
	INSERT INTO silver.crm_sales_details (
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
			SELECT 
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				CASE 
					WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
				END AS sls_order_dt,
				CASE 
					WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
				END AS sls_ship_dt,
				CASE 
					WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
				END AS sls_due_dt,
				CASE 
					WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
						THEN sls_quantity * ABS(sls_price)
					ELSE sls_sales
				END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
				sls_quantity,
				CASE 
					WHEN sls_price IS NULL OR sls_price <= 0 
						THEN sls_sales / NULLIF(sls_quantity, 0)
					ELSE sls_price  -- Derive price if original value is invalid
				END AS sls_price
			FROM bronze.crm_sales_details;

	-------------------------------------------------------------------------------------------------------

	print '>> truncate and insert in table silver.erp_cust_az12'
	truncate table silver.erp_cust_az12
	insert into silver.erp_cust_az12(
	cid,
	bdate,
	gen
	)
	select 
		CASE	WHEN cid like 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- remove  'NAS' prefix
				ELSE cid
		END cid,

		CASE	WHEN bdate > getdate() then null
				ELSE bdate
		END bdate,

		CASE	WHEN upper(trim(gen)) in ('F','FEMALE') then 'Female' 
				WHEN upper(trim(gen)) in ('M','MALE') then 'Male'
				ELSE 'n/a'
		END gen -- normalize genfer values and handle unknown cases
	from bronze.erp_cust_az12

	-----------------------------------------------------------------------------------------------------------

	print '>> truncate and insert in table silver.erp_loc_a101'
	truncate table silver.erp_loc_a101
	insert into silver.erp_loc_a101(
	cid,
	cntry
	)
	select 
	replace(cid,'-','') cid,
	case	when trim (cntry) in ('DE','Germany') then 'Germany'
			when trim (cntry) in ('US', 'USA', 'United States') then 'United States'
			when trim (cntry) = '' or cntry is null then 'n/a'
			else cntry	
	end cntry -- normalize and handle missing or bland country codes
	from bronze.erp_loc_a101

	----------------------------------------------------------------------------------------------------------

	print '>> truncate and insert in table silver.erp_px_cat_g1v2'
	truncate table silver.erp_px_cat_g1v2
	insert into silver.erp_px_cat_g1v2 (
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

END TRY
BEGIN CATCH
	PRINT '================================================'
	PRINT '!! ERROR OCCURED DURING LOADING BRONZE LAYER !!'
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + Cast(ERROR_NUMBER() as varchar);
	PRINT 'Error Message' + Cast(error_state() as varchar);
	PRINT '================================================'
END CATCH
set @end_procedure_time = GETDATE();
print '                          '
print '>>>procedure ended successfully<<<'
print '>>>procedure duration: ' + cast(datediff(second,@start_procedure_time, @end_procedure_time ) as  nvarchar) + ' seconds<<<'; 

end













