CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN 
	declare @start_time datetime, @end_time datetime;
	declare @start_procedure_time datetime, @end_procedure_time datetime;
	BEGIN TRY
			set @start_procedure_time = GETDATE();
			PRINT '========================'
			PRINT 'LOADING BRONZE LAYER'
			PRINT '========================'

			PRINT '------------------------'
			PRINT 'Loading CRM layer'
			PRINT '------------------------'

			set @start_time = GETDATE();
			PRINT '>> Truncating Table:bronze.crm_cust_info'
			TRUNCATE TABLE bronze.crm_cust_info

			Print '>> Inserting data into:bronze.crm_cust_info'
			BULK INSERT bronze.crm_cust_info
			from 'D:\sql_data_warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
				FIRSTROW=2,
				FIELDTERMINATOR = ',',
				TABLOCK 
			);
			set @end_time=GETDATE();
			print '^^ Load duration: ' + cast(datediff(second,@start_time,@end_time ) as  nvarchar) + ' seconds';
			print '-----------------------------------------';
			print '                                                   ';

			set @start_time = GETDATE();
			PRINT '>> Truncating Table:bronze.crm_prd_info'
			TRUNCATE TABLE bronze.crm_prd_info
			Print '>> Inserting data into:bronze.crm_prd_info'
			BULK INSERT bronze.crm_prd_info
			from 'D:\sql_data_warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
				FIRSTROW=2,
				FIELDTERMINATOR = ',',
				TABLOCK 
			);
			set @end_time=GETDATE();
			print '^^ Load duration: ' + cast(datediff(second,@start_time,@end_time ) as  nvarchar) + ' seconds';
			print '-----------------------------------------';
			print '                                                   ';

			set @start_time = GETDATE();
			PRINT '>> Truncating Table:bronze.crm_sales_details'
			TRUNCATE TABLE bronze.crm_sales_details
			Print '>> Inserting data into:bronze.crm_sales_details'
			BULK INSERT bronze.crm_sales_details
			FROM 'D:\sql_data_warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
				FIRSTROW=2,
				FIELDTERMINATOR = ',',
				TABLOCK 
			);
			set @end_time=GETDATE();
			print '^^ Load duration: ' + cast(datediff(second,@start_time,@end_time ) as  nvarchar) + ' seconds';
			print '-----------------------------------------';
			print '                                                   ';

			------------------------------------------------------------------------------------------------------------------------------------------------
			PRINT '-----------------------------'
			PRINT 'Loading ERP layer'
			PRINT '-----------------------------'

			set @start_time = GETDATE();
			PRINT '>> Truncating Table:bronze.erp_cust_az12'
			TRUNCATE TABLE bronze.erp_cust_az12
			Print '>> Inserting data into:bronze.erp_cust_az12'
			BULK INSERT bronze.erp_cust_az12
			from 'D:\sql_data_warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH (
				FIRSTROW=2,
				FIELDTERMINATOR = ',',
				TABLOCK 
			);
			set @end_time=GETDATE();
			print '^^ Load duration: ' + cast(datediff(second,@start_time,@end_time ) as  nvarchar) + ' seconds';
			print '-----------------------------------------';
			print '                                                   ';


			set @start_time = GETDATE();
			PRINT '>> Truncating Table:bronze.erp_loc_a101'
			TRUNCATE TABLE bronze.erp_loc_a101
			Print '>> Inserting data into:bronze.erp_loc_a101'
			BULK INSERT bronze.erp_loc_a101
			from 'D:\sql_data_warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH (
				FIRSTROW=2,
				FIELDTERMINATOR = ',',
				TABLOCK 
			);
				set @end_time=GETDATE();
			print '^^ Load duration: ' + cast(datediff(second,@start_time,@end_time ) as  nvarchar) + ' seconds';
			print '-----------------------------------------';
			print '                                                   ';


			set @start_time = GETDATE();
			PRINT '>> Truncating Table:bronze.erp_px_cat_g1v2'
			TRUNCATE TABLE bronze.erp_px_cat_g1v2
			Print '>> Inserting data into:bronze.erp_px_cat_g1v2'
			BULK INSERT bronze.erp_px_cat_g1v2
			from 'D:\sql_data_warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH (
				FIRSTROW=2,
				FIELDTERMINATOR = ',',
				TABLOCK 
			);
			set @end_time=GETDATE();
			print '^^ Load duration: ' + cast(datediff(second,@start_time,@end_time ) as  nvarchar) + ' seconds';
			print '-----------------------------------------';


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