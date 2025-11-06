
IF OBJECT_ID ('Bronz.crm_cust_info', 'U') IS NOT NULL 
  DROP TABLE Bronz.crm_cust_info; 
CREATE TABLE Bronz.crm_cust_info (
cst_id INT, 
cst_key NVARCHAR(50), 
cst_firstname NVARCHAR(50), 
cst_lastname NVARCHAR(50), 
cst_material_statuts NVARCHAR(50), 
cst_gndr NVARCHAR(50), 
cst_create_date DATE
); 

IF OBJECT_ID ('Bronz.crm_prd_info', 'U') IS NOT NULL 
  DROP TABLE Bronz.crm_prd_info;
CREATE TABLE Bronz.crm_prd_info (
prd_id INT, 
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50), 
prd_cost INT,
prd_line NVARCHAR(50), 
prd_start_dt DATETIME, 
prd_end_dt NVARCHAR(50)
);

IF OBJECT_ID ('Bronz.crm_sales_details', 'U') IS NOT NULL 
  DROP TABLE Bronz.crm_sales_details;
CREATE TABLE Bronz.crm_sales_details (
sls_ord_num NVARCHAR(50), 
sls_prd_key NVARCHAR(50),
sls_cust_id INT, 
sls_order_dt INT, 
sls_ship_dt INT, 
sls_due_dt INT, 
sls_sales INT, 
sls_quantity INT, 
sls_price INT
);

IF OBJECT_ID ('Bronz.erp_loc_a101', 'U') IS NOT NULL 
  DROP TABLE Bronz.erp_loc_a101;
CREATE TABLE Bronz.erp_loc_a101 (
    cid NVARCHAR(50),
	cntry NVARCHAR(50)
); 
GO 

IF OBJECT_ID ('Bronz.erp_cust_az12', 'U') IS NOT NULL 
  DROP TABLE Bronz.erp_cust_az12;
CREATE TABLE Bronz.erp_cust_az12 (
    cid NVARCHAR(50),
	bdate DATE, 
	gen NVARCHAR(50)
);
GO 

IF OBJECT_ID ('Bronz.erp_px_cat_g1v2', 'U') IS NOT NULL 
  DROP TABLE Bronz.erp_px_cat_g1v2;
CREATE TABLE Bronz.erp_px_cat_g1v2 (
    id NVARCHAR(50),
	cat NVARCHAR(50), 
	subcat NVARCHAR(50),
	maintenace NVARCHAR(50)
);
GO 

CREATE OR ALTER PROCEDURE Bronz.load_bronze AS 
BEGIN 
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY 
	    SET @batch_start_time = GETDATE(); 
		PRINT '========================================';
		PRINT 'Loadin the Bronze Layer';
		PRINT '========================================';
		PRINT '------------------------------------------'; 
		PRINT 'Laoding crm Tables';
		PRINT '------------------------------------------'; 
        SET @start_time = GETDATE(); 
		TRUNCATE TABLE Bronz.crm_cust_info; --make sure that the table is empty 
		BULK INSERT Bronz.crm_cust_info 
		FROM 'C:\Users\HP\Desktop\Bureau\SQL YTB Tranning\Projects\DataWherhouse\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		 SET @end_time = GETDATE();
		 PRINT '>> load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Secondes';
		 PRINT '>>---------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE Bronz.crm_prd_info; --make sure that the table is empty 
		BULK INSERT Bronz.crm_prd_info 
		FROM 'C:\Users\HP\Desktop\Bureau\SQL YTB Tranning\Projects\DataWherhouse\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		 SET @end_time = GETDATE();
		 PRINT '>> load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Secondes';
		 PRINT '>>---------------';


		SET @start_time = GETDATE();
		TRUNCATE TABLE Bronz.crm_sales_details; --make sure that the table is empty 
		BULK INSERT Bronz.crm_sales_details 
		FROM 'C:\Users\HP\Desktop\Bureau\SQL YTB Tranning\Projects\DataWherhouse\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
         SET @end_time = GETDATE();
		 PRINT '>> load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Secondes';
		 PRINT '>>---------------';

		PRINT '------------------------------------------'; 
		PRINT 'Laoding erp Tables';
		PRINT '------------------------------------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE Bronz.erp_loc_a101; --make sure that the table is empty 
		BULK INSERT Bronz.erp_loc_a101 
		FROM 'C:\Users\HP\Desktop\Bureau\SQL YTB Tranning\Projects\DataWherhouse\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		 SET @end_time = GETDATE();
		 PRINT '>> load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Secondes';
		 PRINT '>>---------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE Bronz.erp_cust_az12; --make sure that the table is empty 
		BULK INSERT Bronz.erp_cust_az12 
		FROM 'C:\Users\HP\Desktop\Bureau\SQL YTB Tranning\Projects\DataWherhouse\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		);

		SET @start_time = GETDATE();
		TRUNCATE TABLE Bronz.erp_px_cat_g1v2; --make sure that the table is empty 
		BULK INSERT Bronz.erp_px_cat_g1v2 
		FROM 'C:\Users\HP\Desktop\Bureau\SQL YTB Tranning\Projects\DataWherhouse\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		 SET @end_time = GETDATE();
		 PRINT '>> load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Secondes';
		 PRINT '>>---------------';
		 PRINT 'Loading Bronz Layer is Compleated' ;
		 PRINT '======================================';
		 SET  @batch_end_time = GETDATE();
		 PRINT '>> Whol Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Secondes';
		 PRINT '======================================';
	END TRY 
	BEGIN CATCH 
		PRINT '============================================='; 
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR Number' + ERROR_NUMBER(); 
		PRINT 'ERROR STATE' + ERROR_STATE();	
		PRINT '============================================='; 
	END CATCH 
END 

-- this is the script to load information from the sources, so if we have to do it frequently then ze must create a stored procedure
-- the sorted procedure is above create procedure...
