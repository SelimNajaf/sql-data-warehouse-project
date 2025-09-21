-----------------------------------------------------------------------------
--  FULL PROJECT – End-to-End Bronze ➜ Silver ➜ Gold Pipeline
-----------------------------------------------------------------------------
--  This single script executes the complete data-lakehouse life-cycle:
--  1.  Re-create bronze, silver and gold schemas (destructive).
--  2.  Build raw bronze tables and bulk-load them from local CSV files.
--  3.  Build cleaned silver tables and populate them with idempotent SQL
--      (deduplication, data-type repair, surrogate end-dates, NULL defence).
--  4.  Build the gold analytical layer as three star-schema views:
--        - dim_costumers  : conformed customer dimension
--        - dim_products   : type-2 style product dimension (current version)
--        - fact_sales     : transactional fact linked via surrogate keys
-----------------------------------------------------------------------------
--  Run this script end-to-end whenever you want a full refresh of the
--  entire data stack (e.g. nightly batch, on-demand rebuild, CI test).
-----------------------------------------------------------------------------
--  Assumptions
--    - MySQL ≥ 8.0  (window functions, CTEs, TRUNCATE/LOAD DATA LOCAL)
--    - CSV files live in operator’s desktop folder
--    - `LOCAL_INFILE=1` enabled on server + client
--    - Execution window tolerates full reload (no delta loads herein)
-----------------------------------------------------------------------------


-- 0.  (RE-)CREATE THREE LAYER SCHEMAS --------------------------------------

DROP DATABASE IF EXISTS bronze;
DROP DATABASE IF EXISTS silver;
DROP DATABASE IF EXISTS gold;



CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;


-- 1.  BRONZE TABLES – RAW LANDING ------------------------------------------

-- 1.1  CRM Customer Master 
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);

-- 1.2  CRM Product Master
DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE
);

-- 1.3  CRM Sales Transactions
DROP TABLE IF EXISTS bronze.crm_sales_detail;
CREATE TABLE bronze.crm_sales_detail(
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

-- 1.4  ERP Customer Attributes
DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
	cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
);

-- 1.5  ERP Customer Geography
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
	cid NVARCHAR(50),
    cntry NVARCHAR(50)
);

-- 1.6  ERP Product Category
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
	id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);


-- 2.  BULK CSV LOADS INTO BRONZE -------------------------------------------

TRUNCATE TABLE bronze.crm_cust_info;
LOAD DATA LOCAL INFILE '/Users/selim/Desktop/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

TRUNCATE TABLE bronze.crm_prd_info;
LOAD DATA LOCAL INFILE '/Users/selim/Desktop/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

TRUNCATE TABLE bronze.crm_sales_detail;
LOAD DATA LOCAL INFILE '/Users/selim/Desktop/sales_details.csv'
INTO TABLE bronze.crm_sales_detail
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

TRUNCATE TABLE bronze.erp_loc_a101;
LOAD DATA LOCAL INFILE '/Users/selim/Desktop/LOC_A101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

TRUNCATE TABLE bronze.erp_cust_az12;
LOAD DATA LOCAL INFILE '/Users/selim/Desktop/CUST_AZ12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
LOAD DATA LOCAL INFILE '/Users/selim/Desktop/PX_CAT_G1V2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


-- 3.  SILVER TABLES – CLEANSED & CONFORMED ---------------------------------

-- 3.1  De-duplicated Customer Master 
DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);

TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info
(
	cst_id, 
	cst_key, 
	cst_firstname, 
	cst_lastname, 
	cst_marital_status, 
	cst_gndr, 
	cst_create_date
)
WITH cte AS
(
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_list
	FROM bronze.crm_cust_info
)
SELECT 
	cst_id, 
	cst_key, 
	TRIM(cst_firstname) as cst_firstname, 
	TRIM(cst_lastname) as cst_lastname, 
	CASE UPPER(TRIM(cst_marital_status)) 
		WHEN 'M' THEN 'Married'
		WHEN 'S' THEN 'Single'
		ELSE 'n/a'
	END cst_marital_status,
	CASE UPPER(TRIM(cst_gndr))
		WHEN 'M' THEN 'Male'
		WHEN 'F' THEN 'Female'
		ELSE 'n/a'
	END cst_gndr, 
	cst_create_date
FROM cte
WHERE flag_list = 1 AND cst_id <> 0;

-- 3.2  Product Master with Derived Category & End-Date
DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
	prd_id INT,
    cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE
);

TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info
(
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
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_' )AS cat_id,
	SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
	prd_nm, 
	IFNULL(prd_cost, 0) AS prd_cost, 
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'T' THEN 'Touring'
		WHEN 'S' THEN 'Other Sales'
		ELSE 'n/a'
	END prd_line, 
	prd_start_dt, 
	LEAD(prd_start_dt, 1) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)- INTERVAL 1 DAY AS prd_end_dt
FROM bronze.crm_prd_info;

-- 3.3  Sales Facts – Defensive Date & Money Fix
DROP TABLE IF EXISTS silver.crm_sales_detail;
CREATE TABLE silver.crm_sales_detail(
	sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

TRUNCATE TABLE silver.crm_sales_detail;
INSERT INTO silver.crm_sales_detail
(
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
		WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) <> 8 THEN NULL
		ELSE CAST(sls_order_dt AS DATE)
	END sls_order_dt,
	CASE
		WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) <> 8 THEN NULL
		ELSE CAST(sls_ship_dt AS DATE)
	END sls_ship_dt,
	CASE
		WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) <> 8 THEN NULL
		ELSE CAST(sls_due_dt AS DATE)
	END sls_due_dt,
	CASE
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> ABS(sls_price)*sls_quantity AND sls_price <> 0 THEN ABS(sls_price)*sls_quantity
		ELSE sls_sales
	END sls_sales,
	sls_quantity, 
	CASE
		WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity,0)
		ELSE sls_price
	END sls_price
FROM bronze.crm_sales_detail;

-- 3.4  ERP Customer Attributes – NAS Strip & Future Date Guard
DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
	cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
);

TRUNCATE TABLE silver.erp_cust_az12;
INSERT INTO silver.erp_cust_az12
(
	cid, 
	bdate, 
	gen
)
SELECT 
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
		ELSE cid
	END cid,
	CASE
		WHEN bdate > CURDATE() THEN NULL
		ELSE bdate
	END bdate,
	CASE
		WHEN UPPER(TRIM(gen)) LIKE 'F%' THEN 'Female'
		WHEN UPPER(TRIM(gen)) LIKE 'M%' THEN 'Male'
		ELSE 'n/a'
	END gen
FROM bronze.erp_cust_az12;

-- 3.5  ERP Customer Geography – Key Normalise & Country Map
DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
	cid NVARCHAR(50),
    cntry NVARCHAR(50)
);

TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101
(
	cid, 
	cntry
)
SELECT 
	REPLACE(cid,'-','') cid,
	CASE 
		WHEN TRIM(cntry) = '' OR TRIM(cid) IS NULL THEN 'n/a'
		WHEN TRIM(cntry) LIKE 'DE%' THEN 'Germany'
		WHEN TRIM(cntry) LIKE 'US%' THEN 'United States'
		ELSE TRIM(cntry)
	END cntry
FROM bronze.erp_loc_a101;

-- 3.6  ERP Category Reference – Pass-Through
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
	id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);

TRUNCATE TABLE silver.erp_px_cat_g1v2;
INSERT INTO silver.erp_px_cat_g1v2
(
	id, 
	cat, 
	subcat, 
	maintenance
)
SELECT *
FROM bronze.erp_px_cat_g1v2;


-- 4.  GOLD VIEWS – STAR SCHEMA FOR ANALYTICS -------------------------------

-- 4.1  Customer Dimension (Surrogate Key + CRM+ERP Enrichment)
DROP VIEW IF EXISTS gold.dim_costumers;
CREATE VIEW gold.dim_costumers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) costumer_key,
	cst_id costumer_id, 
	cst_key costumer_number, 
	cst_firstname first_name, 
	cst_lastname last_name, 
	cst_marital_status marital_status, 
    CASE
		WHEN cst_gndr != 'n/a' THEN cst_gndr
        ELSE IFNULL(ca.gen,'n/a')
	END gender,
	cst_create_date create_date,
	ca.bdate birth_date,
	la.cntry country
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid;

-- 4.2  Product Dimension (Current Version Only)
DROP VIEW IF EXISTS gold.dim_products;
CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY prd_start_dt,prd_key) product_key,
	prd_id product_id, 
	prd_key product_number, 
	prd_nm product_name,
    cat_id category_id, 
	px.cat category,
    px.subcat subcategory,
    px.maintenance,
    prd_cost cost, 
    prd_line product_line, 
    prd_start_dt start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 px
	ON pn.cat_id = px.id
WHERE prd_end_dt IS NULL;

-- 4.3  Sales Fact (Links to Dimensions via Surrogate Keys)
DROP VIEW IF EXISTS gold.fact_sales;
CREATE VIEW gold.fact_sales AS
SELECT 
	sls_ord_num order_number, 
    pr.product_key, 
    cu.costumer_key, 
    sls_order_dt order_date, 
    sls_ship_dt shipping_date, 
    sls_due_dt due_date, 
    sls_sales sales_amount, 
    sls_quantity quantity, 
    sls_price price
FROM silver.crm_sales_detail sd
LEFT JOIN gold.dim_products pr
	ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_costumers cu
	ON sd.sls_cust_id = cu.costumer_id;

