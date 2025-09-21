-------------------------------------------------------------------------------
-- Silver / Cleansed Layer – Table Re-creation & Idempotent ELT
-------------------------------------------------------------------------------
-- This script rebuilds six cleansed tables under the silver schema and 
-- reloads them from their bronze counterparts. It is designed to be run 
-- every ingestion cycle (hourly, daily, etc.) in an idempotent way: drop, 
-- create, truncate, insert. All business-ready transformations (data-type 
-- fixes, deduplication, standardised codes, surrogate end-date calculation, 
-- NULL handling, etc.) are applied in-transit so that downstream analysts 
-- and star-schema loaders can consume the data without further cleansing.
-------------------------------------------------------------------------------
-- Tables covered 
-- crm_cust_info : de-duplicated customer master, gender & marital 
-- status normalised to full names 
-- crm_prd_info : product master with derived category id, readable 
-- product-line names and slowly-changing end-date 
-- crm_sales_detail : sales facts with defensive date casting and 
-- calculated/validated monetary columns 
-- erp_cust_az12 : ERP customer attributes (birth-date, gender) with 
-- NAS-prefix removal and future-date protection 
-- erp_loc_a101 : customer country mapping, hyphen-free keys and 
-- standardised country names 
-- erp_px_cat_g1v2 : product category & sub-category reference (pass-through)
-------------------------------------------------------------------------------
--  Assumptions
--    - MySQL ≥ 8.0 (window functions & CTE support)
--    - silver and bronze schemas already exist
--    - bronze tables were previously loaded by the raw-layer script
--    - Execution window allows full-table reload (truncate + insert)
-------------------------------------------------------------------------------


-- 1. SILVER CUSTOMER MASTER 

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

-- 2. SILVER PRODUCT MASTER

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

-- 3. SILVER SALES TRANSACTIONS

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

-- 4. SILVER ERP CUSTOMER ATTRIBUTES

DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
	cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
);

-- 5. SILVER ERP CUSTOMER GEOGRAPHY

DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
	cid NVARCHAR(50),
    cntry NVARCHAR(50)
);

-- 6. SILVER ERP PRODUCT CATEGORY REFERENCE

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
	id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);


-- 1. SILVER CUSTOMER MASTER – DEDUPE & STANDARDISE

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

-- 2. SILVER PRODUCT MASTER – DERIVE CAT_ID & END_DT

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

-- 3. SILVER SALES – DEFENSIVE DATE & MONEY FIX

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

-- 4. SILVER ERP CUST ATTR – STRIP NAS & FUTURE BDATE

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

-- 5. SILVER ERP GEO – NORMALISE KEY & COUNTRY

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

-- 6. SILVER ERP CAT REF – STRAIGHT LOAD

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


