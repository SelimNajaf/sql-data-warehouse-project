/*-----------------------------------------------------------------------------
--  Bronze / Raw Layer – Initial Table Creation & CSV Load
-------------------------------------------------------------------------------
--  This script drops and re-creates six raw-data tables under the `bronze`
--  schema, then populates them from local CSV files. It is intended to be
--  executed once, right after the bronze database/schema has been created.
-------------------------------------------------------------------------------
--  Tables covered
--    crm_cust_info      : customer master coming from the CRM system
--    crm_prd_info       : product master coming from the CRM system
--    crm_sales_detail   : order-level sales transactions from CRM
--    erp_cust_az12      : additional customer attributes (birth-date, gender)
--                         exported from ERP table AZ12
--    erp_loc_a101       : customer country information from ERP table A101
--    erp_px_cat_g1v2    : product category / sub-category & maintenance flag
--                         exported from ERP table PX_CAT_G1V2
-------------------------------------------------------------------------------
--  Assumptions
--    - MySQL ≥ 5.7 (or MariaDB ≥ 10.2)
--    - `bronze` schema already exists
--    - `LOCAL_INFILE=1` is enabled on server and client
--    - CSV files reside in the operator’s desktop folder:
--         /Users/selim/Desktop/<file>.csv
--    - Each CSV contains a header row that must be skipped (IGNORE 1 LINES)
-------------------------------------------------------------------------------*/

/* ---------- 1. CRM CUSTOMER MASTER ---------- */
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info
(
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);

/* ---------- 2. CRM PRODUCT MASTER ---------- */
DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info
(
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE
);

/* ---------- 3. CRM SALES TRANSACTIONS ---------- */
DROP TABLE IF EXISTS bronze.crm_sales_detail;
CREATE TABLE bronze.crm_sales_detail
(
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

/* ---------- 4. ERP CUSTOMER AUX (AZ12) ---------- */
DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12
(
    cid  NVARCHAR(50),
    bdate DATE,
    gen  NVARCHAR(50)
);

/* ---------- 5. ERP LOCATION (A101) ---------- */
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101
(
    cid   NVARCHAR(50),
    cntry NVARCHAR(50)
);

/* ---------- 6. ERP PRODUCT CATEGORY (PX_CAT_G1V2) ---------- */
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2
(
    id          NVARCHAR(50),
    cat         NVARCHAR(50),
    subcat      NVARCHAR(50),
    maintenance NVARCHAR(50)
);

/* -------------------------------------------------------------------------- */
/*  L O A D   C S V   F I L E S                                               */
/* -------------------------------------------------------------------------- */

LOAD DATA LOCAL INFILE '/Users/selim/Desktop/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '/Users/selim/Desktop/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '/Users/selim/Desktop/sales_details.csv'
INTO TABLE bronze.crm_sales_detail
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '/Users/selim/Desktop/LOC_A101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '/Users/selim/Desktop/CUST_AZ12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '/Users/selim/Desktop/PX_CAT_G1V2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
