----------------------------------------------------------------------------- 
-- Gold / Presentation Layer – Core Star-Schema Views
----------------------------------------------------------------------------- 
-- This script drops and re-creates three analytical views under the gold 
-- schema: one customer dimension, one product dimension and one sales fact. 
-- All surrogate keys are generated at query-time with ROW_NUMBER(), ensuring 
-- every refresh is deterministic and independent of previous runs.
----------------------------------------------------------------------------- 
-- Views covered -- dim_costumers : conformed customer master with 
-- slowly-changing attributes 
-- (gender, birth-date, country) merged from CRM & ERP 
-- dim_products : clean product hierarchy (category, sub-category, line) 
-- filtered to the currently valid version only 
-- fact_sales : transaction-level sales facts linked to the two dimensions 
-- above via surrogate keys
----------------------------------------------------------------------------- 
--  Assumptions
--    - MySQL ≥ 8.0 (window functions & CTE support)
--    - silver tables are already loaded and cleansed
--    - gold schema exists and is accessible to BI tools
--    - Views are materialised externally if performance requires it
----------------------------------------------------------------------------- 


-- 1. GOLD CUSTOMER DIMENSION – SURROGATE-KEY + CRM+ERP ENRICH

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


-- 2. GOLD PRODUCT DIMENSION – CURRENT VERSION ONLY

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

-- 3. GOLD SALES FACT – LINKED TO DIMS VIA SURROGATE KEYS

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


