# 🚲 Bicycle-Sales Data-Lakehouse  
**MySQL | Bronze → Silver → Gold | Star-Schema | Idempotent SQL**


## 📌 What is this repo?
A complete, single-file, end-to-end data-lakehouse pipeline that turns raw CSV exports from a bicycle retailer's CRM and ERP systems into an analysis-ready star schema in **MySQL 8.x**.  
No external ETL tool required – everything is plain SQL that can be executed from any SQL client or CI runner.


## 🧱 Layers
| Layer  | Purpose |
|--------|---------|
| **bronze** | Raw CSV landing zone – exact copy of source files, quick reload possible at any time. |
| **silver** | Cleansed, conformed, de-duplicated data; business rules and surrogate dates applied. |
| **gold**   | Kimball-style star schema: two dimensions (`dim_costumers`, `dim_products`) and one fact (`fact_sales`) ready for Power BI / Tableau / Superset. |


## 🗂️ Source Files (place in `~/Desktop/`)
| File | System | Description |
|------|--------|-------------|
| `cust_info.csv` | CRM | Customer master |
| `prd_info.csv` | CRM | Product master (slowly-changing) |
| `sales_details.csv` | CRM | Order-line sales transactions |
| `CUST_AZ12.csv` | ERP | Additional customer attributes (birth-date, gender) |
| `LOC_A101.csv` | ERP | Customer country mapping |
| `PX_CAT_G1V2.csv` | ERP | Product category & sub-category reference |


## 🚀 Quick Start
1. Install MySQL 8.x and enable `LOCAL_INFILE=1` (server + client).  
2. Copy the six CSV files above into your desktop folder.  
3. Run the whole script once:

   ```bash
   mysql -u your_user -p &lt; full_project.sql
   ```
4. Connect BI tool to the gold schema and start analysing


## 🔍 Key Features
* **Idempotent**: drop & recreate every object → repeatable CI runs.  
* **Slowly-changing Product Dimension**: `LEAD()` window function generates `prd_end_dt` automatically.  
* **Defensive Cleansing**: trims, null-handles, date-validation, future-date guard, country mapping, gender decoding, etc.  
* **Surrogate Keys**: generated on-the-fly with `ROW_NUMBER()` – no sequence objects needed.  
* **Single File**: 100 % SQL, zero external dependencies.

## 🧪 Extending
* Swap `LOAD DATA LOCAL INFILE` for `AWS S3 LOAD` or `Azure Blob` if running in cloud.  
* Materialise gold views as tables if query-speed becomes critical.  
* Add more ERP sources – just follow the same bronze→silver→gold pattern.

## ⚖️ Licence
MIT – do whatever you want, just don't blame me if your bike gets a flat tyre.

## 👋 About Me
Data Analyst who turns coffee into pipelines and spreadsheets into dashboards.  
Always happy to connect and share tricks.

🔗 [LinkedIn](https://www.linkedin.com/in/selimnajaf-data-analyst/) | 📊 [Tableau Public](https://public.tableau.com/app/profile/selim.najaf/vizzes)
   
