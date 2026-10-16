# fmcg_databricks_project
This is a multi-task Lakeflow Job that implements a medallion architecture (Bronze → Silver → Gold) for incrementally loading and transforming FMCG (Fast-Moving Consumer Goods) order data. The pipeline processes dimension data first, then fact (order) data, and finally aggregates results into a parent-level monthly summary table.

FMCG Incremental Load Pipeline
Job: FMCG Incremental Load · Trigger: File Arrival on S3

Overview
This is a multi-task Lakeflow Job that implements a medallion architecture (Bronze → Silver → Gold) for incrementally loading and transforming FMCG (Fast-Moving Consumer Goods) order data. It is triggered automatically when new files arrive in the S3 landing zone (s3://project-databricks-practice/). The pipeline processes dimension data first, then fact (order) data, and finally aggregates results into a parent-level monthly summary table.

Task Execution Flow
The job runs 4 sequential tasks with dependency chaining (each depends on the previous):

#
Task Key
Notebook
Purpose
1
dim_processing_customers
1_cust_data_processing
Processes customer dimension data
2
dim_processing_products
2_products_data_processing
Processes product dimension data
3
dim_processing_pricing
3_pricing_data_processing
Processes gross price dimension data
4
fact_processing_orders
2_incremental_load_fact
Incremental order fact load (core)
Core Notebook: 2_incremental_load_fact
The final task is the heart of the pipeline and operates in three layers:

Bronze (Raw Ingestion)

Reads new CSV order files from s3://project-databricks-practice/orders/landing/ with schema inference and metadata capture (file name, size, read timestamp)
Appends to fmcg.bronze.orders (Delta table with Change Data Feed enabled)
Writes a snapshot to staging table fmcg.bronze.staging_orders for isolated incremental processing
Moves processed files from /landing/ to /processed/ to prevent re-ingestion
Silver (Cleaned & Enriched)

Reads from the bronze staging table
Cleans and standardizes data:
Filters out records with null order_qty
Normalizes customer_id — non-numeric values replaced with "999999"
Cleans order_placement_date — strips weekday prefixes and handles multiple date formats (dd-MM-yyyy, dd/MM/yyyy, yyyy/MM/dd)
Joins with fmcg.silver.products to enrich with product_code
MERGE (upsert) into fmcg.silver.orders on composite key (order_placement_date, order_id, product_code, customer_id)
Writes the joined result to staging fmcg.silver.staging_orders
Gold (Aggregated Fact)

Selects business-relevant columns from silver staging: order_id, date, customer_code, product_code, product_id, sold_quantity
MERGE into fmcg.gold.sb_fact_orders on (date, product_code, customer_code)
Parent Company Monthly Recalculation

Extracts the affected month(s) from the incremental batch
Pulls all gold-level records for those month(s) from fmcg.gold.fact_orders
Recalculates monthly aggregation: SUM(sold_quantity) grouped by month_start, product_code, customer_code
MERGE the recalculated monthly figures back into the parent table fmcg.gold.fact_orders on (date, product_code, customer_code)
Cleanup

Drops both staging tables (fmcg.bronze.staging_orders, fmcg.silver.staging_orders) to keep the catalog clean for the next run
Key Design Patterns
Incremental via staging tables: Each run isolates only the newly arrived data using staging tables, avoiding full table scans
MERGE (upsert) everywhere: Both silver and gold layers use Delta MERGE to idempotently update existing records and insert new ones
Monthly recalculation: The gold→parent merge re-aggregates the entire affected month, ensuring corrections to individual orders propagate to the monthly summary
Change Data Feed: Enabled on bronze and silver tables to support downstream CDC use cases
File archival: Processed CSVs are moved from landing to processed directory after each run
Current Status
The most recent run (triggered by file arrival) failed — the fact_processing_orders task encountered an execution error, causing all downstream tasks to be skipped. This may warrant investigation in the run output logs.
