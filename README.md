# FMCG Databricks Project: AtliQon BI 360 Sales Analytics Pipeline

This is a multi-task Lakeflow Job that implements a medallion architecture (Bronze → Silver → Gold) for incrementally loading and transforming FMCG (Fast-Moving Consumer Goods) order data. The pipeline processes dimension data first, then fact (order) data, and finally aggregates results into a parent-level monthly summary table.

## Business Context

AtliQon is an FMCG company selling nutrition bars, granola, healthy snacks, and electrolyte products. There are two companies in this data ecosystem:

*   **Parent company:** Its data already lives in the gold tables, fed by a separate, pre-existing pipeline.
*   **Child company:** The data this project processes. The child company's cleaned data is merged into the parent company's gold tables so the business sees a single unified view.

The pipeline ingests raw CSV files from an S3 bucket (`s3://project-databricks-practice/`), cleans and conforms them through a medallion architecture (Bronze → Silver → Gold), and finally serves them through a denormalized view and an interactive dashboard.

## Project Folder Structure

| Folder | Purpose |
| :--- | :--- |
| `1_setup (1)` | One-time setup: creates the `fmcg` catalog, schemas, utility variables, and the date dimension table. |
| `2_dim_data_processing_` | Processes the three dimension tables — customers, products, and gross price. |
| `3_fact_data_processing_` | Processes the fact (orders) table — both full load and incremental loads. |
| **Root queries** | `denormalized_view_for_dashboarding` (the serving view) and `Increamental_parent_comny_frm_csv` (merges child into parent tables). |
| **Dashboard** | AtliQon BI 360 — the final BI dashboard. |

---

## Step 1 — Setup (`1_setup (1)`)

Three notebooks run once to bootstrap the environment:

*   **`1_setup`**: Creates the `fmcg` catalog and the three schemas: `bronze`, `silver`, `gold`.
*   **`utilities`**: Defines shared variables (`bronze_schema`, `silver_schema`, `gold_schema`) reused by every downstream notebook via `%run`.
*   **`dim_date_table_creation`**: Generates a calendar dimension table `fmcg.gold.dim_date` spanning Jan 2024 – Dec 2025, with columns like `date_key`, `year`, `month_name`, `quarter`, and `year_quarter`.

---

## Step 2 — Dimension Data Processing

Each dimension follows the same three-layer pattern: **read raw CSV from S3 → write to Bronze (raw, with Change Data Feed enabled) → clean and transform in Silver → publish a conformed table to Gold.**

### 1. Customer Dimension (`1_cust_data_processing`) → `fmcg.gold.dim_customers`
*   Deduplicates on `customer_id`, trims and title-cases `customer_name`.
*   Fixes city typos (e.g., *Bengaluruu* → *Bengaluru*, *NewDheli* → *New Delhi*).
*   Fills null cities via a manual `customer_id` → `city` mapping.
*   **Final columns:** `customer_code`, `customer`, `market`, `platform`, `channel`, `city`.

### 2. Product Dimension (`2_products_data_process...`) → `fmcg.gold.dim_products`
*   Deduplicates on `product_id`, title-cases `category`.
*   Fixes the spelling "Protien" → "Protein" across product name and category.
*   Derives a higher-level division from category (e.g., *Energy Bars* and *Protein Bars* → "Nutrition Bars"; *Granola & Cereals* → "Breakfast Foods").
*   **Final columns:** `product_code`, `division`, `category`, `product`, `variant`, `product_id`.

### 3. Gross Price Dimension (`3_pricing_data_processi...`) → `fmcg.gold.dim_gross_price`
*   Parses the `month` column using multiple date formats (handles mixed yyyy/MM/dd, dd/MM/yyyy, etc.).
*   Flips negative gross prices to positive, sets non-numeric values to 0.
*   Joins with the silver products table to attach `product_code`.
*   **Final columns:** `product_code`, `month` (as date), `gross_price`.

---

## Step 3 — Fact Data Processing (Orders)

This layer uses a landing/processed folder pattern on S3 to support incremental loading. New order CSV files arrive in the `orders/landing` folder; after processing they are moved to `orders/processed`.

### Full Load (`1_full_load`) → `fmcg.gold.fact_orders`
*   Reads all CSVs from landing, appends them to the bronze table (`fmcg.bronze.orders`) with Change Data Feed enabled.
*   Moves processed files to the processed folder.
*   **Silver transformations:** Filters out null `order_qty`, replaces non-numeric `customer_id` values with `999999`, strips the day-of-week prefix from `order_placement_date`, parses dates in multiple formats, and joins with the products dimension to get `product_code`.
*   Uses a `MERGE` pattern (create-if-not-exists, otherwise Delta MERGE) to upsert into `fmcg.silver.fact_orders`.
*   **Gold:** Selects clean columns (`order_id`, `date`, `customer_code`, `product_code`, `product_id`, `sold_quantity`) and writes to `fmcg.gold.fact_orders`.

### Incremental Load (`2_incremental_load_fact`)
*   Follows the same flow, but only processes newly-arrived files.
*   Uses a staging table (`staging_orders`) to isolate just the new batch.
*   Applies the same silver transformations, then `MERGE`-upserts into the silver and gold fact tables.
*   Finally, merges the child company's fact data into the parent company's gold tables via the `Increamental_parent_com...` query.

---

## Step 4 — Denormalized Serving View

The query `denormalized_view_for_d...` creates a single flat view — `fmcg.gold.vw_fact_orders_enriched` — that joins the fact table with all four dimensions:

| Join | Keys | Attributes brought in |
| :--- | :--- | :--- |
| `fact_orders` ⟕ `dim_date` | `date` = `month_start_date` | `date_key`, `year`, `month_name`, `quarter`, `year_quarter` |
| `fact_orders` ⟕ `dim_customers` | `customer_code` | `customer`, `market`, `platform`, `channel` |
| `fact_orders` ⟕ `dim_products` | `product_code` | `division`, `category`, `product`, `variant` |
| `fact_orders` ⟕ `dim_gross_price` | `product_code` + `year(date)` = `gp.year` | `price_inr` |

It also derives the key business metric: `total_amount_inr = sold_quantity × price_inr`.
*This denormalized view is the single source feeding the dashboard.*

---

## Step 5 — Dashboard (AtliQon BI 360)

Built directly on the `vw_fact_orders_enriched` view, the dashboard provides a 360° sales overview with the following widgets:

| Widget Type | Title | What it shows |
| :--- | :--- | :--- |
| Text box | "Sales Insights" | Dashboard heading |
| Counter | Total Revenue | `SUM(total_amount_inr)` — total sales revenue in INR |
| Counter | Total Quantity | `SUM(sold_quantity)` — total units sold |
| Filter (single-select) | Year | Filter the entire dashboard by year |
| Filter (single-select) | Quarter | Filter by quarter (Q1–Q4) |
| Filter (single-select) | Month Name | Filter by month (January–December) |
| Filter (single-select) | Channel | Filter by sales channel |
| Bar chart | Top 10 products by total revenue | Revenue ranked by product |
| Pie chart | Revenue By Channel | Revenue distribution across sales channels |

*All four filters (year, quarter, month, channel) are cross-filtering — selecting any combination instantly updates the counters, bar chart, and pie chart together.*

---

## End-to-End Flow Summary

```text
RAW S3 CSVs (Orders, Customers, Products, Prices)
   │
   ▼
BRONZE (Raw ingestion, Change Data Feed enabled)
   │  ── Data cleansing, standardization, derivations
   ▼
SILVER (Conformed tables, MERGE upsert logic)
   │  ── MERGE upsert, select business columns
   ▼
GOLD (Conformed facts & dimensions)
   │  ── join all dims into one flat view
   ▼
vw_fact_orders_enriched (denormalized view)
   │  ── merge child company into parent company
   ▼
AtliQon BI 360 DASHBOARD
```

**In short:** Raw order, customer, product, and price CSVs land on S3, get cleaned through the medallion layers, are enriched into a single denormalized view, and served on an interactive dashboard that lets business users slice revenue and quantity by time, product, and channel — combining both parent and child company data into one unified view.
