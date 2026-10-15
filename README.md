# fmcg_databricks_project
This is a multi-task Lakeflow Job that implements a medallion architecture (Bronze → Silver → Gold) for incrementally loading and transforming FMCG (Fast-Moving Consumer Goods) order data. The pipeline processes dimension data first, then fact (order) data, and finally aggregates results into a parent-level monthly summary table.
