# Wide World Importers - Microsoft Fabric Analytics Pipeline

## Overview
This project implements an end-to-end data engineering pipeline in Microsoft Fabric, extracting transactional data from an Azure SQL Database into a unified OneLake analytics model. By adopting a strict Medallion Architecture, this solution bridges the gap between operational constraints and enterprise business intelligence demands.

## Stakeholder Objectives & Solutions

This architecture was explicitly designed to resolve four core enterprise challenges:

*   **VP of Sales (Profitability & Velocity):** 
    *   *Solution:* Engineered Gold-layer Fact tables specifically aggregating regional sales velocity and calculating strict profit margins, exposed through high-speed semantic models for immediate dashboarding.
*   **Head of Supply Chain (Fulfillment & Logistics):** 
    *   *Solution:* Designed dedicated logistics dimensional models tracking supplier fulfillment rates and highlighting heavy-delivery bottlenecks using standardized routing metadata.
*   **Lead BI Architect (Performance & Governance):** 
    *   *Solution:* Enforced data cleanliness in the Silver layer (deduplication, schema enforcement, standard metadata generation) and optimized Delta Parquet tables (V-Order enabled) for blazing-fast DirectLake queries.
*   **Systems Architect (Operational Stability):** 
    *   *Solution:* Completely decoupled OLAP from OLTP. Data is ingested into the Bronze layer using efficient incremental loads, eliminating analytical query lockups on the source operational Azure SQL database.

## Architecture details

*   **Data Source:** Azure SQL Database (Wide World Importers).
*   **Bronze Layer (Raw):** Exact replica of WWI transactional tables stored natively in OneLake. Ingested via Fabric Pipelines/Copy Data with minimal source system impact.
*   **Silver Layer (Cleansed & Conformed):** Data processed using PySpark notebooks. Invalid records are filtered, duplicates dropped, and metadata standardized (e.g., audit columns, standardized date formats). 
*   **Gold Layer (Curated):** Dimensional modeling (Star Schema). Data is logically separated into subject areas (Sales Fact, Supply Chain Fact, Date/Geography/Product Dimensions) optimized for BI consumption.

## Tech Stack
*   **Microsoft Fabric:** Core data platform and orchestration.
*   **OneLake / Delta Lake:** Storage layer using open Delta Parquet format.
*   **PySpark / Spark SQL:** Data transformation and medallion tier processing.
*   **Data Factory (Fabric Pipelines):** Automated data movement and pipeline triggers.
*   **Power BI (DirectLake):** Zero-copy, high-performance analytical serving.

## Deployment Instructions
1.  **Configure Source Connection:** Set up a linked connection to the Azure SQL Database within the Fabric workspace.
2.  **Execute Ingestion (Bronze):** Trigger the ingestion pipeline to load raw WWI tables into the Bronze Lakehouse.
3.  **Run Silver Transformations:** Execute the Silver PySpark notebooks to apply deduplication, cleaning, and schema rules.
4.  **Build Gold Models:** Run the Gold notebooks to aggregate the data into the analytical star schemas.
5.  **Serve Data:** Sync the default Fabric semantic model using DirectLake mode to expose the Gold tables to downstream Power BI reports.
