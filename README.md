# NYC Green Taxi - End-to-End Azure Data Pipeline & Analysis
<img width="631" height="582" alt="image" src="https://github.com/user-attachments/assets/53ca4538-c166-4241-815e-d2e2b027956a" />

## Project Overview

This project presents an **end-to-end cloud data pipeline** designed to ingest, process, and analyze the 2024 NYC Green Taxi dataset using Microsoft Azure. The pipeline transforms raw taxi trip data into actionable insights, visualized through an interactive Power BI dashboard, enabling data-driven understanding of taxi operations.

---

## Objectives

The main objectives of this data engineering project were to:

* **Automate Data Ingestion:** Build a dynamic pipeline to fetch raw data directly from web APIs.
* **Implement Cloud Transformation:** Clean, transform, and enrich large datasets using cloud-based tools (Databricks, PySpark).
* **Model Data for Analytics:** Structure the final data using the Medallion Architecture and Delta Lake for reliable querying.
* **Visualize Key Metrics:** Develop a Power BI dashboard showcasing operational performance, demand patterns, and key insights.

---

## Problem Statement

Analyzing NYC taxi data often involves handling large, raw datasets with inconsistencies. Key challenges included:

1.  **Manual Ingestion:** Need for an automated process to handle monthly data releases.
2.  **Data Quality:** Raw data contains errors, null values, and requires significant cleaning and transformation.
3.  **Scalability:** Processing large volumes of trip data efficiently requires scalable cloud tools.
4.  **Actionable Insights:** Raw data needs to be modeled and visualized effectively to understand trends (peak hours, popular locations).

This pipeline addresses these issues by providing an automated workflow from ingestion to visualization.

---

## Dataset Information

The analysis used the publicly available NYC Green Taxi dataset for 2024, combined with standard lookup tables.

| Table Name        | Description                                       | Key Fields Used                                      | Source        |
| :---------------- | :------------------------------------------------ | :--------------------------------------------------- | :------------ |
| `trip_main_table` | Core transaction records for each taxi trip.      | Pickup/Dropoff Datetime, Fare, Distance, LocationIDs | NYC TLC Data  |
| `trip_zone`       | Lookup table mapping LocationIDs to Borough/Zone. | LocationID, Borough, Zone                            | NYC TLC Data  |
| `trip_type`       | Lookup table describing payment/trip types.       | trip\_type, trip\_description                        | NYC TLC Data  |
| `Date Table`      | Dimension table created in Power BI using DAX.    | Date, Year, Month, Week, Day                         | Generated     |

---

## Technology Stack

| Component                  | Tool / Language                                    |
| :------------------------- | :------------------------------------------------- |
| Cloud Platform             | Microsoft Azure                                    |
| Data Ingestion             | Azure Data Factory (ADF)                           |
| Data Storage               | Azure Data Lake Storage (ADLS) Gen2                |
| Data Cleaning & Processing | Azure Databricks, PySpark                          |
| Data Modeling / Warehouse  | Delta Lake, Databricks SQL (Managed Tables)        |
| Visualization / Dashboard  | Power BI (Import Mode), DAX                        |
| Documentation              | Markdown (README)                                  |

---

## Data Model Overview

<img width="962" height="550" alt="image" src="https://github.com/user-attachments/assets/8b3d6428-b977-4166-b04f-647c65b339a4" />


*A star schema was implemented in Power BI. The central fact table (`trip_main_table`) contains transactional trip data. It is linked to dimension tables (`trip_zone`, `trip_type`, `Date Table`) via established relationships, enabling multi-dimensional analysis.*

---

## Key Insights (from Dashboard)

<img width="1109" height="595" alt="image" src="https://github.com/user-attachments/assets/901ad0ac-4e0d-4c50-bb0a-8635ab9564eb" />

Analysis of **593K trips** in the final dashboard revealed:

* **Busiest Days:** Friday and Saturday show the highest trip volume.
* **Peak Hours:** Demand significantly increases during the evening commute (around 5 PM).
* **Top Boroughs:** Manhattan and Queens lead in trip counts.
* **Popular Zone:** East Harlem North is the most frequent pickup location.
* **Average Trip:** Average fare is **$17.88**; average distance is **2.95 miles**.

---

## Recommendations

Based on the analysis, potential strategies include:

1.  **Optimize Driver Allocation:** Increase driver presence in East Harlem and other top zones during evening peaks and weekends.
2.  **Off-Peak Incentives:** Consider promotions during slower periods (e.g., Monday mornings) to boost utilization.
3.  **Route Analysis:** Further investigate popular routes originating from top zones for potential service enhancements.

---
