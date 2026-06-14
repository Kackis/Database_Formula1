# Formula 1 Data Engineering Project

### 1. Project Overview
Azure Databricks data engineering and analytics project using a Medallion Architecture (Bronze, Silver, and Gold layers). Build using Apache Spark, Delta Lake, Azure Storage, Databricks Workflows, analytical dashboards, pipeline for data processing and business reporting.

### 2. Data Source

The dataset used in this project was provided as part of the Udemy course "Azure Databricks & Spark Core For Data Engineers" by Ramesh Retnasamy.
This repository contains only the implementation, data engineering pipelines, dashboards, and project documentation.

### 3. Business Objective

Data is distributed across multiple source files containing information about races, drivers, constructors, circuits, results, and sprint events. The objective of this project is to integrate these datasets into a single, consistent analytical model that enables analysis.

The data is cleansed, standardized, and enriched to create a reliable foundation for reporting. Race results are processed to assign championship points based on finishing positions, allowing the calculation of season standings for both drivers and constructors.

The final analytical layer enables users to:
* Analyze driver championship standings by season.
* Analyze constructor championship standings by season.
* Compare performance across different years and teams.
* Track historical Formula 1 trends and achievements.
* Generate dashboards and reports for business analysis.

For example, users can view the top-performing drivers in the 1953 season or analyze constructor rankings based on the cumulative points earned by their teams.

### 4. Solution Architecture

```
Source Data
   ↓
Azure Data Lake
   ↓
Databricks
   ↓
Bronze
   ↓
Silver
   ↓
Gold
   ↓
Dashboard
```

### 5. Data pipeline 
![Pipeline Workflow](docs/pipeline_workflow.jpg)

### 6. Lakehouse Data Organization
![Lakehouse Organization](docs/catalog.jpg)

The project implements a Medallion Architecture using Unity Catalog in Azure Databricks.

- Landing layer stores source files.
- Bronze layer contains raw ingested data.
- Silver layer contains cleansed and transformed datasets.
- Gold layer contains dimensional models used for reporting and dashboarding.
- Analytical layer contains SQL code, dashboards, and diagrams.

### 7. Dashboard Examples
![Constructors 1983](docs/constructors/constructor_championship_1983.jpg) 
Constructor Championship Standings - 1983 Season

![Drivers 1995](docs/drivers/driver_championship_1995.jpg) 
Driver Championship Standings - 1995 Season

More dashboards are available in the "docs" folder.

### 8. Technology

Cloud Platform:
- Microsoft Azure

Data Processing:
- Azure Databricks
- Apache Spark
- PySpark
- SQL
- Python

Storage:
- Azure Data Lake Storage Gen2

Orchestration:
- Databricks Workflows

Analytics:
- Databricks Dashboards

### 9. Project Setup

To run this project, the following components are required:

- Azure Databricks Workspace
- Azure Data Lake Storage Gen2
- Apache Spark Runtime
- Python and SQL support enabled in Databricks

The notebooks and workflows included in this repository can be imported into Azure Databricks and adapted to another Formula 1 dataset with a similar structure.
