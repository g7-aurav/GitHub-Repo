Project 1: Fintech Data Migration from MSSQL Database to ADLS (Industrial Project)

Tech Stack: Python, Azure SQL Database, SQL, Azure Synapse Analytics, ADLS, PySpark, Delta Tables, Azure Logic Apps

→ Source System Setup: Historical financial data structured and validated in Azure SQL Database across multiple tables, ensuring referential integrity for downstream migration.

→ Dynamic Ingestion (Bronze Layer): Azure Synapse Pipeline orchestrated to extract and load data from SQL Database into ADLS Bronze Layer using parameterized and dynamic linked services for scalable ingestion.

→ Transformation (Silver Layer): PySpark Notebook processes raw Bronze data — cleansing, standardizing, and enriching — before writing to Silver Layer Delta Tables for optimized query performance.

→ Aggregation (Gold Layer): Additional PySpark Notebook performs business transformations and aggregations, generating Gold Layer Delta Tables ready for analytics and reporting.

→ Operational Workflow: Synapse Web Activity triggers Azure Logic App via HTTP POST call after each pipeline run to report execution status.

→ Automated Notifications: Logic App parses the pipeline’s response payload and automatically sends success/failure email alerts to designated FinTech operations teams.

→ End-to-End Orchestration: Fully automated, sequential execution pipeline with built-in error handling, ensuring reliable data migration, transformation, and monitoring.

→ Outcome: Seamless modernization of FinTech data infrastructure from MSSQL to a Delta Lake architecture on ADLS, enabling faster analytics, reliable lineage, and automated operational visibility.