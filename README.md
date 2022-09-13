### ETL & Data Warehousing
#### TechStack: ETL (Python, SQL), Data Warehosue (Postgresql)

Hi! I'm going to implement a data warehouse project using open source tech stacks. I'll be using python and sql for the **ETL (Extreact, Transform, Load)** and **Postgresql** as the data warehouse. 

###  Project Definition
A single transaction file will be shared every month that will contain sales data. Data has **customer information, product, category, country, region, shipment, sales, quantity, revenue values and date**.  At the beginning a full data dump of all available data is given and after that every month an excel file will be provided to you to ingest data. Reporting team will use the data warehosue to report the following:

- Customer ranking by revenue, quantity & Sales
- Region & City ranking by revenue, quantity & Sales
- Salesperson ranking by revenue, quantity & Sales
- Product ranking by revenue, quantity & Sales
- Revenue, Quantity & Sales trend analysis by Year, Quarter & Month

**So the task is to:**
- Design and Implement a Data storage or Warehouse based on the reporting requirements above
- Design an ETL for the data ingestion process. The source file will be provided monthly and need to be loaded into the data warehouse, so design the ETL in that way
- Design the ETL for the entire process of data warehousing
