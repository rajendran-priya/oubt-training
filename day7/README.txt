•	Day 7: Introduction to Apache Spark and AWS Glue 
o	Spark is, its lazy evaluation architecture, and its main components
o	AWS Glue Data Catalog - Central metadata repository for all your data and integrations
o	Glue Job types: Spark (standard), Python Shell, Streaming ETL with DPUs (Data Processing Units) ,Built-in transformations and data quality rules
o	Glue Crawlers  Configure data sources (S3, RDS, etc.), Schedule crawlers to run automatically, Handle schema evolution and partitioning
o	Glue Data Catalog as governance tool:
•	Not just technical metadata (schema, partitions)
•	Business metadata (definitions, ownership)
•	Operational metadata (quality scores, lineage)
o	Data discovery for governance: How do users find trustworthy data?

o	Hands-on:
•	Write PySpark scripts for NYC taxi transformations
•	Implement master data lookups/enrichment reference data validation
•	Optimize Spark jobs
•	Enhance Glue Catalog with governance metadata
•	Add custom properties: data_owner, domain, classification
•	Create crawler with metadata extraction rules 
•	Document data lineage in catalog

