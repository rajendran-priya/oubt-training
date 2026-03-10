•	Day 11 : Data Pipeline Orchestration & Monitoring
o	AWS Step Functions, Event Bridge, workflow orchestration, CloudWatch, logging, cost optimization
o	Review AWS Well-Architected Framework for data
o	cost optimization techniques and logging strategies
o	Build a complete workflow pipeline
o	Audit trail: Every pipeline run logged with:
•	Who triggered it? (User, schedule, event)
•	What data was processed? (Lineage)
•	What quality checks passed/failed?
•	What master data was used? (Version snapshot)
o	Hands-On Lab
•	Create Step Functions state machines to Orchestrate multiple AWS services ( Glue, lambda etc.)
•	Implement error handling and retries & Set up CloudWatch alarms and dashboards
•	Add governance checkpoints: 
•	Quality validation step (fail pipeline if below threshold)
•	Master data freshness check (alert if reference data stale)
•	Approval notification (SNS to steward for manual review)
•	Audit log publication (to dedicated audit table)

