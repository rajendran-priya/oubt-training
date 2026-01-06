# NYC Taxi Data Transformation Scripts

This directory contains advanced PySpark scripts for NYC taxi data transformations with comprehensive MDM (Master Data Management) capabilities.

## Scripts Overview

### 1. `aws_glue_corrected.py`
**Purpose**: AWS Glue ETL job for NYC taxi data processing with data quality validation
**Key Features**:
- Reads data from Glue Catalog (raw_data and master_data tables)
- Performs comprehensive data quality checks using AWS Glue Data Quality
- Routes passed/failed records to different S3 paths
- Includes referential integrity validation

**Fix Applied**: Changed `additional_data_sources` parameter to `secondary_datasets` in the EvaluateDataQuality call to resolve the "wrong number of arguments" error.

### 2. `advanced_nyc_taxi_transformer.py`
**Purpose**: Comprehensive PySpark pipeline for NYC taxi data transformation
**Key Features**:
- **Data Loading & Validation**: Loads taxi and zone data with schema validation
- **Data Quality Assessment**: Completeness, uniqueness, validity, and referential integrity checks
- **Data Cleaning**: Removes duplicates, invalid records, and applies business rules
- **Data Enrichment**: Joins with zone lookup for location names
- **Feature Engineering**: Creates 15+ derived features for analysis
- **Gold Layer Aggregations**: Business intelligence aggregations by borough, time, routes
- **Quality Reporting**: Generates comprehensive data quality reports

## Data Quality Checks Implemented

### Completeness Analysis
- Null value detection for all columns
- Completeness percentage calculation

### Uniqueness Analysis
- Distinct value counts
- Uniqueness ratio calculation

### Validity Checks
- Passenger count validation (1-6)
- Trip distance validation (> 0)
- Fare amount validation (> 0)
- Total amount validation (0-1000)

### Referential Integrity
- Pickup location ID validation against zone lookup
- Dropoff location ID validation against zone lookup

## Feature Engineering

### Time-Based Features
- Pickup hour, day of week, month, year
- Trip duration in minutes
- Time of day categorization (Morning/Afternoon/Evening/Night)
- Weekend indicator

### Performance Features
- Average speed (mph)
- Cost per mile
- Cost per minute
- Tip percentage

### Categorical Features
- Tip categories (No Tip/Low/Medium/High)
- Route types (Intra-borough/Inter-borough)

## Gold Layer Aggregations

1. **Borough Performance**: Trip counts, averages by pickup borough
2. **Hourly Patterns**: Trip volume and pricing by hour
3. **Day of Week Analysis**: Weekly patterns
4. **Route Analysis**: Origin-destination borough combinations
5. **Time of Day Analysis**: Performance by time segments

## Usage

### Running the Advanced Transformer

```bash
cd /Users/priyarajendran/Desktop/git/rajendran-priya/oubt-training/day7
python advanced_nyc_taxi_transformer.py
```

### AWS Glue Job
Deploy `aws_glue_corrected.py` to AWS Glue and run as an ETL job.

## Output Structure

```
processed_data/
├── silver_layer/           # Feature-enriched taxi data
├── gold_layer/
│   ├── borough_performance/
│   ├── hourly_patterns/
│   ├── day_of_week_analysis/
│   ├── route_analysis/
│   └── time_of_day_analysis/
└── quality_report.md       # Data quality assessment
```

## Data Sources

- **Taxi Data**: `yellow_tripdata_2025-08.parquet`
- **Zone Lookup**: `taxi_zone_lookup.csv`

## Key MDM Concepts Implemented

1. **Data Profiling**: Comprehensive analysis of data structure and quality
2. **Data Cleansing**: Removal of invalid and duplicate records
3. **Data Enrichment**: Adding contextual information via joins
4. **Master Data Integration**: Zone lookup as master data reference
5. **Data Quality Monitoring**: Automated quality checks and reporting
6. **Referential Integrity**: Ensuring data consistency across datasets

## Performance Optimizations

- Adaptive query execution enabled
- Partition coalescing for efficiency
- Memory optimization settings
- Parallel processing for aggregations

## Error Handling

- Comprehensive logging
- Schema validation
- Graceful failure handling
- Detailed error reporting</content>
<parameter name="filePath">/Users/priyarajendran/Desktop/git/rajendran-priya/oubt-training/day7/README.md