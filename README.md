# FMCG Consolidate Pipeline - M&A Data Integration

## Overview
This project implements a **post-acquisition data consolidation pipeline** for FMCG (Fast-Moving Consumer Goods) companies. When a **Parent Company acquires a Child Company**, the acquired company's messy, inconsistent data needs to be cleaned, transformed, and merged with the parent's existing analytics. This pipeline automates that process using Databricks, enabling stakeholders to view consolidated performance across all entities in a single unified dashboard.

## Business Context

**Scenario**: Parent Company acquires a new company (Child Company) with:
- ✗ Messy, inconsistent data formats
- ✗ Different schema structures
- ✗ Quality issues and missing values
- ✗ Disparate business rules

**Goal**: Transform and consolidate this data so that:
- ✓ Child Company data matches Parent Company format
- ✓ Data quality meets enterprise standards
- ✓ Stakeholders can analyze both companies together
- ✓ Single unified dashboard shows cross-company performance

## Architecture

![Architecture Diagram](docs/architecture.png)

### Data Flow Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          PARENT COMPANY                                  │
│                                                                          │
│          Unity Catalog                                                   │
│     ┌──────────────────────┐                                           │
│     │  Gold Analytics      │ ◄── Already exists (production data)      │
│     │  Table (Parent ORG)  │                                           │
│     └──────────▲───────────┘                                           │
│                │                                                         │
│                │ Merge with parent gold                                 │
│                │                                                         │
└────────────────┼─────────────────────────────────────────────────────────┘
                 │
                 │
┌────────────────┼─────────────────────────────────────────────────────────┐
│                │        CHILD COMPANY (ACQUIRED)                         │
│                │                                                          │
│    ┌───────────┴──────────┐      Unity Catalog                          │
│    │   Child Gold Data    │                                              │
│    │      Layer           │                                              │
│    └───────────▲──────────┘                                              │
│                │                                                          │
│                │ Transformation                                           │
│                │                                                          │
│    ┌───────────┴──────────┐                                              │
│    │   Silver Data        │  ← Cleaned, validated, standardized         │
│    │      Layer           │                                              │
│    └───────────▲──────────┘                                              │
│                │                                                          │
│                │ Transformation                                           │
│                │                                                          │
│    ┌───────────┴──────────┐                                              │
│    │   Bronze Data        │  ← Raw ingestion (as-is from S3)            │
│    │      Layer           │                                              │
│    └───────────▲──────────┘                                              │
│                │                                                          │
│    ┌───────────┴──────────┐                                              │
│    │   Lakeflow Jobs      │  ← Auto Loader ingestion                    │
│    └───────────▲──────────┘                                              │
│                │                                                          │
│    ┌───────────┴──────────┐                                              │
│    │   S3 Raw Data        │  ← Messy data from acquired company         │
│    │   + Archive          │                                              │
│    └──────────────────────┘                                              │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘

                              ▼
                    
┌───────────────────────────────────────────────────────────────────────────┐
│                         SERVING LAYER                                     │
│                                                                           │
│    ┌────────────────┐              ┌────────────────┐                   │
│    │  Dashboards    │              │     Genie      │                   │
│    │                │              │   (AI Query)   │                   │
│    │ Unified KPIs & │              │                │                   │
│    │ Visualizations │              │ Natural Lang.  │                   │
│    └────────────────┘              └────────────────┘                   │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

## Key Pipeline Stages

### Stage 1: Child Company Data Ingestion (S3 → Bronze)
**Input**: Messy raw data from acquired company stored in AWS S3
- Multiple file formats (CSV, JSON, Excel)
- Inconsistent schemas
- Data quality issues
- Missing mandatory fields

**Process**: Databricks Lakeflow Jobs with Auto Loader
- Automatically detects new files in S3
- Schema inference and evolution
- Incremental ingestion
- Metadata tracking

**Output**: Bronze layer tables (immutable raw data)

### Stage 2: Data Cleansing & Standardization (Bronze → Silver)
**Input**: Raw bronze tables

**Transformations**:
1. **Customer Data Processing**
   - Standardize customer names and addresses
   - Deduplicate customer records
   - Map to parent company customer IDs
   - Validate contact information

2. **Product Data Processing**
   - Normalize product codes
   - Map to parent product catalog
   - Standardize category and division names
   - Enrich with product attributes

3. **Pricing Data Processing**
   - Convert to parent company currency
   - Apply exchange rates
   - Standardize pricing units
   - Validate price ranges

**Output**: Silver layer dimension tables (clean, validated, enriched)

### Stage 3: Business-Ready Analytics (Silver → Gold)
**Input**: Clean silver dimension tables

**Process**:
- Join dimensions with fact data
- Calculate business metrics (revenue, quantities, order values)
- Apply business rules
- Create denormalized analytics tables

**Output**: Child Company Gold layer (matches parent format)

### Stage 4: Consolidation (Child Gold → Parent Gold)
**Input**: 
- Parent Company Gold Analytics Table (existing production data)
- Child Company Gold layer (newly processed)

**Merge Logic**:
```sql
MERGE INTO parent_analytics AS target
USING child_gold AS source
ON target.order_id = source.order_id 
   AND target.company_id = source.company_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Output**: Unified Parent ORG Analytics Table

### Stage 5: Serving Layer
**Dashboards**: Lakeview dashboards with:
- Consolidated KPIs across both companies
- Cross-company performance comparisons
- Revenue trends and forecasts
- Product and customer analytics

**Genie**: AI-powered natural language interface
- Ask questions in plain English
- Automatic query generation
- Interactive exploration

## Project Structure

```
FMCG-Consolidate-Pipeline/
├── 1_setup/
│   ├── setup_catalog.ipynb          # Unity Catalog and schema setup
│   └── dim_date_table_creation.ipynb # Date dimension initialization
│
├── 2_dimesion_data_processing/      # Bronze → Silver transformations
│   ├── utilities.ipynb                    # Shared utility functions
│   ├── 1_customer_data_processing.ipynb   # Clean customer data
│   ├── 2_products_data_processing.ipynb   # Standardize products
│   └── 3_pricing_data_processing.ipynb    # Normalize pricing
│
├── 3_fact_data_processing/          # Silver → Gold transformations
│   ├── 1_full_load_fact.ipynb        # Initial full load
│   └── 2_incremental_load_fact.ipynb # Ongoing incremental updates
│
├── dashboard/
│   └── dashboard_export.json          # Unified dashboard configuration
│
├── docs/
│   └── architecture.png               # Architecture diagram
│
├── README.md
├── .gitignore
└── GITHUB_SETUP.md
```

## Setup Instructions

### Prerequisites
- Databricks Workspace (Runtime 14.3 LTS or higher)
- Unity Catalog enabled
- AWS S3 access configured
- Permissions to create catalogs and schemas

### Step 1: Unity Catalog Setup

Run the catalog setup notebook to create the data structure:

```bash
1_setup/setup_catalog.ipynb
```

This creates:
- **Catalog**: `fmcg`
- **Schemas**: 
  - `fmcg.bronze` (raw ingestion layer)
  - `fmcg.silver` (cleaned dimension tables)
  - `fmcg.gold` (business analytics layer)

### Step 2: Configure Data Source

Set up S3 access for child company data:
```python
# Configure S3 bucket access
S3_BUCKET = "s3://acquired-company-data/"
CHECKPOINT_PATH = "s3://fmcg-checkpoints/"
ARCHIVE_PATH = "s3://acquired-company-archive/"
```

### Step 3: Configure Lakeflow Ingestion Job

Create a Databricks Lakeflow Job to ingest child company data:

1. Navigate to **Lakeflow Jobs** in Databricks
2. Click **Create Job**
3. Configure:
   - **Source**: `s3://acquired-company-data/`
   - **Target**: `fmcg.bronze` schema
   - **Format**: Auto-detect (CSV, JSON, Parquet)
   - **Mode**: Incremental with Auto Loader
   - **Schedule**: Hourly or on file arrival

4. Enable:
   - Schema inference
   - Schema evolution
   - Error handling and quarantine

### Step 4: Initialize Date Dimension

Create the date dimension table for time-based analysis:
```bash
1_setup/dim_date_table_creation.ipynb
```

### Step 5: Process Dimension Tables (Bronze → Silver)

Run dimension processing notebooks **in order**:

```bash
# 1. Customer dimension
2_dimesion_data_processing/1_customer_data_processing.ipynb

# 2. Products dimension
2_dimesion_data_processing/2_products_data_processing.ipynb

# 3. Pricing dimension
2_dimesion_data_processing/3_pricing_data_processing.ipynb
```

These notebooks:
- Read from Bronze layer
- Apply data quality rules
- Standardize formats to match parent schema
- Write to Silver layer

### Step 6: Build Gold Layer

**Initial Full Load** (run once):
```bash
3_fact_data_processing/1_full_load_fact.ipynb
```

This creates the child company gold analytics table by:
- Joining all silver dimensions
- Calculating business metrics
- Creating denormalized fact table

**Incremental Updates** (schedule daily):
```bash
3_fact_data_processing/2_incremental_load_fact.ipynb
```

### Step 7: Merge with Parent Gold Layer

Create a scheduled job to merge child company data with parent analytics:

```sql
MERGE INTO fmcg.gold.parent_analytics AS target
USING fmcg.gold.child_company_analytics AS source
ON target.order_id = source.order_id 
   AND target.company_id = source.company_id
WHEN MATCHED THEN 
  UPDATE SET 
    target.total_amount_inr = source.total_amount_inr,
    target.sold_quantity = source.sold_quantity,
    target.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN 
  INSERT (order_id, company_id, date, product_code, customer_code, 
          sold_quantity, total_amount_inr, created_at)
  VALUES (source.order_id, source.company_id, source.date, 
          source.product_code, source.customer_code, 
          source.sold_quantity, source.total_amount_inr, 
          CURRENT_TIMESTAMP())
```

**Schedule**: Run daily after incremental fact load completes

### Step 8: Deploy Unified Dashboard

1. Import the dashboard configuration:
   ```bash
   dashboard/dashboard_export.json
   ```

2. Connect to the unified analytics table:
   ```
   fmcg.gold.parent_analytics
   ```

3. Verify all visualizations render correctly

4. Publish the dashboard and share with stakeholders

## Data Schema

### Gold Layer: Unified Analytics Table

**Table**: `fmcg.gold.parent_analytics`

```sql
CREATE TABLE fmcg.gold.parent_analytics (
  -- Keys
  order_id STRING,
  company_id STRING,              -- 'PARENT' or 'CHILD_<ID>'
  company_type STRING,            -- 'parent' or 'child'
  
  -- Dimensions
  date DATE,
  year INT,
  quarter STRING,
  month_name STRING,
  
  customer_code STRING,
  customer STRING,
  market STRING,
  
  product_code STRING,
  product STRING,
  category STRING,
  division STRING,
  variant STRING,
  platform STRING,
  channel STRING,
  
  -- Metrics
  sold_quantity INT,
  price_inr DECIMAL(18,2),
  total_amount_inr DECIMAL(18,2),
  
  -- Audit fields
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  source_system STRING
)
PARTITIONED BY (date, company_id)
LOCATION 's3://fmcg-gold/parent_analytics/';
```

### Measures (Calculated)

```sql
-- Total Revenue
SUM(total_amount_inr) as total_revenue

-- Total Quantity
SUM(sold_quantity) as total_quantity

-- Order Count
COUNT(DISTINCT order_id) as order_count

-- Average Order Value
SUM(total_amount_inr) / COUNT(DISTINCT order_id) as avg_order_value
```

## Dashboard Features

The unified dashboard provides stakeholders with a **single pane of glass** view across parent and acquired companies:

### KPI Metrics (Top Row)
- **Total Revenue (INR)**: Consolidated revenue across all entities
- **Total Quantity Sold**: Units sold across all products
- **Total Orders**: Transaction count from both companies
- **Average Order Value**: Revenue per transaction

### Visualizations

1. **Revenue Trend Over Time** (Line Chart)
   - Daily/monthly revenue trends
   - Compare parent vs child company performance
   - Identify seasonality and growth patterns

2. **Top 10 Products by Revenue** (Horizontal Bar Chart)
   - Cross-company product ranking
   - Identify best sellers from both portfolios
   - Spot opportunities for cross-selling

3. **Revenue by Division** (Pie Chart)
   - Category mix analysis
   - Portfolio diversification view
   - Identify strategic product lines

4. **Revenue by Market** (Bar Chart)
   - Geographic performance comparison
   - Identify expansion opportunities
   - Track market penetration

5. **Revenue by Channel** (Bar Chart)
   - Sales channel effectiveness
   - Compare distribution strategies
   - Optimize channel mix

6. **Top 10 Customers by Revenue** (Horizontal Bar Chart)
   - Key account identification
   - Customer concentration risk
   - Cross-company customer opportunities

7. **Revenue by Month** (Bar Chart)
   - Monthly performance tracking
   - Year-over-year comparisons
   - Budget vs actual analysis

8. **Company Comparison** (Side-by-side metrics)
   - Parent vs Child performance
   - Integration progress tracking
   - Synergy realization metrics

### Genie AI Interface

Natural language queries enabled:
- "What was the total revenue from the acquired company last quarter?"
- "Show me top performing products from both companies"
- "Compare sales by market for parent and child entities"
- "Which customers buy from both companies?"
- "What's the revenue contribution of the acquired company?"

## Data Transformation Examples

### Customer Data Standardization

**Before (Child Company - Messy)**:
```
customer_id: "CUST_001"
name: "john doe"
address: "123 main st, city"
phone: "123-456-7890"
```

**After (Standardized - Silver Layer)**:
```
customer_code: "CHILD_CUST_001"
customer: "John Doe"
address_line1: "123 Main Street"
city: "City"
phone: "+1-123-456-7890"
customer_type: "child"
mapped_to_parent_customer: NULL
```

### Product Mapping

**Before (Child Company - Different Codes)**:
```
product_id: "SKU-ABC-123"
name: "Protein Bar - Choc"
price: "$2.50"
category: "Bars"
```

**After (Mapped to Parent Catalog)**:
```
product_code: "CHILD_SKU_ABC_123"
product: "SportsBar Protein Bar Chocolate"
price_inr: 208.75
category: "Protein Bars"
division: "Nutrition Bars"
variant: "65g"
parent_product_code: "778c2a7a..."
```

## Query Examples

### Consolidated Revenue by Company
```sql
SELECT 
  company_type,
  company_id,
  SUM(total_amount_inr) as total_revenue,
  COUNT(DISTINCT order_id) as order_count,
  SUM(total_amount_inr) / COUNT(DISTINCT order_id) as avg_order_value
FROM fmcg.gold.parent_analytics
GROUP BY company_type, company_id
ORDER BY total_revenue DESC;
```

### Cross-Company Product Performance
```sql
SELECT 
  product,
  SUM(CASE WHEN company_type = 'parent' THEN sold_quantity ELSE 0 END) as parent_qty,
  SUM(CASE WHEN company_type = 'child' THEN sold_quantity ELSE 0 END) as child_qty,
  SUM(total_amount_inr) as total_revenue
FROM fmcg.gold.parent_analytics
GROUP BY product
ORDER BY total_revenue DESC
LIMIT 20;
```

### Integration Progress Tracking
```sql
SELECT 
  DATE_TRUNC('month', date) as month,
  company_type,
  SUM(total_amount_inr) as revenue,
  COUNT(DISTINCT customer_code) as unique_customers
FROM fmcg.gold.parent_analytics
WHERE date >= CURRENT_DATE - INTERVAL 6 MONTHS
GROUP BY DATE_TRUNC('month', date), company_type
ORDER BY month, company_type;
```

## Best Practices

### Data Quality Assurance
1. **Validation at Every Layer**
   - Bronze: Schema validation, file completeness
   - Silver: Business rule validation, referential integrity
   - Gold: Metric calculations, outlier detection

2. **Quarantine Process**
   - Invalid records moved to quarantine tables
   - Data quality dashboard for monitoring
   - Manual review workflow for exceptions

3. **Audit Logging**
   - Track all transformations
   - Record merge statistics
   - Monitor data lineage

### Performance Optimization
1. **Partitioning Strategy**
   ```sql
   PARTITIONED BY (date, company_id)
   ```
   - Faster queries on recent data
   - Efficient company-specific filters

2. **Z-Ordering**
   ```sql
   OPTIMIZE fmcg.gold.parent_analytics
   ZORDER BY (customer_code, product_code);
   ```

3. **Auto-Optimize**
   ```sql
   ALTER TABLE fmcg.gold.parent_analytics 
   SET TBLPROPERTIES (
     'delta.autoOptimize.optimizeWrite' = 'true',
     'delta.autoOptimize.autoCompact' = 'true'
   );
   ```

### Security & Governance

**Row-Level Security by Company**:
```sql
CREATE FUNCTION company_filter(company_id STRING)
RETURN 
  CASE 
    WHEN IS_ACCOUNT_GROUP_MEMBER('parent_admin_group') THEN TRUE
    WHEN IS_ACCOUNT_GROUP_MEMBER('child_company_group') 
         AND company_id LIKE 'CHILD_%' THEN TRUE
    ELSE FALSE
  END;

ALTER TABLE fmcg.gold.parent_analytics 
SET ROW FILTER company_filter ON (company_id);
```

**Column-Level Encryption**:
```sql
-- Mask sensitive customer information
CREATE FUNCTION mask_customer_data(customer STRING)
RETURN 
  CASE 
    WHEN IS_ACCOUNT_GROUP_MEMBER('pii_access_group') THEN customer
    ELSE CONCAT(LEFT(customer, 3), '***')
  END;
```

## Troubleshooting

### Issue: Lakeflow Job Fails to Ingest S3 Data
**Symptoms**: Files remain in S3, bronze tables are empty

**Solutions**:
1. Verify S3 bucket permissions and IAM roles
2. Check Databricks instance profile has S3 read access
3. Validate file formats match Auto Loader configuration
4. Review checkpoint location accessibility

### Issue: Schema Mismatch During Merge
**Symptoms**: `MERGE INTO` statement fails with column errors

**Solutions**:
1. Compare schemas:
   ```sql
   DESCRIBE TABLE fmcg.gold.parent_analytics;
   DESCRIBE TABLE fmcg.gold.child_company_analytics;
   ```
2. Run schema alignment script in silver layer
3. Enable schema evolution if appropriate
4. Update merge statement to handle new columns

### Issue: Duplicate Records After Merge
**Symptoms**: Same order appears multiple times

**Solutions**:
1. Verify merge key uniqueness:
   ```sql
   SELECT order_id, company_id, COUNT(*)
   FROM fmcg.gold.child_company_analytics
   GROUP BY order_id, company_id
   HAVING COUNT(*) > 1;
   ```
2. Add deduplication step before merge
3. Review data generation logic in source

### Issue: Dashboard Shows Incomplete Data
**Symptoms**: Missing data for child company

**Solutions**:
1. Check table refresh timestamps:
   ```sql
   DESCRIBE HISTORY fmcg.gold.parent_analytics;
   ```
2. Verify merge job completed successfully
3. Confirm Unity Catalog permissions for dashboard users
4. Refresh dataset connection in dashboard

## Technologies Used

- **Databricks Lakehouse Platform**: Unified data and AI platform
- **Lakeflow Jobs**: Managed ingestion pipelines with Auto Loader
- **Delta Lake**: ACID transactions, schema enforcement, time travel
- **Unity Catalog**: Data governance, lineage tracking, access control
- **Apache Spark**: Distributed data processing engine
- **Lakeview Dashboards**: Interactive business intelligence
- **Genie**: Natural language to SQL interface
- **AWS S3**: Scalable object storage for raw data

## Success Metrics

Track these KPIs to measure integration success:

1. **Data Quality Score**: % of records passing validation
   - Target: >99% after 30 days

2. **Merge Success Rate**: % of child records successfully merged
   - Target: >99.5%

3. **Data Freshness**: Time from S3 upload to dashboard availability
   - Target: <2 hours

4. **Dashboard Adoption**: % of stakeholders actively using unified view
   - Target: 100% of executives within 60 days

5. **Query Performance**: Dashboard load time
   - Target: <3 seconds

## Roadmap

### Phase 1: Foundation (Complete)
- ✅ Bronze/Silver/Gold pipeline
- ✅ Initial merge logic
- ✅ Unified dashboard

### Phase 2: Enhancement (In Progress)
- [ ] Real-time streaming ingestion
- [ ] Advanced data quality monitoring
- [ ] Automated anomaly detection

### Phase 3: Advanced Analytics (Planned)
- [ ] ML-powered demand forecasting
- [ ] Customer lifetime value modeling
- [ ] Predictive inventory optimization
- [ ] Synergy opportunity identification

### Phase 4: Scale (Future)
- [ ] Support for multiple acquisitions
- [ ] Multi-cloud deployment (Azure, GCP)
- [ ] Advanced attribution modeling

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/data-quality-checks`)
3. Test thoroughly in development environment
4. Commit with descriptive messages (`git commit -am 'Add customer deduplication logic'`)
5. Push to branch (`git push origin feature/data-quality-checks`)
6. Create Pull Request with detailed description

## License

This project is licensed under the MIT License.

## Support

For questions, issues, or feature requests:
- **GitHub Issues**: [Open an issue](https://github.com/your-org/FMCG-Consolidate-Pipeline/issues)
- **Email**: data-engineering@company.com
- **Slack**: #data-platform channel

---

**Project Status**: ✅ Production Ready  
**Last Updated**: January 2025  
**Version**: 2.0.0  
**Maintained By**: Data Engineering Team  
**Business Owner**: Chief Data Officer
