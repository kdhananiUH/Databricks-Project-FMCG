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
├── docs/
│   └── architecture.png               # Architecture diagram
│
├── README.md
├── .gitignore
└── GITHUB_SETUP.md
```

## Technologies Used

- **Databricks Lakehouse Platform**: Unified data and AI platform
- **Lakeflow Jobs**: Managed ingestion pipelines with Auto Loader
- **Delta Lake**: ACID transactions, schema enforcement, time travel
- **Unity Catalog**: Data governance, lineage tracking, access control
- **Apache Spark**: Distributed data processing engine
- **Lakeview Dashboards**: Interactive business intelligence
- **Genie**: Natural language to SQL interface
- **AWS S3**: Scalable object storage for raw data

**Maintained By**: Data Engineering Team  
**Business Owner**: Chief Data Officer
