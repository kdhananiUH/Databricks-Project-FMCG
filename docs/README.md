# Architecture Documentation

## Adding the Architecture Diagram

1. Save your architecture diagram as 'architecture.png'
2. Place it in this /docs/ folder
3. The README.md already references it as:
   ![Architecture Diagram](docs/architecture.png)

## Current Architecture Overview

**Data Flow:**
```
S3/Archive → Lakeflow Jobs → Bronze Layer → Silver Layer → Gold Layer
                                                              ↓
                              Parent Company ← Merge ← Child Company
                                    ↓
                            Serving Layer (Dashboards + Genie)
```

**Key Components:**
- **Ingestion**: Lakeflow Jobs with Auto Loader
- **Storage**: Delta Lake tables in Unity Catalog
- **Governance**: Unity Catalog with row-level security
- **Consolidation**: Parent-Child merge operations
- **Analytics**: Lakeview Dashboards + Genie

## Additional Documentation

Add these files to this folder as your project grows:
- architecture.png (main architecture diagram)
- data_model.png (ERD of dimensional model)
- pipeline_flow.png (detailed ETL flow)
- security_model.png (access control diagram)
- deployment_guide.md (step-by-step deployment)
- troubleshooting.md (common issues and solutions)
