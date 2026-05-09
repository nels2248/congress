# Congress Data Dashboard

## Overview

The Congress Data Dashboard is a data engineering and analytics project focused on processing, analyzing, and visualizing U.S. congressional bill activity.

The system extracts raw legislative XML data, transforms it into structured analytical datasets, stores the results in DuckDB, and generates interactive dashboards using D3.js.

In addition to traditional reporting and visualization, the project also includes machine learning components for legislative next-step prediction and congressional bill clustering analysis.

---

# Project Goals

This project was designed to demonstrate:

- End-to-end ETL pipeline development
- XML data parsing and transformation
- Analytical database design using DuckDB
- JSON-based dashboard architecture
- Interactive frontend visualization with D3.js
- Machine learning integration for legislative analytics
- Feature engineering and predictive modeling
- Clustering analysis for identifying legislative behavior patterns

---

# Current Project Status

The project currently operates as a manually refreshed analytical pipeline.

Originally, the system was planned to support nightly automated processing and incremental congressional data updates. During development, a more complete and higher-quality legislative dataset was discovered late in the project timeline.

Because of this improved data source, the nightly automation process is temporarily paused while the pipeline architecture is being redesigned around the new dataset.

The current implementation still demonstrates the full analytical workflow and system architecture.

Planned future enhancements include:

- Automated nightly ETL processing
- Incremental update tracking
- Expanded congressional datasets
- Enhanced machine learning models
- Additional dashboard visualizations and analytics
- Trend monitoring over time

---

# System Architecture

## 1. Data Extraction

The pipeline reads raw XML congressional bill files containing:

- Bill metadata
- Sponsor information
- Legislative actions
- Co-sponsor data
- Status updates

---

## 2. Data Transformation

The ETL process cleans and structures the raw XML data into analytical datasets.

### Generated Tables

### Bills
Contains:
- Bill number
- Title
- Bill type
- Sponsor
- Chamber
- Introduced date

### Actions
Contains:
- Legislative timeline events
- Action descriptions
- Action dates

### CoSponsors
Contains:
- Co-sponsor names
- Supporting legislator data

---

## 3. Data Loading

The transformed datasets are loaded into a local analytical database using DuckDB.

The system also exports processed data into JSON format for frontend visualization.

### Generated Output

```text
congress_dashboard.json
```

---

# Dashboard Features

The frontend dashboard is built using HTML, CSS, JavaScript, and D3.js.

## Included Features

### Bill Explorer
- Searchable bill table
- Sorting by columns
- Pagination
- Sponsor filtering

### Detail View
Displays:
- Full bill details
- Co-sponsor information
- Legislative timeline
- Bill activity metrics

### Metrics Dashboard
Displays:
- Total bills processed
- Total actions
- Total co-sponsors
- Average activity levels

---

# Machine Learning Components

## 1. Legislative Next-Step Prediction

This machine learning component attempts to predict the likely next legislative action or progression step for congressional bills.

### Features Used
- Bill type
- Chamber
- Sponsor data
- Action history
- Co-sponsor counts
- Current status

### Models Tested
- Random Forest
- Extra Trees
- Gradient Boosting
- Logistic Regression

The dashboard compares prediction confidence and classification performance between models.

---

## 2. Congressional Bill Clustering

The project also includes unsupervised machine learning analysis using clustering techniques.

The clustering system groups bills based on shared characteristics and legislative behavior patterns.

### Clustering Inputs
- Action counts
- Co-sponsor participation
- Activity levels
- Legislative progression patterns
- Structural bill similarities

### Clustering Goals
Identify:
- Highly active legislation
- Low-engagement bills
- Similar bill behavior patterns
- Legislative trend groupings

---

# Technologies Used

## Backend / Data Engineering
- Python
- DuckDB
- Pandas
- XML Parsing
- JSON Processing

## Machine Learning
- Scikit-learn
- Classification Models
- Clustering Algorithms

## Frontend / Visualization
- HTML
- CSS
- JavaScript
- D3.js

---

# Project Structure

```text
/project
│
├── data/
│   ├── raw_xml/
│   ├── processed/
│
├── db/
│   └── congress.duckdb
│
├── output/
│   └── congress_dashboard.json
│
├── dashboards/
│   ├── index.html
│   ├── details.html
│   ├── cluster_groups.html
│   └── ml_dashboard.html
│
├── scripts/
│   ├── etl_pipeline.py
│   ├── clustering.py
│   └── prediction_models.py
│
└── README.md
```

---

# Future Improvements

Planned future enhancements include:

- Real-time data ingestion
- Automated nightly processing
- Expanded legislative datasets
- Improved feature engineering
- Additional machine learning models
- Geographic visualizations
- Trend forecasting dashboards
- Cloud deployment options

---

# Educational Value

This project demonstrates practical applications of:

- Data engineering
- ETL pipeline development
- Database management
- Data visualization
- Predictive analytics
- Machine learning workflows
- Frontend analytical dashboard development

---

# Author

Chad Alan Nelson

Master's of Data Science Program  
Business Intelligence / Data Engineering / Analytics