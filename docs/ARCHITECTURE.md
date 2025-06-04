# System Architecture

## Overview
This document describes the technical architecture of the Smart Water Management Platform.

## Components

### Data Ingestion Layer
- **Unified Ingestion Platform**: Orchestrates multiple API sources
- **Circuit Breakers**: Prevents cascade failures
- **Data Validation**: Ensures quality at ingestion

### Processing Layer
- **PySpark**: Distributed processing for scalability
- **ML Pipeline**: Clustering and anomaly detection

### Storage Layer
- **PostgreSQL**: Dimensional data warehouse
- **Redis**: Caching layer (future use)

### Presentation Layer
- **Flask API**: RESTful endpoints
- **Chart.js**: Interactive visualizations

## Data Flow
1. External APIs → Ingestion Platform
2. Raw Data → Validation Framework
3. Validated Data → Spark Processing
4. Processed Data → PostgreSQL
5. PostgreSQL → Flask API → Dashboard