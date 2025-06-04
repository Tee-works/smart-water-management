-- Updated Water Management Schema for Real API Data
-- Supports real data from OpenMeteo, UK Environment Agency, OpenWeather

-- =============================================================================
-- DROP EXISTING TABLES (for clean restart)
-- =============================================================================
DROP TABLE IF EXISTS fact_sensor_readings CASCADE;
DROP TABLE IF EXISTS dim_sensors CASCADE;
DROP TABLE IF EXISTS dim_sensor_types CASCADE;
DROP TABLE IF EXISTS dim_locations CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;
DROP TABLE IF EXISTS dim_data_sources CASCADE;

-- =============================================================================
-- DIMENSION TABLES (Updated for Real Data)
-- =============================================================================

-- Time dimension (same as before)
CREATE TABLE dim_time (
    time_key INTEGER PRIMARY KEY,
    date_actual DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_business_day BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    season VARCHAR(10) NOT NULL,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
);

-- Data sources dimension (NEW - for lineage tracking)
CREATE TABLE dim_data_sources (
    data_source_key INTEGER PRIMARY KEY,
    data_source_name VARCHAR(100) NOT NULL UNIQUE,
    source_type VARCHAR(50) NOT NULL, -- 'API', 'GOVERNMENT', 'COMMERCIAL', 'MOCK'
    provider VARCHAR(100),
    api_endpoint VARCHAR(500),
    refresh_frequency_minutes INTEGER,
    requires_api_key BOOLEAN DEFAULT FALSE,
    reliability_score DECIMAL(3,2) DEFAULT 1.0,
    description TEXT,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Location hierarchy (updated for real London districts)
CREATE TABLE dim_locations (
    location_key INTEGER PRIMARY KEY,
    district_id VARCHAR(20) NOT NULL,
    district_name VARCHAR(100) NOT NULL,
    zone_id VARCHAR(20),
    zone_name VARCHAR(100),
    region_name VARCHAR(100) DEFAULT 'Greater London',
    city VARCHAR(100) DEFAULT 'London',
    country VARCHAR(100) DEFAULT 'United Kingdom',
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    population INTEGER,
    area_sq_km DECIMAL(10,2),
    infrastructure_type VARCHAR(50),
    priority_level VARCHAR(20),
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sensor types (updated for real sensor types)
CREATE TABLE dim_sensor_types (
    sensor_type_key INTEGER PRIMARY KEY,
    sensor_type VARCHAR(50) NOT NULL UNIQUE,
    category VARCHAR(50) NOT NULL,
    measurement_unit VARCHAR(20) NOT NULL,
    normal_min_value DECIMAL(10,3),
    normal_max_value DECIMAL(10,3),
    warning_min_value DECIMAL(10,3),
    warning_max_value DECIMAL(10,3),
    critical_min_value DECIMAL(10,3),
    critical_max_value DECIMAL(10,3),
    precision_decimal_places INTEGER DEFAULT 2,
    calibration_frequency_days INTEGER,
    description TEXT,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sensors dimension (updated for real sensor IDs)
CREATE TABLE dim_sensors (
    sensor_key BIGINT PRIMARY KEY,
    sensor_id VARCHAR(50) NOT NULL, -- Longer for real IDs like "UK_EA_3400TH"
    sensor_type VARCHAR(50) NOT NULL,
    district VARCHAR(100) NOT NULL,
    zone VARCHAR(100),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    installation_date DATE,
    manufacturer VARCHAR(100),
    model VARCHAR(100),
    firmware_version VARCHAR(50),
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    maintenance_schedule VARCHAR(50),
    -- SCD Type 2 fields
    effective_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- FACT TABLES (Updated for Real Data)
-- =============================================================================

-- Main fact table (enhanced for real API data)
CREATE TABLE fact_sensor_readings (
    reading_key BIGINT PRIMARY KEY,
    sensor_key BIGINT NOT NULL,
    time_key INTEGER NOT NULL,
    location_key INTEGER NOT NULL,
    sensor_type_key INTEGER NOT NULL,
    data_source_key INTEGER NOT NULL, -- NEW: Track data lineage
    
    -- Measures
    reading_value DECIMAL(15,6) NOT NULL,
    quality_score DECIMAL(5,3),
    anomaly_flag INTEGER DEFAULT 0,
    anomaly_score DECIMAL(8,4),
    z_score DECIMAL(8,4),
    
    -- Technical fields (enhanced)
    reading_timestamp TIMESTAMP NOT NULL,
    ingestion_timestamp TIMESTAMP,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(100),              -- Source-specific batch ID
    unified_batch_id VARCHAR(100),      -- Cross-source batch ID
    pipeline_version VARCHAR(20),       -- Pipeline version
    ingestion_source VARCHAR(100),      -- Ingestion source name
    ingestion_priority VARCHAR(20),     -- Processing priority
    data_source VARCHAR(100),           -- Original data source name
    
    -- Thames-specific fields (nullable for non-Thames data)
    station_name VARCHAR(200),          -- Thames station name
    catchment VARCHAR(200),             -- Thames catchment area
    
    -- Foreign key constraints
    FOREIGN KEY (sensor_key) REFERENCES dim_sensors(sensor_key),
    FOREIGN KEY (time_key) REFERENCES dim_time(time_key),
    FOREIGN KEY (location_key) REFERENCES dim_locations(location_key),
    FOREIGN KEY (sensor_type_key) REFERENCES dim_sensor_types(sensor_type_key),
    FOREIGN KEY (data_source_key) REFERENCES dim_data_sources(data_source_key)
);

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================

-- Fact table indexes
CREATE INDEX idx_fact_readings_timestamp ON fact_sensor_readings(reading_timestamp);
CREATE INDEX idx_fact_readings_sensor_time ON fact_sensor_readings(sensor_key, time_key);
CREATE INDEX idx_fact_readings_location_time ON fact_sensor_readings(location_key, time_key);
CREATE INDEX idx_fact_readings_batch ON fact_sensor_readings(unified_batch_id);
CREATE INDEX idx_fact_readings_data_source ON fact_sensor_readings(data_source_key);
CREATE INDEX idx_fact_readings_ingestion ON fact_sensor_readings(ingestion_timestamp);

-- Dimension table indexes
CREATE INDEX idx_dim_sensors_id ON dim_sensors(sensor_id);
CREATE INDEX idx_dim_sensors_current ON dim_sensors(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_sensors_type ON dim_sensors(sensor_type);
CREATE INDEX idx_dim_time_date ON dim_time(date_actual);
CREATE INDEX idx_dim_time_year_month ON dim_time(year, month);
CREATE INDEX idx_dim_locations_district ON dim_locations(district_id);
CREATE INDEX idx_data_sources_name ON dim_data_sources(data_source_name);

-- =============================================================================
-- ANALYTICAL VIEWS (Updated)
-- =============================================================================

-- Real-time sensor status view
CREATE VIEW v_sensor_status AS
SELECT 
    s.sensor_id,
    s.sensor_type,
    l.district_name,
    s.status,
    st.measurement_unit,
    st.normal_min_value,
    st.normal_max_value,
    s.effective_date,
    s.is_current,
    ds.data_source_name,
    ds.provider
FROM dim_sensors s
JOIN dim_locations l ON s.district = l.district_name
JOIN dim_sensor_types st ON s.sensor_type = st.sensor_type
LEFT JOIN fact_sensor_readings f ON s.sensor_key = f.sensor_key
LEFT JOIN dim_data_sources ds ON f.data_source_key = ds.data_source_key
WHERE s.is_current = TRUE;

-- Enhanced district performance view
CREATE VIEW v_district_performance AS
SELECT 
    l.district_name,
    st.sensor_type,
    ds.data_source_name,
    t.date_actual,
    AVG(f.reading_value) as avg_reading,
    COUNT(f.reading_key) as total_readings,
    SUM(f.anomaly_flag) as anomaly_count,
    (SUM(f.anomaly_flag) * 100.0 / COUNT(f.reading_key)) as anomaly_rate_percent,
    AVG(f.quality_score) as avg_quality_score,
    COUNT(DISTINCT f.station_name) as unique_stations,
    COUNT(DISTINCT f.catchment) as unique_catchments
FROM fact_sensor_readings f
JOIN dim_locations l ON f.location_key = l.location_key
JOIN dim_time t ON f.time_key = t.time_key
JOIN dim_sensor_types st ON f.sensor_type_key = st.sensor_type_key
JOIN dim_data_sources ds ON f.data_source_key = ds.data_source_key
GROUP BY l.district_name, st.sensor_type, ds.data_source_name, t.date_actual;

-- Thames-specific monitoring view
CREATE VIEW v_thames_monitoring AS
SELECT 
    f.station_name,
    f.catchment,
    l.district_name,
    f.reading_value as water_level_meters,
    f.reading_timestamp,
    f.quality_score,
    CASE 
        WHEN f.reading_value < -1 THEN 'Low'
        WHEN f.reading_value > 5 THEN 'High'
        ELSE 'Normal'
    END as water_level_status
FROM fact_sensor_readings f
JOIN dim_locations l ON f.location_key = l.location_key
JOIN dim_sensor_types st ON f.sensor_type_key = st.sensor_type_key
WHERE st.sensor_type = 'water_level' 
AND f.station_name IS NOT NULL
ORDER BY f.reading_timestamp DESC;

-- Data lineage and quality tracking view
CREATE VIEW v_data_lineage AS
SELECT 
    f.unified_batch_id,
    f.pipeline_version,
    ds.data_source_name,
    ds.provider,
    COUNT(f.reading_key) as record_count,
    AVG(f.quality_score) as avg_quality,
    SUM(f.anomaly_flag) as anomaly_count,
    MIN(f.ingestion_timestamp) as first_ingested,
    MAX(f.ingestion_timestamp) as last_ingested
FROM fact_sensor_readings f
JOIN dim_data_sources ds ON f.data_source_key = ds.data_source_key
GROUP BY f.unified_batch_id, f.pipeline_version, ds.data_source_name, ds.provider
ORDER BY first_ingested DESC;

-- =============================================================================
-- GRANTS AND PERMISSIONS
-- =============================================================================

-- Grant permissions for dashboard user
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO dashboard_user;
-- GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO dashboard_user;