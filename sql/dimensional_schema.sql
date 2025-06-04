-- Water Management Data Warehouse Schema
-- Dimensional modeling for analytics and reporting

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

-- Time dimension with comprehensive date attributes
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

-- Sensor dimension with SCD Type 2 for tracking changes
CREATE TABLE dim_sensors (
    sensor_key BIGINT PRIMARY KEY,
    sensor_id VARCHAR(20) NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    district VARCHAR(100) NOT NULL,
    zone VARCHAR(100),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    installation_date DATE,
    manufacturer VARCHAR(100),
    model VARCHAR(100),
    firmware_version VARCHAR(50),
    status VARCHAR(20) NOT NULL,
    maintenance_schedule VARCHAR(50),
    -- SCD Type 2 fields
    effective_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Location hierarchy dimension
CREATE TABLE dim_locations (
    location_key INTEGER PRIMARY KEY,
    district_id VARCHAR(20) NOT NULL,
    district_name VARCHAR(100) NOT NULL,
    zone_id VARCHAR(20),
    zone_name VARCHAR(100),
    region_id VARCHAR(20),
    region_name VARCHAR(100),
    city VARCHAR(100) DEFAULT 'London',
    country VARCHAR(100) DEFAULT 'United Kingdom',
    population INTEGER,
    area_sq_km DECIMAL(10,2),
    infrastructure_type VARCHAR(50),
    priority_level VARCHAR(20),
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sensor type specifications and thresholds
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

-- =============================================================================
-- FACT TABLES
-- =============================================================================

-- Main fact table for sensor readings
CREATE TABLE fact_sensor_readings (
    reading_key BIGINT PRIMARY KEY,
    sensor_key BIGINT NOT NULL,
    time_key INTEGER NOT NULL,
    location_key INTEGER NOT NULL,
    sensor_type_key INTEGER NOT NULL,
    
    -- Measures
    reading_value DECIMAL(15,6) NOT NULL,
    quality_score DECIMAL(5,3),
    anomaly_flag INTEGER DEFAULT 0,
    anomaly_score DECIMAL(8,4),
    z_score DECIMAL(8,4),
    
    -- Technical fields
    reading_timestamp TIMESTAMP NOT NULL,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(100),
    data_source VARCHAR(50),
    
    -- Foreign key constraints
    FOREIGN KEY (sensor_key) REFERENCES dim_sensors(sensor_key),
    FOREIGN KEY (time_key) REFERENCES dim_time(time_key),
    FOREIGN KEY (location_key) REFERENCES dim_locations(location_key),
    FOREIGN KEY (sensor_type_key) REFERENCES dim_sensor_types(sensor_type_key)
);

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================

-- Fact table indexes
CREATE INDEX idx_fact_readings_timestamp ON fact_sensor_readings(reading_timestamp);
CREATE INDEX idx_fact_readings_sensor_time ON fact_sensor_readings(sensor_key, time_key);
CREATE INDEX idx_fact_readings_location_time ON fact_sensor_readings(location_key, time_key);
CREATE INDEX idx_fact_readings_batch ON fact_sensor_readings(batch_id);

-- Dimension table indexes
CREATE INDEX idx_dim_sensors_id ON dim_sensors(sensor_id);
CREATE INDEX idx_dim_sensors_current ON dim_sensors(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_sensors_type ON dim_sensors(sensor_type);
CREATE INDEX idx_dim_time_date ON dim_time(date_actual);
CREATE INDEX idx_dim_time_year_month ON dim_time(year, month);
CREATE INDEX idx_dim_locations_district ON dim_locations(district_id);

-- =============================================================================
-- ANALYTICAL VIEWS
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
    s.is_current
FROM dim_sensors s
JOIN dim_locations l ON s.district = l.district_name
JOIN dim_sensor_types st ON s.sensor_type = st.sensor_type
WHERE s.is_current = TRUE;

-- District performance dashboard view
CREATE VIEW v_district_performance AS
SELECT 
    l.district_name,
    st.sensor_type,
    t.date_actual,
    AVG(f.reading_value) as avg_reading,
    COUNT(f.reading_key) as total_readings,
    SUM(f.anomaly_flag) as anomaly_count,
    (SUM(f.anomaly_flag) * 100.0 / COUNT(f.reading_key)) as anomaly_rate_percent
FROM fact_sensor_readings f
JOIN dim_locations l ON f.location_key = l.location_key
JOIN dim_time t ON f.time_key = t.time_key
JOIN dim_sensor_types st ON f.sensor_type_key = st.sensor_type_key
GROUP BY l.district_name, st.sensor_type, t.date_actual;
