-- Enhanced Water Management Schema for Expanded Real API Data
-- Supports historical data, multi-location tracking, and advanced analytics

-- =============================================================================
-- DROP EXISTING TABLES (for clean restart)
-- =============================================================================
DROP TABLE IF EXISTS fact_sensor_readings CASCADE;
DROP TABLE IF EXISTS fact_sensor_readings_hourly CASCADE;
DROP TABLE IF EXISTS fact_anomaly_events CASCADE;
DROP TABLE IF EXISTS dim_sensors CASCADE;
DROP TABLE IF EXISTS dim_sensor_types CASCADE;
DROP TABLE IF EXISTS dim_locations CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;
DROP TABLE IF EXISTS dim_data_sources CASCADE;
DROP TABLE IF EXISTS dim_weather_conditions CASCADE;
DROP TABLE IF EXISTS dim_sensor_maintenance CASCADE;

-- =============================================================================
-- ENHANCED DIMENSION TABLES
-- =============================================================================

-- Enhanced time dimension with additional attributes
CREATE TABLE dim_time (
    time_key INTEGER PRIMARY KEY,
    date_actual DATE NOT NULL UNIQUE,
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
    fiscal_quarter INTEGER,
    week_start_date DATE,
    week_end_date DATE,
    month_start_date DATE,
    month_end_date DATE,
    quarter_start_date DATE,
    quarter_end_date DATE,
    days_in_month INTEGER,
    is_leap_year BOOLEAN,
    holiday_name VARCHAR(100),
    weather_season VARCHAR(20), -- meteorological season
    daylight_hours DECIMAL(4,2) -- estimated daylight hours for London
);

-- Enhanced data sources dimension with metadata
CREATE TABLE dim_data_sources (
    data_source_key INTEGER PRIMARY KEY,
    data_source_name VARCHAR(100) NOT NULL UNIQUE,
    source_type VARCHAR(50) NOT NULL,
    provider VARCHAR(100),
    api_endpoint VARCHAR(500),
    refresh_frequency_minutes INTEGER,
    requires_api_key BOOLEAN DEFAULT FALSE,
    reliability_score DECIMAL(3,2) DEFAULT 1.0,
    data_quality_grade VARCHAR(10),
    last_successful_ingestion TIMESTAMP,
    total_records_ingested BIGINT DEFAULT 0,
    average_latency_ms INTEGER,
    cost_per_1000_calls DECIMAL(10,2),
    rate_limit_per_hour INTEGER,
    description TEXT,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Enhanced location hierarchy with coordinates and metadata
CREATE TABLE dim_locations (
    location_key INTEGER PRIMARY KEY,
    location_id VARCHAR(50) NOT NULL UNIQUE,
    location_name VARCHAR(200),
    district_id VARCHAR(20) NOT NULL,
    district_name VARCHAR(100) NOT NULL,
    borough_name VARCHAR(100),
    zone_id VARCHAR(20),
    zone_name VARCHAR(100),
    region_name VARCHAR(100) DEFAULT 'Greater London',
    city VARCHAR(100) DEFAULT 'London',
    country VARCHAR(100) DEFAULT 'United Kingdom',
    country_code VARCHAR(2) DEFAULT 'GB',
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    elevation_meters DECIMAL(8,2),
    population INTEGER,
    area_sq_km DECIMAL(10,2),
    population_density_per_sq_km DECIMAL(10,2),
    infrastructure_type VARCHAR(50),
    water_network_zone VARCHAR(50),
    priority_level VARCHAR(20),
    risk_category VARCHAR(20),
    postal_code VARCHAR(20),
    grid_reference VARCHAR(20),
    time_zone VARCHAR(50) DEFAULT 'Europe/London',
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Enhanced sensor types with calibration and threshold management
CREATE TABLE dim_sensor_types (
    sensor_type_key INTEGER PRIMARY KEY,
    sensor_type VARCHAR(50) NOT NULL UNIQUE,
    category VARCHAR(50) NOT NULL,
    subcategory VARCHAR(50),
    measurement_unit VARCHAR(20) NOT NULL,
    measurement_unit_symbol VARCHAR(10),
    data_type VARCHAR(20), -- continuous, discrete, binary
    normal_min_value DECIMAL(10,3),
    normal_max_value DECIMAL(10,3),
    warning_min_value DECIMAL(10,3),
    warning_max_value DECIMAL(10,3),
    critical_min_value DECIMAL(10,3),
    critical_max_value DECIMAL(10,3),
    precision_decimal_places INTEGER DEFAULT 2,
    accuracy_percentage DECIMAL(5,2),
    calibration_frequency_days INTEGER,
    expected_lifespan_days INTEGER,
    maintenance_cost_per_year DECIMAL(10,2),
    power_consumption_watts DECIMAL(8,2),
    communication_protocol VARCHAR(50),
    sampling_rate_seconds INTEGER,
    description TEXT,
    manufacturer_specs JSONB,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Enhanced sensors dimension with detailed tracking
CREATE TABLE dim_sensors (
    sensor_key BIGINT PRIMARY KEY,
    sensor_id VARCHAR(100) NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    location_key INTEGER,
    district VARCHAR(100) NOT NULL,
    zone VARCHAR(100),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    elevation_meters DECIMAL(8,2),
    installation_date DATE,
    last_maintenance_date DATE,
    next_maintenance_date DATE,
    manufacturer VARCHAR(100),
    model VARCHAR(100),
    serial_number VARCHAR(100),
    firmware_version VARCHAR(50),
    hardware_version VARCHAR(50),
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    operational_status VARCHAR(50), -- online, offline, maintenance, faulty
    battery_level_percent DECIMAL(5,2),
    signal_strength_dbm INTEGER,
    uptime_hours BIGINT,
    total_readings BIGINT DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    maintenance_schedule VARCHAR(50),
    deployment_type VARCHAR(50), -- permanent, temporary, mobile
    network_type VARCHAR(50), -- wifi, cellular, lorawan, zigbee
    power_source VARCHAR(50), -- mains, battery, solar
    -- SCD Type 2 fields
    effective_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (location_key) REFERENCES dim_locations(location_key)
);

-- Weather conditions dimension for correlation analysis
CREATE TABLE dim_weather_conditions (
    weather_key INTEGER PRIMARY KEY,
    condition_code VARCHAR(20) NOT NULL,
    condition_name VARCHAR(100) NOT NULL,
    category VARCHAR(50), -- clear, cloudy, rain, snow, extreme
    severity_level INTEGER, -- 1-5 scale
    icon_code VARCHAR(20),
    description TEXT
);

-- Sensor maintenance tracking
CREATE TABLE dim_sensor_maintenance (
    maintenance_key INTEGER PRIMARY KEY,
    sensor_key BIGINT NOT NULL,
    maintenance_type VARCHAR(50), -- scheduled, emergency, calibration, replacement
    maintenance_date DATE NOT NULL,
    technician_id VARCHAR(50),
    duration_hours DECIMAL(5,2),
    cost DECIMAL(10,2),
    parts_replaced TEXT,
    notes TEXT,
    next_maintenance_date DATE,
    
    FOREIGN KEY (sensor_key) REFERENCES dim_sensors(sensor_key)
);

-- =============================================================================
-- ENHANCED FACT TABLES
-- =============================================================================

-- Main fact table with additional measures and attributes
CREATE TABLE fact_sensor_readings (
    reading_key BIGINT PRIMARY KEY,
    sensor_key BIGINT NOT NULL,
    time_key INTEGER NOT NULL,
    location_key INTEGER NOT NULL,
    sensor_type_key INTEGER NOT NULL,
    data_source_key INTEGER NOT NULL,
    weather_key INTEGER,
    
    -- Core measures
    reading_value DECIMAL(15,6) NOT NULL,
    reading_value_normalized DECIMAL(15,6), -- normalized to 0-1 scale
    quality_score DECIMAL(5,3),
    confidence_score DECIMAL(5,3),
    anomaly_flag INTEGER DEFAULT 0,
    anomaly_score DECIMAL(8,4),
    z_score DECIMAL(8,4),
    
    -- Additional measures
    rolling_avg_1h DECIMAL(15,6),
    rolling_avg_24h DECIMAL(15,6),
    rolling_std_24h DECIMAL(15,6),
    value_change_from_prev DECIMAL(15,6),
    value_change_pct DECIMAL(8,4),
    
    -- Environmental context
    temperature_celsius DECIMAL(5,2),
    humidity_percent DECIMAL(5,2),
    pressure_hpa DECIMAL(7,2),
    precipitation_mm DECIMAL(8,2),
    wind_speed_ms DECIMAL(6,2),
    
    -- Technical metadata
    reading_timestamp TIMESTAMP NOT NULL,
    ingestion_timestamp TIMESTAMP,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(100),
    unified_batch_id VARCHAR(100),
    pipeline_version VARCHAR(20),
    ingestion_source VARCHAR(100),
    ingestion_priority VARCHAR(20),
    processing_duration_ms INTEGER,
    
    -- Data lineage
    raw_value TEXT, -- original value before any transformation
    transformation_applied VARCHAR(200),
    validation_status VARCHAR(50),
    
    -- Location specifics
    exact_latitude DECIMAL(10,6),
    exact_longitude DECIMAL(10,6),
    location_accuracy_meters DECIMAL(6,2),
    
    -- Thames-specific fields
    station_name VARCHAR(200),
    station_reference VARCHAR(50),
    catchment VARCHAR(200),
    river_name VARCHAR(100),
    
    -- Forecasting support
    is_interpolated BOOLEAN DEFAULT FALSE,
    interpolation_method VARCHAR(50),
    forecast_value DECIMAL(15,6),
    forecast_confidence DECIMAL(5,3),
    
    -- Foreign key constraints
    FOREIGN KEY (sensor_key) REFERENCES dim_sensors(sensor_key),
    FOREIGN KEY (time_key) REFERENCES dim_time(time_key),
    FOREIGN KEY (location_key) REFERENCES dim_locations(location_key),
    FOREIGN KEY (sensor_type_key) REFERENCES dim_sensor_types(sensor_type_key),
    FOREIGN KEY (data_source_key) REFERENCES dim_data_sources(data_source_key),
    FOREIGN KEY (weather_key) REFERENCES dim_weather_conditions(weather_key)
);

-- Hourly aggregated fact table for faster analytics
CREATE TABLE fact_sensor_readings_hourly (
    hourly_key BIGINT PRIMARY KEY,
    sensor_key BIGINT NOT NULL,
    time_key INTEGER NOT NULL,
    location_key INTEGER NOT NULL,
    sensor_type_key INTEGER NOT NULL,
    hour_starting TIMESTAMP NOT NULL,
    
    -- Aggregated measures
    readings_count INTEGER NOT NULL,
    avg_value DECIMAL(15,6),
    min_value DECIMAL(15,6),
    max_value DECIMAL(15,6),
    sum_value DECIMAL(20,6),
    std_dev_value DECIMAL(15,6),
    median_value DECIMAL(15,6),
    percentile_25 DECIMAL(15,6),
    percentile_75 DECIMAL(15,6),
    
    -- Quality metrics
    avg_quality_score DECIMAL(5,3),
    min_quality_score DECIMAL(5,3),
    readings_below_quality_threshold INTEGER,
    
    -- Anomaly metrics
    anomaly_count INTEGER,
    anomaly_rate DECIMAL(5,4),
    max_anomaly_score DECIMAL(8,4),
    
    -- Pattern detection
    trend_direction VARCHAR(20), -- increasing, decreasing, stable
    volatility_score DECIMAL(8,4),
    seasonality_score DECIMAL(8,4),
    
    -- Operational metrics
    uptime_minutes INTEGER,
    downtime_minutes INTEGER,
    missing_readings INTEGER,
    
    FOREIGN KEY (sensor_key) REFERENCES dim_sensors(sensor_key),
    FOREIGN KEY (time_key) REFERENCES dim_time(time_key),
    FOREIGN KEY (location_key) REFERENCES dim_locations(location_key),
    FOREIGN KEY (sensor_type_key) REFERENCES dim_sensor_types(sensor_type_key)
);

-- Anomaly events fact table for detailed tracking
CREATE TABLE fact_anomaly_events (
    anomaly_event_key BIGINT PRIMARY KEY,
    sensor_key BIGINT NOT NULL,
    time_key INTEGER NOT NULL,
    location_key INTEGER NOT NULL,
    
    -- Event details
    event_start_timestamp TIMESTAMP NOT NULL,
    event_end_timestamp TIMESTAMP,
    duration_minutes INTEGER,
    severity_level INTEGER, -- 1-5 scale
    anomaly_type VARCHAR(50), -- spike, drop, drift, noise, pattern
    
    -- Measurements
    baseline_value DECIMAL(15,6),
    anomaly_value DECIMAL(15,6),
    deviation_amount DECIMAL(15,6),
    deviation_percentage DECIMAL(8,4),
    
    -- Context
    preceded_by_maintenance BOOLEAN DEFAULT FALSE,
    weather_related BOOLEAN DEFAULT FALSE,
    confirmed_incident BOOLEAN DEFAULT FALSE,
    false_positive BOOLEAN DEFAULT FALSE,
    
    -- Response
    alert_sent BOOLEAN DEFAULT FALSE,
    alert_timestamp TIMESTAMP,
    acknowledged_by VARCHAR(100),
    resolution_timestamp TIMESTAMP,
    resolution_notes TEXT,
    
    FOREIGN KEY (sensor_key) REFERENCES dim_sensors(sensor_key),
    FOREIGN KEY (time_key) REFERENCES dim_time(time_key),
    FOREIGN KEY (location_key) REFERENCES dim_locations(location_key)
);

-- =============================================================================
-- ENHANCED INDEXES FOR PERFORMANCE
-- =============================================================================

-- Fact table indexes
CREATE INDEX idx_fact_readings_timestamp ON fact_sensor_readings(reading_timestamp);
CREATE INDEX idx_fact_readings_sensor_time ON fact_sensor_readings(sensor_key, time_key);
CREATE INDEX idx_fact_readings_location_time ON fact_sensor_readings(location_key, time_key);
CREATE INDEX idx_fact_readings_batch ON fact_sensor_readings(unified_batch_id);
CREATE INDEX idx_fact_readings_data_source ON fact_sensor_readings(data_source_key);
CREATE INDEX idx_fact_readings_ingestion ON fact_sensor_readings(ingestion_timestamp);
CREATE INDEX idx_fact_readings_anomaly ON fact_sensor_readings(anomaly_flag) WHERE anomaly_flag = 1;
CREATE INDEX idx_fact_readings_quality ON fact_sensor_readings(quality_score);
CREATE INDEX idx_fact_readings_station ON fact_sensor_readings(station_name) WHERE station_name IS NOT NULL;

-- Hourly fact indexes
CREATE INDEX idx_fact_hourly_sensor_hour ON fact_sensor_readings_hourly(sensor_key, hour_starting);
CREATE INDEX idx_fact_hourly_location_hour ON fact_sensor_readings_hourly(location_key, hour_starting);
CREATE INDEX idx_fact_hourly_anomaly_rate ON fact_sensor_readings_hourly(anomaly_rate) WHERE anomaly_rate > 0;

-- Dimension indexes
CREATE INDEX idx_dim_sensors_id ON dim_sensors(sensor_id);
CREATE INDEX idx_dim_sensors_current ON dim_sensors(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_sensors_type ON dim_sensors(sensor_type);
CREATE INDEX idx_dim_sensors_status ON dim_sensors(operational_status);
CREATE INDEX idx_dim_time_date ON dim_time(date_actual);
CREATE INDEX idx_dim_time_year_month ON dim_time(year, month);
CREATE INDEX idx_dim_time_hour_parts ON dim_time(year, month, day_of_month);
CREATE INDEX idx_dim_locations_district ON dim_locations(district_id);
CREATE INDEX idx_dim_locations_coords ON dim_locations(latitude, longitude);
CREATE INDEX idx_data_sources_name ON dim_data_sources(data_source_name);

-- =============================================================================
-- MATERIALIZED VIEWS FOR PERFORMANCE
-- =============================================================================

-- Real-time sensor status
CREATE MATERIALIZED VIEW mv_sensor_current_status AS
SELECT 
    s.sensor_id,
    s.sensor_type,
    s.operational_status,
    l.location_name,
    l.district_name,
    st.measurement_unit,
    latest.last_reading_time,
    latest.last_reading_value,
    latest.hours_since_last_reading,
    latest.avg_quality_24h,
    latest.reading_count_24h,
    CASE 
        WHEN latest.hours_since_last_reading > 2 THEN 'OFFLINE'
        WHEN latest.avg_quality_24h < 0.7 THEN 'DEGRADED'
        WHEN s.operational_status != 'online' THEN s.operational_status
        ELSE 'HEALTHY'
    END as health_status
FROM dim_sensors s
JOIN dim_locations l ON s.location_key = l.location_key
JOIN dim_sensor_types st ON s.sensor_type = st.sensor_type
LEFT JOIN LATERAL (
    SELECT 
        f.sensor_key,
        MAX(f.reading_timestamp) as last_reading_time,
        EXTRACT(EPOCH FROM (NOW() - MAX(f.reading_timestamp)))/3600 as hours_since_last_reading,
        AVG(CASE WHEN f.reading_timestamp > NOW() - INTERVAL '24 hours' THEN f.quality_score END) as avg_quality_24h,
        COUNT(CASE WHEN f.reading_timestamp > NOW() - INTERVAL '24 hours' THEN 1 END) as reading_count_24h,
        (array_agg(f.reading_value ORDER BY f.reading_timestamp DESC))[1] as last_reading_value
    FROM fact_sensor_readings f
    WHERE f.sensor_key = s.sensor_key
    AND f.reading_timestamp > NOW() - INTERVAL '48 hours'
    GROUP BY f.sensor_key
) latest ON TRUE
WHERE s.is_current = TRUE;

CREATE INDEX idx_mv_sensor_status_health ON mv_sensor_current_status(health_status);
CREATE INDEX idx_mv_sensor_status_district ON mv_sensor_current_status(district_name);

-- District performance summary
CREATE MATERIALIZED VIEW mv_district_performance_daily AS
SELECT 
    l.district_name,
    t.date_actual,
    st.sensor_type,
    st.measurement_unit,
    COUNT(DISTINCT f.sensor_key) as active_sensors,
    COUNT(f.reading_key) as total_readings,
    AVG(f.reading_value) as avg_reading,
    STDDEV(f.reading_value) as std_reading,
    MIN(f.reading_value) as min_reading,
    MAX(f.reading_value) as max_reading,
    AVG(f.quality_score) as avg_quality,
    SUM(f.anomaly_flag) as anomaly_count,
    AVG(f.anomaly_score) as avg_anomaly_score,
    SUM(CASE WHEN f.quality_score < 0.7 THEN 1 ELSE 0 END) as low_quality_readings,
    AVG(f.temperature_celsius) as avg_temperature,
    AVG(f.humidity_percent) as avg_humidity,
    SUM(f.precipitation_mm) as total_precipitation
FROM fact_sensor_readings f
JOIN dim_locations l ON f.location_key = l.location_key
JOIN dim_time t ON f.time_key = t.time_key
JOIN dim_sensor_types st ON f.sensor_type_key = st.sensor_type_key
WHERE t.date_actual >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY l.district_name, t.date_actual, st.sensor_type, st.measurement_unit;

CREATE INDEX idx_mv_district_perf_date ON mv_district_performance_daily(date_actual);
CREATE INDEX idx_mv_district_perf_district ON mv_district_performance_daily(district_name);

-- =============================================================================
-- PARTITIONING SUPPORT (PostgreSQL 12+)
-- =============================================================================

-- Create partitioned version of fact table for better performance
CREATE TABLE fact_sensor_readings_partitioned (
    reading_key BIGINT NOT NULL,
    sensor_key BIGINT NOT NULL,
    time_key INTEGER NOT NULL,
    location_key INTEGER NOT NULL,
    sensor_type_key INTEGER NOT NULL,
    data_source_key INTEGER NOT NULL,
    weather_key INTEGER,
    
    -- Core measures
    reading_value DECIMAL(15,6) NOT NULL,
    reading_value_normalized DECIMAL(15,6), -- normalized to 0-1 scale
    quality_score DECIMAL(5,3),
    confidence_score DECIMAL(5,3),
    anomaly_flag INTEGER DEFAULT 0,
    anomaly_score DECIMAL(8,4),
    z_score DECIMAL(8,4),
    
    -- Additional measures
    rolling_avg_1h DECIMAL(15,6),
    rolling_avg_24h DECIMAL(15,6),
    rolling_std_24h DECIMAL(15,6),
    value_change_from_prev DECIMAL(15,6),
    value_change_pct DECIMAL(8,4),
    
    -- Environmental context
    temperature_celsius DECIMAL(5,2),
    humidity_percent DECIMAL(5,2),
    pressure_hpa DECIMAL(7,2),
    precipitation_mm DECIMAL(8,2),
    wind_speed_ms DECIMAL(6,2),
    
    -- Technical metadata
    reading_timestamp TIMESTAMP NOT NULL,
    ingestion_timestamp TIMESTAMP,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(100),
    unified_batch_id VARCHAR(100),
    pipeline_version VARCHAR(20),
    ingestion_source VARCHAR(100),
    ingestion_priority VARCHAR(20),
    processing_duration_ms INTEGER,
    
    -- Data lineage
    raw_value TEXT, -- original value before any transformation
    transformation_applied VARCHAR(200),
    validation_status VARCHAR(50),
    
    -- Location specifics
    exact_latitude DECIMAL(10,6),
    exact_longitude DECIMAL(10,6),
    location_accuracy_meters DECIMAL(6,2),
    
    -- Thames-specific fields
    station_name VARCHAR(200),
    station_reference VARCHAR(50),
    catchment VARCHAR(200),
    river_name VARCHAR(100),
    
    -- Forecasting support
    is_interpolated BOOLEAN DEFAULT FALSE,
    interpolation_method VARCHAR(50),
    forecast_value DECIMAL(15,6),
    forecast_confidence DECIMAL(5,3),
    
    -- Foreign key constraints
    PRIMARY KEY (reading_key, reading_timestamp),  -- Include the partition column in the primary key
    FOREIGN KEY (sensor_key) REFERENCES dim_sensors(sensor_key),
    FOREIGN KEY (time_key) REFERENCES dim_time(time_key),
    FOREIGN KEY (location_key) REFERENCES dim_locations(location_key),
    FOREIGN KEY (sensor_type_key) REFERENCES dim_sensor_types(sensor_type_key),
    FOREIGN KEY (data_source_key) REFERENCES dim_data_sources(data_source_key),
    FOREIGN KEY (weather_key) REFERENCES dim_weather_conditions(weather_key)
)
PARTITION BY RANGE (reading_timestamp);  -- Partitioning by the reading_timestamp

-- Example partition creation (adjust based on your needs)
CREATE TABLE fact_sensor_readings_y2025m01 PARTITION OF fact_sensor_readings_partitioned
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
    
CREATE TABLE fact_sensor_readings_y2025m02 PARTITION OF fact_sensor_readings_partitioned
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
    
CREATE TABLE fact_sensor_readings_y2025m03 PARTITION OF fact_sensor_readings_partitioned
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');


-- =============================================================================
-- FUNCTIONS AND TRIGGERS
-- =============================================================================

-- Function to automatically update sensor operational status
CREATE OR REPLACE FUNCTION update_sensor_operational_status()
RETURNS TRIGGER AS $$
BEGIN
    -- Update sensor's last activity
    UPDATE dim_sensors
    SET 
        operational_status = CASE 
            WHEN NEW.quality_score < 0.5 THEN 'faulty'
            WHEN NEW.quality_score < 0.7 THEN 'degraded'
            ELSE 'online'
        END,
        total_readings = total_readings + 1,
        error_count = error_count + CASE WHEN NEW.quality_score < 0.5 THEN 1 ELSE 0 END,
        updated_timestamp = CURRENT_TIMESTAMP
    WHERE sensor_key = NEW.sensor_key
    AND is_current = TRUE;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for operational status updates
CREATE TRIGGER trg_update_sensor_status
AFTER INSERT ON fact_sensor_readings
FOR EACH ROW
EXECUTE FUNCTION update_sensor_operational_status();

-- Function to refresh materialized views
CREATE OR REPLACE FUNCTION refresh_materialized_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_sensor_current_status;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_district_performance_daily;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- GRANTS AND PERMISSIONS
-- =============================================================================

-- Create read-only role for dashboard
CREATE ROLE dashboard_readonly;
GRANT CONNECT ON DATABASE water_analytics TO dashboard_readonly;
GRANT USAGE ON SCHEMA public TO dashboard_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dashboard_readonly;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO dashboard_readonly;

-- Create data ingestion role
CREATE ROLE data_ingestion;
GRANT CONNECT ON DATABASE water_analytics TO data_ingestion;
GRANT USAGE ON SCHEMA public TO data_ingestion;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO data_ingestion;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO data_ingestion;