import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Simple water sensor data generator
def generate_sensor_data():
    print("Generating water sensor data...")
    
    # Create data directory
    os.makedirs('data', exist_ok=True)
    
    # Generate 30 days of data
    start_date = datetime.now() - timedelta(days=30)
    
    sensors = []
    readings = []
    
    # Create 20 sensors across 5 districts
    districts = ['Central', 'North', 'South', 'East', 'West']
    sensor_types = ['flow', 'pressure', 'quality', 'temperature']
    
    sensor_id = 1
    for district in districts:
        for sensor_type in sensor_types:
            sensors.append({
                'sensor_id': f'WS_{sensor_id:03d}',
                'sensor_type': sensor_type,
                'district': district,
                'latitude': round(random.uniform(51.3, 51.7), 6),
                'longitude': round(random.uniform(-0.5, 0.2), 6),
                'status': 'active'
            })
            sensor_id += 1
    
    # Generate readings for each sensor
    for sensor in sensors:
        for day in range(30):
            current_date = start_date + timedelta(days=day)
            
            # Generate 24 hourly readings
            for hour in range(24):
                timestamp = current_date + timedelta(hours=hour)
                
                # Generate realistic values based on sensor type
                if sensor['sensor_type'] == 'flow':
                    base_value = 50
                    if 6 <= hour <= 9 or 18 <= hour <= 21:  # Peak hours
                        base_value *= 1.5
                    elif 22 <= hour <= 5:  # Night
                        base_value *= 0.3
                    value = max(0, np.random.normal(base_value, base_value * 0.2))
                    unit = 'L/s'
                
                elif sensor['sensor_type'] == 'pressure':
                    base_value = 40
                    if 6 <= hour <= 9 or 18 <= hour <= 21:  # Lower during peak
                        base_value -= 5
                    value = max(10, np.random.normal(base_value, 3))
                    unit = 'PSI'
                
                elif sensor['sensor_type'] == 'quality':
                    value = np.random.normal(7.2, 0.3)  # pH
                    value = max(6.0, min(9.0, value))
                    unit = 'pH'
                
                else:  # temperature
                    value = np.random.normal(15, 3)  # Celsius
                    value = max(2, min(25, value))
                    unit = 'C'
                
                # Quality score and anomaly flag
                quality_score = random.uniform(0.7, 1.0)
                anomaly_flag = 1 if random.random() < 0.05 else 0
                
                readings.append({
                    'timestamp': timestamp,
                    'sensor_id': sensor['sensor_id'],
                    'sensor_type': sensor['sensor_type'],
                    'district': sensor['district'],
                    'value': round(value, 2),
                    'unit': unit,
                    'quality_score': round(quality_score, 3),
                    'anomaly_flag': anomaly_flag
                })
    
    # Save to CSV files
    sensors_df = pd.DataFrame(sensors)
    readings_df = pd.DataFrame(readings)
    
    sensors_df.to_csv('data/sensor_locations.csv', index=False)
    readings_df.to_csv('data/sensor_readings.csv', index=False)
    
    print(f"Generated {len(sensors)} sensors and {len(readings)} readings")
    print("Files saved: data/sensor_locations.csv, data/sensor_readings.csv")
    print("\nSample data:")
    print(readings_df.head())

if __name__ == "__main__":
    generate_sensor_data()