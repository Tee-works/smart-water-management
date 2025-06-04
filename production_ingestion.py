#!/usr/bin/env python3
"""
Production ingestion runner with enhanced setup and validation
"""

import os
import sys
import subprocess
from datetime import datetime

def setup_environment():
    """Setup environment variables and dependencies"""
    print("🔧 Setting up environment...")
    
    # Set OpenWeather API key if not already set
    if not os.getenv('OPENWEATHER_API_KEY'):
        api_key = "api_key"
        os.environ['OPENWEATHER_API_KEY'] = api_key
        print(f"✅ OpenWeather API key configured")
    
    # Create data directory
    os.makedirs('data', exist_ok=True)
    print("✅ Data directory ready")

def run_unified_ingestion():
    """Run the unified data ingestion pipeline"""
    print("\n🌊 Running unified data ingestion...")
    
    try:
        # Import and run the unified ingestion
        sys.path.append('src')
        from data_ingestion import main as run_ingestion
        
        result = run_ingestion()
        return result
        
    except Exception as e:
        print(f"Ingestion failed: {e}")
        return None

def run_data_validation(data_file):
    """Run production data validation"""
    print(f"\n🔍 Running data validation on {data_file}...")
    
    try:
        # Update the validator to use the specific file
        sys.path.append('src')
        from data_validator import ProductionDataValidator
        import pandas as pd
        
        # Load the specific data file
        df = pd.read_csv(data_file)
        print(f"Loaded {len(df)} records for validation")
        
        # Run validation
        validator = ProductionDataValidator()
        report = validator.validate_dataset(df)
        
        print(f"\nValidation Results:")
        print(f"Overall Score: {report['overall_score']:.2f}")
        print(f"Status: {report['status']}")
        
        summary = report['summary']
        print(f"Issues: {summary['critical_issues']} critical, {summary['errors']} errors, {summary['warnings']} warnings")
        
        # Save validation report
        report_file = data_file.replace('.csv', '_validation_report.json')
        import json
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        print(f"📋 Validation report saved: {report_file}")
        
        return report
        
    except Exception as e:
        print(f"Validation failed: {e}")
        return None

def generate_summary_report(data_file, validation_report):
    """Generate a comprehensive summary report"""
    print(f"\n📈 Generating summary report...")
    
    try:
        import pandas as pd
        
        # Load data
        df = pd.read_csv(data_file)
        
        # Generate summary
        summary = {
            'execution_time': datetime.now().isoformat(),
            'data_summary': {
                'total_records': len(df),
                'data_sources': list(df['data_source'].unique()),
                'sensor_types': list(df['sensor_type'].unique()),
                'districts': list(df['district'].unique()),
                'date_range': f"{df['timestamp'].min()} to {df['timestamp'].max()}",
                'quality_score_avg': df['quality_score'].mean() if 'quality_score' in df.columns else 'N/A',
                'anomaly_rate': (df['anomaly_flag'].sum() / len(df) * 100) if 'anomaly_flag' in df.columns else 'N/A'
            },
            'validation_summary': {
                'overall_score': validation_report['overall_score'] if validation_report else 'N/A',
                'status': validation_report['status'] if validation_report else 'N/A',
                'critical_issues': validation_report['summary']['critical_issues'] if validation_report else 'N/A',
                'recommendations': validation_report['recommendations'][:3] if validation_report else []
            },
            'source_breakdown': df.groupby('data_source').size().to_dict(),
            'technical_metrics': {
                'file_size_mb': os.path.getsize(data_file) / (1024*1024),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024*1024)
            }
        }
        
        # Save summary
        summary_file = data_file.replace('.csv', '_summary.json')
        import json
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        print(f"📊 Summary report saved: {summary_file}")
        
        # Print key metrics
        print(f"\n🎯 KEY METRICS:")
        print(f"   Total Records: {summary['data_summary']['total_records']:,}")
        print(f"   Data Sources: {len(summary['data_summary']['data_sources'])}")
        print(f"   Sensor Types: {len(summary['data_summary']['sensor_types'])}")
        print(f"   Districts: {len(summary['data_summary']['districts'])}")
        print(f"   Data Quality: {summary['validation_summary']['overall_score']}")
        print(f"   File Size: {summary['technical_metrics']['file_size_mb']:.2f} MB")
        
        return summary
        
    except Exception as e:
        print(f"Summary generation failed: {e}")
        return None

def main():
    """Main execution function"""
    print("🌊 SMART WATER MANAGEMENT - PRODUCTION INGESTION PIPELINE")
    print("=" * 70)
    
    # Setup
    setup_environment()
    
    # Run ingestion
    result_df = run_unified_ingestion()
    
    if result_df is not None and not result_df.empty:
        # Find the latest data file
        import glob
        data_files = glob.glob('data/unified_sensor_data_*.csv')
        if data_files:
            latest_file = max(data_files, key=os.path.getctime)
            print(f"📁 Latest data file: {latest_file}")
            
            # Run validation
            validation_report = run_data_validation(latest_file)
            
            # Generate summary
            summary = generate_summary_report(latest_file, validation_report)
            
            # Final status
            print(f"\n🎉 PIPELINE EXECUTION COMPLETE!")
            print(f"📊 Data: {latest_file}")
            if validation_report:
                print(f"🔍 Validation: {validation_report['status']}")
            print(f"📈 Summary: Available in JSON format")
            
            print(f"\nReady for dashboard and analytics!")
            
        else:
            print("No data files found")
    else:
        print("No data was ingested")

if __name__ == "__main__":
    main()