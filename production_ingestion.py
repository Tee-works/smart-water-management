#!/usr/bin/env python3
"""
Enhanced Production ingestion runner with historical data and multi-location support
Phase 1: More Data Implementation
"""

import os
import sys
import subprocess
from datetime import datetime
import time

def setup_environment():
    """Setup environment variables and dependencies"""
    print("🔧 Setting up enhanced environment...")
    
    # Set OpenWeather API key if not already set
    if not os.getenv('OPENWEATHER_API_KEY'):
        api_key = "api_key"
        os.environ['OPENWEATHER_API_KEY'] = api_key
        print(f"✅ OpenWeather API key configured")
    
    # Create data directory
    os.makedirs('data', exist_ok=True)
    print("✅ Data directory ready")

def setup_ingestion_mode():
    """Setup enhanced data ingestion mode with user options"""
    print("\n🚀 ENHANCED DATA INGESTION OPTIONS")
    print("=" * 50)
    print("1. Current data only (default - fast)")
    print("2. Multi-location current data (20 locations)")
    print("3. Historical data (1 week - medium)")
    print("4. Historical data (1 month - slow)")
    print("5. Historical data (3 months - very slow)")
    print("6. Historical data (1 year - extremely slow)")
    print("7. Everything! (Multi-location + Historical)")
    print()
    
    choice = input("🎯 Select ingestion mode (1-7) [default: 2]: ").strip() or "2"
    
    # Configuration based on choice
    config = {
        'historical_mode': False,
        'historical_days': 0,
        'multi_location_mode': True,
        'description': '',
        'estimated_records': 0,
        'estimated_time': ''
    }
    
    if choice == "1":
        config.update({
            'multi_location_mode': False,
            'description': 'Current data only',
            'estimated_records': 50,
            'estimated_time': '30 seconds'
        })
    elif choice == "2":
        config.update({
            'description': 'Multi-location current data',
            'estimated_records': 1200,
            'estimated_time': '2-3 minutes'
        })
    elif choice == "3":
        config.update({
            'historical_mode': True,
            'historical_days': 7,
            'description': '1 week historical + multi-location',
            'estimated_records': 8400,
            'estimated_time': '3-5 minutes'
        })
    elif choice == "4":
        config.update({
            'historical_mode': True,
            'historical_days': 30,
            'description': '1 month historical + multi-location',
            'estimated_records': 36000,
            'estimated_time': '8-12 minutes'
        })
    elif choice == "5":
        config.update({
            'historical_mode': True,
            'historical_days': 90,
            'description': '3 months historical + multi-location',
            'estimated_records': 108000,
            'estimated_time': '20-30 minutes'
        })
    elif choice == "6":
        config.update({
            'historical_mode': True,
            'historical_days': 365,
            'description': '1 year historical + multi-location',
            'estimated_records': 438000,
            'estimated_time': '1-2 hours'
        })
    elif choice == "7":
        config.update({
            'historical_mode': True,
            'historical_days': 180,  # 6 months for "everything"
            'description': 'Everything! (6 months historical + multi-location)',
            'estimated_records': 219000,
            'estimated_time': '45-90 minutes'
        })
    else:
        print("⚠️ Invalid choice, using default (multi-location current)")
        config.update({
            'description': 'Multi-location current data (default)',
            'estimated_records': 1200,
            'estimated_time': '2-3 minutes'
        })
    
    print(f"\n✅ Selected: {config['description']}")
    print(f"📊 Estimated records: {config['estimated_records']:,}")
    print(f"⏱️ Estimated time: {config['estimated_time']}")
    
    if config['historical_mode'] and config['historical_days'] > 30:
        print("\n⚠️ WARNING: Large historical data collection selected!")
        print("   This will make many API calls and take significant time.")
        print("   Consider starting with a smaller dataset first.")
        confirm = input("\n   Continue? (y/N): ").strip().lower()
        if confirm != 'y':
            print("   Operation cancelled. Please select a smaller option.")
            return setup_ingestion_mode()  # Recursive call to re-select
    
    return config

def run_enhanced_unified_ingestion(config):
    """Run the enhanced unified data ingestion pipeline"""
    print(f"\n🌊 Running enhanced unified data ingestion...")
    print(f"📋 Mode: {config['description']}")
    
    start_time = time.time()
    
    try:
        # Import and configure the enhanced platform
        sys.path.append('src')
        from enhanced_ingestion import UnifiedDataIngestionPlatform
        
        # Initialize platform with enhanced configuration
        platform = UnifiedDataIngestionPlatform()
        platform.historical_mode = config['historical_mode']
        platform.historical_days = config['historical_days']
        platform.multi_location_mode = config['multi_location_mode']
        
        print("\n🔧 Platform Configuration:")
        print(f"   Historical Mode: {'✅' if config['historical_mode'] else '❌'}")
        if config['historical_mode']:
            print(f"   Historical Days: {config['historical_days']}")
        print(f"   Multi-Location: {'✅' if config['multi_location_mode'] else '❌'}")
        
        # Show progress for longer operations
        if config['historical_mode'] and config['historical_days'] > 7:
            print(f"\n⏳ Starting large data collection...")
            print(f"   This may take {config['estimated_time']}")
            print(f"   Progress will be logged as data is collected...")
        
        # Run enhanced ingestion
        result = platform.run_unified_ingestion()
        
        end_time = time.time()
        actual_duration = end_time - start_time
        
        if result is not None and not result.empty:
            print(f"\n🎉 ENHANCED INGESTION COMPLETED!")
            print(f"⏱️ Actual time: {actual_duration/60:.1f} minutes")
            print(f"📊 Records collected: {len(result):,}")
            print(f"🎯 Performance: {len(result)/actual_duration:.1f} records/second")
        
        return result
        
    except Exception as e:
        print(f"❌ Enhanced ingestion failed: {e}")
        return None

def run_data_validation(data_file):
    """Run production data validation with enhanced reporting"""
    print(f"\n🔍 Running enhanced data validation on {data_file}...")
    
    try:
        # Update the validator to use the specific file
        sys.path.append('src')
        from data_validator import ProductionDataValidator
        import pandas as pd
        
        # Load the specific data file
        df = pd.read_csv(data_file)
        print(f"📊 Loaded {len(df):,} records for validation")
        
        # Show data overview
        print(f"\n📋 Data Overview:")
        print(f"   Date Range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"   Data Sources: {', '.join(df['data_source'].unique())}")
        print(f"   Sensor Types: {', '.join(df['sensor_type'].unique())}")
        print(f"   Districts: {', '.join(df['district'].unique())}")
        
        # Run validation
        validator = ProductionDataValidator()
        report = validator.validate_dataset(df)
        
        print(f"\n🎯 Validation Results:")
        print(f"   Overall Score: {report['overall_score']:.2f}")
        print(f"   Status: {report['status']}")
        
        summary = report['summary']
        print(f"   Issues: {summary['critical_issues']} critical, {summary['errors']} errors, {summary['warnings']} warnings")
        
        # Enhanced validation breakdown
        if 'data_quality_breakdown' in report:
            breakdown = report['data_quality_breakdown']
            print(f"\n📊 Quality Breakdown:")
            for category, score in breakdown.items():
                emoji = "✅" if score > 0.9 else "⚠️" if score > 0.7 else "❌"
                print(f"   {emoji} {category.replace('_', ' ').title()}: {score:.2f}")
        
        # Save validation report
        report_file = data_file.replace('.csv', '_validation_report.json')
        import json
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        print(f"\n💾 Validation report saved: {report_file}")
        
        return report
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return None

def generate_enhanced_summary_report(data_file, validation_report, config):
    """Generate an enhanced comprehensive summary report"""
    print(f"\n📈 Generating enhanced summary report...")
    
    try:
        import pandas as pd
        
        # Load data
        df = pd.read_csv(data_file)
        
        # Enhanced data analysis
        source_breakdown = df.groupby('data_source').size().to_dict()
        sensor_breakdown = df.groupby('sensor_type').size().to_dict()
        district_breakdown = df.groupby('district').size().to_dict()
        
        # Time-based analysis
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        date_range_days = (df['timestamp'].max() - df['timestamp'].min()).days
        
        # Enhanced summary
        summary = {
            'execution_time': datetime.now().isoformat(),
            'ingestion_configuration': {
                'mode': config['description'],
                'historical_mode': config['historical_mode'],
                'historical_days': config['historical_days'],
                'multi_location_mode': config['multi_location_mode'],
                'estimated_vs_actual': {
                    'estimated_records': config['estimated_records'],
                    'actual_records': len(df),
                    'efficiency': len(df) / max(config['estimated_records'], 1)
                }
            },
            'data_summary': {
                'total_records': len(df),
                'unique_sensors': df['sensor_id'].nunique(),
                'data_sources': list(df['data_source'].unique()),
                'sensor_types': list(df['sensor_type'].unique()),
                'districts': list(df['district'].unique()),
                'date_range': f"{df['timestamp'].min()} to {df['timestamp'].max()}",
                'date_range_days': date_range_days,
                'quality_score_avg': df['quality_score'].mean() if 'quality_score' in df.columns else 'N/A',
                'anomaly_rate': (df['anomaly_flag'].sum() / len(df) * 100) if 'anomaly_flag' in df.columns else 'N/A'
            },
            'detailed_breakdowns': {
                'by_data_source': source_breakdown,
                'by_sensor_type': sensor_breakdown,
                'by_district': district_breakdown
            },
            'validation_summary': {
                'overall_score': validation_report['overall_score'] if validation_report else 'N/A',
                'status': validation_report['status'] if validation_report else 'N/A',
                'critical_issues': validation_report['summary']['critical_issues'] if validation_report else 'N/A',
                'recommendations': validation_report['recommendations'][:3] if validation_report else []
            },
            'technical_metrics': {
                'file_size_mb': os.path.getsize(data_file) / (1024*1024),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024*1024),
                'records_per_day': len(df) / max(date_range_days, 1) if date_range_days > 0 else len(df)
            },
            'data_quality_insights': {
                'coverage_score': len(df) / (config['estimated_records'] if config['estimated_records'] > 0 else len(df)),
                'temporal_coverage': date_range_days,
                'geographic_coverage': len(df['district'].unique()),
                'sensor_diversity': len(df['sensor_type'].unique())
            }
        }
        
        # Save enhanced summary
        summary_file = data_file.replace('.csv', '_enhanced_summary.json')
        import json
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        print(f"📊 Enhanced summary report saved: {summary_file}")
        
        # Print enhanced key metrics
        print(f"\n🎯 ENHANCED KEY METRICS:")
        print(f"   Total Records: {summary['data_summary']['total_records']:,}")
        print(f"   Unique Sensors: {summary['data_summary']['unique_sensors']:,}")
        print(f"   Data Sources: {len(summary['data_summary']['data_sources'])}")
        print(f"   Sensor Types: {len(summary['data_summary']['sensor_types'])}")
        print(f"   Districts: {len(summary['data_summary']['districts'])}")
        print(f"   Date Range: {summary['data_summary']['date_range_days']} days")
        print(f"   Data Quality: {summary['validation_summary']['overall_score']}")
        print(f"   File Size: {summary['technical_metrics']['file_size_mb']:.2f} MB")
        print(f"   Records/Day: {summary['technical_metrics']['records_per_day']:.0f}")
        
        # Show top sources and types
        print(f"\n📊 TOP DATA SOURCES:")
        for source, count in sorted(source_breakdown.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"   • {source}: {count:,} records")
        
        print(f"\n🌐 SENSOR TYPE DISTRIBUTION:")
        for sensor_type, count in sorted(sensor_breakdown.items(), key=lambda x: x[1], reverse=True):
            print(f"   • {sensor_type}: {count:,} records")
        
        return summary
        
    except Exception as e:
        print(f"❌ Enhanced summary generation failed: {e}")
        return None

def main():
    """Enhanced main execution function"""
    print("🌊 SMART WATER MANAGEMENT - ENHANCED PRODUCTION INGESTION PIPELINE")
    print("=" * 80)
    print("Phase 1: More Data Implementation")
    print()
    
    # Setup
    setup_environment()
    
    # Get user configuration
    config = setup_ingestion_mode()
    
    # Run enhanced ingestion
    result_df = run_enhanced_unified_ingestion(config)
    
    if result_df is not None and not result_df.empty:
        # Find the latest data file by modification time (not creation time)
        import glob
        import os
        
        data_files = glob.glob('data/unified_sensor_data_*.csv')
        if data_files:
            # Sort by modification time (most recent first)
            latest_file = max(data_files, key=os.path.getmtime)
            print(f"\n📁 Latest data file: {latest_file}")
            
            # Verify this is the correct file by checking size
            file_size = os.path.getsize(latest_file)
            print(f"📏 File size: {file_size / 1024:.1f} KB")
            
            # Double-check record count
            import pandas as pd
            quick_check = pd.read_csv(latest_file)
            print(f"📊 Quick verification: {len(quick_check):,} records in file")
            
            if len(quick_check) != len(result_df):
                print(f"⚠️ WARNING: Mismatch detected!")
                print(f"   Expected: {len(result_df):,} records")
                print(f"   Found in file: {len(quick_check):,} records")
                print("   Creating new file with correct data...")
                
                # Save the correct data with a new timestamp
                from datetime import datetime
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                correct_file = f"data/unified_sensor_data_enhanced_{timestamp}.csv"
                result_df.to_csv(correct_file, index=False)
                latest_file = correct_file
                print(f"✅ Correct data saved to: {latest_file}")
            
            # Run enhanced validation
            validation_report = run_data_validation(latest_file)
            
            # Generate enhanced summary
            summary = generate_enhanced_summary_report(latest_file, validation_report, config)
            
            # Final status
            print(f"\n🎉 ENHANCED PIPELINE EXECUTION COMPLETE!")
            print(f"📊 Data: {latest_file}")
            if validation_report:
                print(f"🔍 Validation: {validation_report['status']}")
            print(f"📈 Enhanced Summary: Available in JSON format")
            print(f"🚀 Ready for warehouse loading and dashboard!")
            
            # Performance summary
            if summary:
                efficiency = summary['ingestion_configuration']['estimated_vs_actual']['efficiency']
                print(f"\n📈 PERFORMANCE SUMMARY:")
                print(f"   Efficiency: {efficiency:.1%} of estimated")
                if efficiency > 1.0:
                    print("   🎯 Exceeded expectations!")
                elif efficiency > 0.8:
                    print("   ✅ Good performance!")
                else:
                    print("   ⚠️ Below expectations - check configuration")
            
            # Next steps
            print(f"\n🔄 NEXT STEPS:")
            print(f"   1. Run warehouse loader: python src/warehouse_loader_real_data.py")
            print(f"   2. Start dashboard: python src/app.py")
            print(f"   3. View analytics: http://localhost:5000")
            
        else:
            print("❌ No data files found")
    else:
        print("❌ No data was ingested")
        print("\n🔧 TROUBLESHOOTING:")
        print("   • Check internet connection")
        print("   • Verify API endpoints are accessible")
        print("   • Try a smaller dataset first")
        print("   • Check logs for detailed error messages")

if __name__ == "__main__":
    main()