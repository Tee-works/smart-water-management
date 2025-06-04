"""
Production-grade data validation for real-world API ingestion
Handles schema validation, data quality checks, and anomaly detection
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import logging
from dataclasses import dataclass
from enum import Enum
import json

class ValidationSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

@dataclass
class ValidationResult:
    field: str
    severity: ValidationSeverity
    message: str
    failed_count: int
    total_count: int
    failed_values: List[Any] = None
    
    @property
    def failure_rate(self) -> float:
        return (self.failed_count / self.total_count) if self.total_count > 0 else 0

class ProductionDataValidator:
    """
    Enterprise-grade data validation for water management platform
    - Schema validation with flexible constraints
    - Business rule validation
    - Data quality scoring
    - Anomaly detection with context
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.validation_results = []
        
        # Define expected schema with validation rules
        self.expected_schema = {
            'timestamp': {
                'type': 'datetime64[ns]',
                'nullable': False,
                'business_rules': ['not_future', 'not_too_old'],
                'description': 'Reading timestamp'
            },
            'sensor_id': {
                'type': 'object',
                'nullable': False,
                'pattern': r'^[A-Z0-9_]+$',
                'min_length': 5,
                'business_rules': ['valid_format'],
                'description': 'Unique sensor identifier'
            },
            'sensor_type': {
                'type': 'object',
                'nullable': False,
                'allowed_values': ['flow', 'pressure', 'quality', 'temperature', 'water_level', 'air_quality', 'precipitation'],
                'description': 'Type of sensor measurement'
            },
            'district': {
                'type': 'object',
                'nullable': False,
                'allowed_values': ['Central', 'North', 'South', 'East', 'West', 'Thames Valley'],
                'description': 'Geographic district'
            },
            'value': {
                'type': 'float64',
                'nullable': False,
                'min_value': -100,
                'max_value': 10000,
                'business_rules': ['sensor_specific_ranges'],
                'description': 'Sensor reading value'
            },
            'unit': {
                'type': 'object',
                'nullable': False,
                'allowed_values': ['L/s', 'PSI', 'pH', 'C', '°C', 'meters', 'm³/s', 'AQI', 'mm', 'units'],
                'description': 'Unit of measurement'
            },
            'quality_score': {
                'type': 'float64',
                'nullable': True,
                'min_value': 0.0,
                'max_value': 1.0,
                'description': 'Data quality score (0-1)'
            },
            'anomaly_flag': {
                'type': 'int64',
                'nullable': True,
                'allowed_values': [0, 1],
                'description': 'Binary anomaly indicator'
            },
            'data_source': {
                'type': 'object',
                'nullable': False,
                'allowed_values': ['UK_Environment_Agency', 'OpenMeteo_API', 'OpenWeatherMap_API', 'MOCK_DATA'],
                'description': 'Source of the data'
            }
        }
        
        # Sensor-specific validation ranges
        self.sensor_ranges = {
            'flow': {'min': 0, 'max': 1000, 'unit': ['L/s', 'm³/s']},
            'pressure': {'min': 0, 'max': 100, 'unit': ['PSI']},
            'quality': {'min': 0, 'max': 14, 'unit': ['pH']},
            'temperature': {'min': -10, 'max': 40, 'unit': ['C', '°C']},
            'water_level': {'min': -5, 'max': 20, 'unit': ['meters']},
            'air_quality': {'min': 1, 'max': 5, 'unit': ['AQI']},
            'precipitation': {'min': 0, 'max': 200, 'unit': ['mm']}
        }
    
    def validate_dataset(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Comprehensive dataset validation
        Returns validation report with scores and recommendations
        """
        self.validation_results = []
        
        if df.empty:
            return {
                'overall_score': 0.0,
                'status': 'FAILED',
                'message': 'Dataset is empty',
                'results': [],
                'recommendations': ['Investigate data ingestion pipeline']
            }
        
        self.logger.info(f"Validating dataset with {len(df)} records and {len(df.columns)} columns")
        
        # 1. Schema validation
        schema_results = self._validate_schema(df)
        
        # 2. Business rules validation
        business_results = self._validate_business_rules(df)
        
        # 3. Data quality checks
        quality_results = self._validate_data_quality(df)
        
        # 4. Cross-field validation
        cross_field_results = self._validate_cross_field_consistency(df)
        
        # 5. Statistical validation
        statistical_results = self._validate_statistical_properties(df)
        
        # Compile overall results
        all_results = (schema_results + business_results + quality_results + 
                      cross_field_results + statistical_results)
        
        overall_score = self._calculate_overall_score(all_results)
        status = self._determine_status(overall_score, all_results)
        recommendations = self._generate_recommendations(all_results)
        
        validation_report = {
            'timestamp': datetime.now().isoformat(),
            'dataset_info': {
                'record_count': len(df),
                'column_count': len(df.columns),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024 * 1024),
                'date_range': f"{df['timestamp'].min()} to {df['timestamp'].max()}" if 'timestamp' in df.columns else 'N/A'
            },
            'overall_score': overall_score,
            'status': status,
            'summary': self._generate_summary(all_results),
            'detailed_results': [
                {
                    'field': result.field,
                    'severity': result.severity.value,
                    'message': result.message,
                    'failure_rate': result.failure_rate,
                    'failed_count': result.failed_count,
                    'total_count': result.total_count
                }
                for result in all_results
            ],
            'recommendations': recommendations,
            'data_quality_breakdown': self._generate_quality_breakdown(df, all_results)
        }
        
        self._log_validation_summary(validation_report)
        
        return validation_report
    
    def _validate_schema(self, df: pd.DataFrame) -> List[ValidationResult]:
        """Validate dataset schema against expected schema"""
        results = []
        
        # Check required columns
        expected_columns = set(self.expected_schema.keys())
        actual_columns = set(df.columns)
        
        missing_columns = expected_columns - actual_columns
        extra_columns = actual_columns - expected_columns
        
        if missing_columns:
            results.append(ValidationResult(
                field='schema',
                severity=ValidationSeverity.ERROR,
                message=f"Missing required columns: {list(missing_columns)}",
                failed_count=len(missing_columns),
                total_count=len(expected_columns)
            ))
        
        if extra_columns:
            results.append(ValidationResult(
                field='schema',
                severity=ValidationSeverity.WARNING,
                message=f"Unexpected columns found: {list(extra_columns)}",
                failed_count=len(extra_columns),
                total_count=len(actual_columns)
            ))
        
        # Validate each column
        for column, rules in self.expected_schema.items():
            if column not in df.columns:
                continue
                
            column_results = self._validate_column(df, column, rules)
            results.extend(column_results)
        
        return results
    
    def _validate_column(self, df: pd.DataFrame, column: str, rules: Dict) -> List[ValidationResult]:
        """Validate a specific column against its rules"""
        results = []
        series = df[column]
        
        # Null validation
        if not rules.get('nullable', True):
            null_count = series.isnull().sum()
            if null_count > 0:
                results.append(ValidationResult(
                    field=column,
                    severity=ValidationSeverity.ERROR,
                    message=f"Column {column} has {null_count} null values but should not be nullable",
                    failed_count=null_count,
                    total_count=len(series)
                ))
        
        # Data type validation (for non-null values)
        non_null_series = series.dropna()
        if not non_null_series.empty:
            expected_type = rules.get('type')
            if expected_type and expected_type != 'object':
                try:
                    if expected_type == 'datetime64[ns]':
                        pd.to_datetime(non_null_series)
                    elif expected_type == 'float64':
                        pd.to_numeric(non_null_series, errors='raise')
                    elif expected_type == 'int64':
                        pd.to_numeric(non_null_series, errors='raise', downcast='integer')
                except Exception:
                    results.append(ValidationResult(
                        field=column,
                        severity=ValidationSeverity.ERROR,
                        message=f"Column {column} contains values incompatible with expected type {expected_type}",
                        failed_count=len(non_null_series),
                        total_count=len(series)
                    ))
            
            # Value range validation
            if 'min_value' in rules or 'max_value' in rules:
                numeric_series = pd.to_numeric(non_null_series, errors='coerce')
                
                min_val = rules.get('min_value')
                max_val = rules.get('max_value')
                
                if min_val is not None:
                    below_min = (numeric_series < min_val).sum()
                    if below_min > 0:
                        results.append(ValidationResult(
                            field=column,
                            severity=ValidationSeverity.WARNING,
                            message=f"Column {column} has {below_min} values below minimum {min_val}",
                            failed_count=below_min,
                            total_count=len(numeric_series)
                        ))
                
                if max_val is not None:
                    above_max = (numeric_series > max_val).sum()
                    if above_max > 0:
                        results.append(ValidationResult(
                            field=column,
                            severity=ValidationSeverity.WARNING,
                            message=f"Column {column} has {above_max} values above maximum {max_val}",
                            failed_count=above_max,
                            total_count=len(numeric_series)
                        ))
            
            # Allowed values validation
            if 'allowed_values' in rules:
                allowed = set(rules['allowed_values'])
                actual = set(non_null_series.unique())
                invalid_values = actual - allowed
                
                if invalid_values:
                    invalid_count = non_null_series.isin(invalid_values).sum()
                    results.append(ValidationResult(
                        field=column,
                        severity=ValidationSeverity.ERROR,
                        message=f"Column {column} contains invalid values: {list(invalid_values)}",
                        failed_count=invalid_count,
                        total_count=len(non_null_series),
                        failed_values=list(invalid_values)
                    ))
        
        return results
    
    def _validate_business_rules(self, df: pd.DataFrame) -> List[ValidationResult]:
        """Validate business-specific rules"""
        results = []
        
        # Timestamp business rules
        if 'timestamp' in df.columns:
            timestamp_series = pd.to_datetime(df['timestamp'], errors='coerce')
            
            # Not future
            future_count = (timestamp_series > datetime.now()).sum()
            if future_count > 0:
                results.append(ValidationResult(
                    field='timestamp',
                    severity=ValidationSeverity.ERROR,
                    message=f"Found {future_count} future timestamps",
                    failed_count=future_count,
                    total_count=len(timestamp_series)
                ))
            
            # Not too old (older than 1 year)
            cutoff_date = datetime.now() - timedelta(days=365)
            too_old_count = (timestamp_series < cutoff_date).sum()
            if too_old_count > 0:
                results.append(ValidationResult(
                    field='timestamp',
                    severity=ValidationSeverity.WARNING,
                    message=f"Found {too_old_count} timestamps older than 1 year",
                    failed_count=too_old_count,
                    total_count=len(timestamp_series)
                ))
        
        # Sensor-specific value validation
        if all(col in df.columns for col in ['sensor_type', 'value', 'unit']):
            for sensor_type, ranges in self.sensor_ranges.items():
                sensor_mask = df['sensor_type'] == sensor_type
                sensor_data = df[sensor_mask]
                
                if not sensor_data.empty:
                    # Value range validation
                    out_of_range = (
                        (sensor_data['value'] < ranges['min']) |
                        (sensor_data['value'] > ranges['max'])
                    ).sum()
                    
                    if out_of_range > 0:
                        results.append(ValidationResult(
                            field=f'{sensor_type}_value_range',
                            severity=ValidationSeverity.WARNING,
                            message=f"{sensor_type} sensors have {out_of_range} values outside expected range {ranges['min']}-{ranges['max']}",
                            failed_count=out_of_range,
                            total_count=len(sensor_data)
                        ))
                    
                    # Unit consistency validation
                    expected_units = set(ranges['unit'])
                    actual_units = set(sensor_data['unit'].unique())
                    invalid_units = actual_units - expected_units
                    
                    if invalid_units:
                        invalid_count = sensor_data['unit'].isin(invalid_units).sum()
                        results.append(ValidationResult(
                            field=f'{sensor_type}_unit_consistency',
                            severity=ValidationSeverity.ERROR,
                            message=f"{sensor_type} sensors have invalid units: {list(invalid_units)}",
                            failed_count=invalid_count,
                            total_count=len(sensor_data)
                        ))
        
        return results
    
    def _validate_data_quality(self, df: pd.DataFrame) -> List[ValidationResult]:
        """Validate overall data quality metrics"""
        results = []
        
        # Completeness check
        total_cells = df.size
        null_cells = df.isnull().sum().sum()
        completeness_rate = 1 - (null_cells / total_cells)
        
        if completeness_rate < 0.95:
            results.append(ValidationResult(
                field='completeness',
                severity=ValidationSeverity.WARNING if completeness_rate > 0.9 else ValidationSeverity.ERROR,
                message=f"Dataset completeness is {completeness_rate:.2%} (below 95% threshold)",
                failed_count=null_cells,
                total_count=total_cells
            ))
        
        # Duplicate detection
        if 'sensor_id' in df.columns and 'timestamp' in df.columns:
            duplicate_count = df.duplicated(subset=['sensor_id', 'timestamp']).sum()
            if duplicate_count > 0:
                results.append(ValidationResult(
                    field='duplicates',
                    severity=ValidationSeverity.WARNING,
                    message=f"Found {duplicate_count} duplicate records based on sensor_id and timestamp",
                    failed_count=duplicate_count,
                    total_count=len(df)
                ))
        
        # Data freshness check
        if 'timestamp' in df.columns:
            latest_timestamp = pd.to_datetime(df['timestamp']).max()
            hours_old = (datetime.now() - latest_timestamp).total_seconds() / 3600
            
            if hours_old > 24:
                results.append(ValidationResult(
                    field='data_freshness',
                    severity=ValidationSeverity.WARNING,
                    message=f"Latest data is {hours_old:.1f} hours old",
                    failed_count=1,
                    total_count=1
                ))
        
        return results
    
    def _validate_cross_field_consistency(self, df: pd.DataFrame) -> List[ValidationResult]:
        """Validate consistency between related fields"""
        results = []
        
        # Sensor type and unit consistency
        if all(col in df.columns for col in ['sensor_type', 'unit']):
            for sensor_type in df['sensor_type'].unique():
                sensor_data = df[df['sensor_type'] == sensor_type]
                units = sensor_data['unit'].unique()
                
                if len(units) > 1 and sensor_type in self.sensor_ranges:
                    expected_units = self.sensor_ranges[sensor_type]['unit']
                    unexpected_units = [u for u in units if u not in expected_units]
                    
                    if unexpected_units:
                        results.append(ValidationResult(
                            field=f'{sensor_type}_unit_consistency',
                            severity=ValidationSeverity.WARNING,
                            message=f"{sensor_type} sensors have inconsistent units: {list(units)}",
                            failed_count=len(unexpected_units),
                            total_count=len(units)
                        ))
        
        # Quality score and anomaly flag consistency
        if all(col in df.columns for col in ['quality_score', 'anomaly_flag']):
            # Low quality should correlate with anomalies
            low_quality_high_anomaly = df[
                (df['quality_score'] < 0.7) & (df['anomaly_flag'] == 0)
            ]
            
            if len(low_quality_high_anomaly) > len(df) * 0.1:  # More than 10%
                results.append(ValidationResult(
                    field='quality_anomaly_consistency',
                    severity=ValidationSeverity.INFO,
                    message=f"Found {len(low_quality_high_anomaly)} records with low quality but no anomaly flag",
                    failed_count=len(low_quality_high_anomaly),
                    total_count=len(df)
                ))
        
        return results
    
    def _validate_statistical_properties(self, df: pd.DataFrame) -> List[ValidationResult]:
        """Validate statistical properties of the data"""
        results = []
        
        # Check for statistical outliers in numeric columns
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        
        for column in numeric_columns:
            if column in ['quality_score', 'anomaly_flag']:  # Skip these special columns
                continue
                
            series = df[column].dropna()
            if len(series) > 10:  # Need sufficient data for statistics
                
                # Z-score based outlier detection
                z_scores = np.abs((series - series.mean()) / series.std())
                outliers = (z_scores > 3).sum()
                
                if outliers > len(series) * 0.05:  # More than 5% outliers
                    results.append(ValidationResult(
                        field=f'{column}_statistical_outliers',
                        severity=ValidationSeverity.INFO,
                        message=f"Column {column} has {outliers} statistical outliers (Z-score > 3)",
                        failed_count=outliers,
                        total_count=len(series)
                    ))
                
                # Check for constant values (no variance)
                if series.std() == 0:
                    results.append(ValidationResult(
                        field=f'{column}_variance',
                        severity=ValidationSeverity.WARNING,
                        message=f"Column {column} has no variance (all values are identical)",
                        failed_count=0,
                        total_count=len(series)
                    ))
        
        return results
    
    def _calculate_overall_score(self, results: List[ValidationResult]) -> float:
        """Calculate overall data quality score (0-1)"""
        if not results:
            return 1.0
        
        total_score = 0.0
        total_weight = 0.0
        
        # Weight by severity
        severity_weights = {
            ValidationSeverity.INFO: 0.1,
            ValidationSeverity.WARNING: 0.5,
            ValidationSeverity.ERROR: 1.0,
            ValidationSeverity.CRITICAL: 2.0
        }
        
        for result in results:
            weight = severity_weights[result.severity]
            # Score is 1 - failure_rate, weighted by severity
            score = max(0, 1 - result.failure_rate) * weight
            total_score += score
            total_weight += weight
        
        return total_score / total_weight if total_weight > 0 else 1.0
    
    def _determine_status(self, overall_score: float, results: List[ValidationResult]) -> str:
        """Determine overall validation status"""
        has_critical = any(r.severity == ValidationSeverity.CRITICAL for r in results)
        has_error = any(r.severity == ValidationSeverity.ERROR for r in results)
        
        if has_critical or overall_score < 0.5:
            return "FAILED"
        elif has_error or overall_score < 0.8:
            return "WARNING"
        else:
            return "PASSED"
    
    def _generate_recommendations(self, results: List[ValidationResult]) -> List[str]:
        """Generate actionable recommendations based on validation results"""
        recommendations = []
        
        # Group results by issue type
        schema_issues = [r for r in results if 'schema' in r.field]
        quality_issues = [r for r in results if r.field in ['completeness', 'duplicates', 'data_freshness']]
        business_issues = [r for r in results if 'range' in r.field or 'consistency' in r.field]
        
        if schema_issues:
            recommendations.append("Review data ingestion pipeline to ensure schema compliance")
        
        if quality_issues:
            recommendations.append("Implement data quality monitoring and alerting")
            recommendations.append("Add data deduplication logic to ingestion pipeline")
        
        if business_issues:
            recommendations.append("Validate sensor calibration and measurement ranges")
            recommendations.append("Review business logic for data transformation")
        
        # High failure rate recommendations
        high_failure_results = [r for r in results if r.failure_rate > 0.1]
        if high_failure_results:
            recommendations.append("Investigate root cause for high failure rates in: " + 
                                  ", ".join([r.field for r in high_failure_results]))
        
        return recommendations
    
    def _generate_summary(self, results: List[ValidationResult]) -> Dict[str, int]:
        """Generate summary statistics of validation results"""
        return {
            'total_validations': len(results),
            'critical_issues': sum(1 for r in results if r.severity == ValidationSeverity.CRITICAL),
            'errors': sum(1 for r in results if r.severity == ValidationSeverity.ERROR),
            'warnings': sum(1 for r in results if r.severity == ValidationSeverity.WARNING),
            'info': sum(1 for r in results if r.severity == ValidationSeverity.INFO)
        }
    
    def _generate_quality_breakdown(self, df: pd.DataFrame, results: List[ValidationResult]) -> Dict[str, float]:
        """Generate detailed quality breakdown by category"""
        return {
            'schema_compliance': self._calculate_category_score(results, 'schema'),
            'business_rules_compliance': self._calculate_category_score(results, 'range|consistency'),
            'data_completeness': 1 - (df.isnull().sum().sum() / df.size),
            'data_freshness': self._calculate_freshness_score(df),
            'statistical_quality': self._calculate_category_score(results, 'statistical|variance')
        }
    
    def _calculate_category_score(self, results: List[ValidationResult], category_pattern: str) -> float:
        """Calculate score for a specific category of validations"""
        import re
        category_results = [r for r in results if re.search(category_pattern, r.field)]
        
        if not category_results:
            return 1.0
        
        return sum(max(0, 1 - r.failure_rate) for r in category_results) / len(category_results)
    
    def _calculate_freshness_score(self, df: pd.DataFrame) -> float:
        """Calculate data freshness score"""
        if 'timestamp' not in df.columns:
            return 1.0
        
        try:
            latest_timestamp = pd.to_datetime(df['timestamp']).max()
            hours_old = (datetime.now() - latest_timestamp).total_seconds() / 3600
            
            # Score decreases with age: 1.0 for <1hr, 0.5 for 24hr, 0.0 for >48hr
            if hours_old <= 1:
                return 1.0
            elif hours_old <= 24:
                return 1.0 - (hours_old - 1) / 23 * 0.5
            elif hours_old <= 48:
                return 0.5 - (hours_old - 24) / 24 * 0.5
            else:
                return 0.0
        except:
            return 0.5  # Default score if calculation fails
    
    def _log_validation_summary(self, report: Dict[str, Any]):
        """Log validation summary"""
        self.logger.info("=== DATA VALIDATION SUMMARY ===")
        self.logger.info(f"Overall Score: {report['overall_score']:.2f}")
        self.logger.info(f"Status: {report['status']}")
        self.logger.info(f"Records Validated: {report['dataset_info']['record_count']:,}")
        
        summary = report['summary']
        self.logger.info(f"Issues Found: {summary['critical_issues']} critical, {summary['errors']} errors, {summary['warnings']} warnings")
        
        if report['recommendations']:
            self.logger.info("Recommendations:")
            for rec in report['recommendations']:
                self.logger.info(f"  - {rec}")

def main():
    """Demo of production data validation"""
    logging.basicConfig(level=logging.INFO)
    
    # Load the data from your ingestion
    try:
        df = pd.read_csv('data/real_sensor_readings.csv')
        print(f"📊 Loaded {len(df)} records for validation")
        
        # Run validation
        validator = ProductionDataValidator()
        report = validator.validate_dataset(df)
        
        print(f"\n🎯 Validation Results:")
        print(f"Overall Score: {report['overall_score']:.2f}")
        print(f"Status: {report['status']}")
        
        print(f"\n📋 Issue Summary:")
        summary = report['summary']
        print(f"  Critical: {summary['critical_issues']}")
        print(f"  Errors: {summary['errors']}")
        print(f"  Warnings: {summary['warnings']}")
        print(f"  Info: {summary['info']}")
        
        print(f"\n💡 Top Recommendations:")
        for i, rec in enumerate(report['recommendations'][:3], 1):
            print(f"  {i}. {rec}")
        
        # Save detailed report
        import json
        with open('data/validation_report.json', 'w') as f:
            json.dump(report, f, indent=2, default=str)
        print(f"\n💾 Detailed report saved to data/validation_report.json")
        
    except FileNotFoundError:
        print("❌ No data file found. Run the ingestion first:")
        print("python src/real_data_ingestion.py")

if __name__ == "__main__":
    main()