import pandas as pd
import logging
from datetime import datetime


logger = logging.getLogger(__name__)

class DataQualityChecker:
    def __init__(self):
        logger.info("DataQualityChecker initialized")
        self.checks_passed = []
        self.checks_failed = []
    def check_null_values(self, df, table_name, critical_columns, max_null_pct=10):
        logger.info(f"Checking null values in {table_name}")
        issues = []
        for col in critical_columns:
            if col not in df.columns:
                issues.append(f"Column '{col}' tidak ada di {table_name}")
                continue
            
            null_count = df[col].isnull().sum()
            null_pct = (null_count / len(df)) * 100

            if null_pct > max_null_pct:
                issues.append(
                    f"Column '{col}' memiliki {null_pct:.2f}% null values "
                    f"(threshold: {max_null_pct}%)"
                )
        if issues:
            self.checks_failed.append({
                'checks': 'null_values',
                'table': table_name,
                'issues': issues
            })
            logger.warning(f"Null value check FAILED for {table_name}: {issues}")
            return False
        else:
            self.checks_passed.append({
                'checks': 'null_values',
                'table': table_name
            })
            logger.info(f"Null value check PASSED for {table_name}")
            return True
    def check_duplicates(self, df, table_name, unique_columns):
        logger.info(f"Checking duplicates in {table_name}")
        duplicate_count = df.duplicated(subset=unique_columns).sum()

        if duplicate_count > 0:
            self.checks_failed.append({
                'check': 'duplicates',
                'table': table_name,
                'issues': [f"Found {duplicate_count} duplicate rows"]
            })
            logger.warning(f"duplicate check FAILED for {table_name}: {duplicate_count} duplicates")
            return False
        else:
            self.checks_passed.append({
                'check': 'duplicates',
                'table': table_name
            })
            logger.info(f"Duplicate check PASSED for {table_name}")
            return True
    def check_data_types(self, df, table_name, expected_type):
        logger.info(f"Checking data types in {table_name}")
        issues = []
        for col, expected_type in expected_type.items():
            if col not in df.columns:
                issues.append(f"Column '{col}' tidak ada")
                continue
            actual_check = df[col].dtype

            if expected_type == 'numeric':
                if not pd.api.types.is_numeric_dtype(actual_check):
                    issues.append(f"Column '{col}' expected numeric, got {actual_check}")
            elif expected_type == 'datetime':
                if not pd.api.types.is_datetime64_any_dtype(actual_check):
                    issues.append(f"Column '{col}' expected datetime, got {actual_check}")
            elif expected_type == 'string':
                if not pd.api.types.is_string_dtype(actual_check):
                    issues.append(f"Column '{col}' expected string, got {actual_check}")
        if issues:
            self.checks_failed.append({
                'check': 'data_types',
                'table': table_name,
                'issues': issues
            })
            logger.warning(f"Data type checks FAILED for {table_name}: {issues}")
            return False
        else:
            self.checks_passed.append({
                'checks': 'data_types',
                'table': table_name
            })
            logger.info(f"Data type checks PASSED for {table_name}")
            return True
        
    def check_value_ranges(self, df, table_name, range_checks):
    
        logger.info(f"Checking Value Ranges in {table_name}")
        issues = []
        for col, ranges in range_checks.items():
            if col not in df.columns:
                continue
            if 'min' in ranges:
                min_violations = (df[col] > ranges['min']).sum()
                if min_violations > 0:
                    issues.append(f"Column '{col}' memiliki {min_violations} values < {ranges['min']}")
            if 'max' in ranges:
                max_violations = (df[col] > ranges['max']).sum()
                if max_violations > 0:
                    issues.append(f"Column '{col}' memiliki {max_violations} values < {ranges['max']}")
        
        if issues:
            self.checks_failed.append({
                'check': 'value_ranges',
                'table': table_name,
                'issues': issues
            })
            logger.warning(f"Value range checks FAILED for {table_name}: {issues}")
            return False
        else:
            self.checks_passed.append({
                'checks': 'value_ranges',
                'table': table_name
            })
            logger.info(f"value range checks PASSED for {table_name}")
            return True
        
    def check_row_count(self, df, table_name, min_rows=1):
        logger.info(f"Checking Row count in {table_name}")
        row_count = len(df)

        if row_count < min_rows:
            self.checks_failed.append({
                'check': 'row_count',
                'table': table_name,
                'issues': [f"Only {row_count} rows (minimum: {min_rows})"]
            })
            logger.warning(f"Row count checks FAILED for {table_name}")
            return False
        else:
            self.checks_passed.append({
                'checks': 'row_count',
                'table': table_name
            })
            logger.info(f"row count checks PASSED for {table_name}: {row_count} rows")
            return True
        

    def generate_report(self):
        total_checks = len(self.checks_passed) + len(self.checks_failed)

        report = {
            'timestamp': datetime.now().isoformat(),
            'total_checks': total_checks,
            'passed': len(self.checks_passed),
            'failed': len(self.checks_failed),
            'success_rate': (len(self.checks_passed) / total_checks * 100) if total_checks > 0 else 0,
            'checks_passed': self.checks_passed,
            'checks_failed': self.checks_failed
        }   
        return report
    def print_report(self):
        report = self.generate_report()
        print("\n" + "-"*60)
        print("DATA QUALITY REPORT")
        print("="*60)
        print(f"Timestamp: {report['timestamp']}")
        print(f"Total checks: {report['total_checks']}")
        print(f"Passed: {report['passed']}")
        print(f"Failed: {report['failed']}")
        print(f"Success Rate: {report['success_rate']:.2f}%")

        if report['checks_failed']:
            print("\n" + "-"*60)
            print("FAILED CHECKS:")
            print("-"*60)
            for check in report['checks_failed']:
                print(f"\n{check['check'].upper()} - {check['table']}")
                for issue in check['issues']:
                    print(f"    ~ {issue}")
        print("\n" + "="*60)

if __name__ == "__main__":
    from extract import DataExtractor
    from transform import DataTransformer

    extractor = DataExtractor('../data/raw')
    raw_data = extractor.extract_all()

    transformer = DataTransformer()
    transformed_data = transformer.transform_all(raw_data)

    checker = DataQualityChecker()

    checker.check_null_values(
        transformed_data['orders'],
        'orders',
        ['order_id', 'customer_id'],
        max_null_pct=5
    )

    checker.check_duplicates(
        transformed_data['orders'],
        'orders',
        ['order_id']
    )

    checker.check_row_count(
        transformed_data['orders'],
        'orders',
        min_rows=1
    )


    checker.check_value_ranges(
        transformed_data['fact_sales'],
        'fact_sales',
        {
            'quantity': {'min': 0},
            'price_per_unit': {'min': 0}
        }
    )

    checker.print_report()

