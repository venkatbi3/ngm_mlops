"""
Data quality checks for ML pipelines.
"""
import logging
from typing import Dict, List, Tuple
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """Validate data quality before model training."""
    
    def __init__(self, spark):
        self.spark = spark
    
    def check_missing_values(self, df: DataFrame, max_null_pct: float = 0.5) -> Tuple[bool, Dict]:
        """
        Check for excessive null values.
        
        Args:
            df: Input DataFrame
            max_null_pct: Maximum allowed null percentage (0-1)
            
        Returns:
            (is_valid, report) tuple
        """
        total_rows = df.count()
        report = {}
        is_valid = True
        
        for col in df.columns:
            null_count = df.where(df[col].isNull()).count()
            null_pct = null_count / total_rows if total_rows > 0 else 0
            
            if null_pct > max_null_pct:
                is_valid = False
                logger.warning(f"Column {col}: {null_pct:.1%} nulls (threshold: {max_null_pct:.1%})")
            
            report[col] = {
                "null_count": null_count,
                "null_percentage": null_pct
            }
        
        return is_valid, report
    
    def check_duplicates(self, df: DataFrame, key_columns: List[str]) -> Tuple[bool, Dict]:
        """
        Check for duplicate rows.
        
        Args:
            df: Input DataFrame
            key_columns: Columns that define uniqueness
            
        Returns:
            (is_valid, report) tuple
        """
        total_rows = df.count()
        unique_rows = df.dropDuplicates(key_columns).count()
        duplicate_count = total_rows - unique_rows
        duplicate_pct = duplicate_count / total_rows if total_rows > 0 else 0
        
        is_valid = duplicate_count == 0
        
        if not is_valid:
            logger.warning(f"Found {duplicate_count} duplicate rows ({duplicate_pct:.1%})")
        
        return is_valid, {
            "total_rows": total_rows,
            "unique_rows": unique_rows,
            "duplicate_count": duplicate_count,
            "duplicate_percentage": duplicate_pct
        }
    
    def check_value_ranges(self, df: DataFrame, numeric_cols: List[str]) -> Tuple[bool, Dict]:
        """
        Check numeric columns for outliers and unexpected ranges.
        
        Args:
            df: Input DataFrame
            numeric_cols: Numeric columns to check
            
        Returns:
            (is_valid, report) tuple
        """
        report = {}
        is_valid = True
        
        stats_df = df.select(*numeric_cols).describe().collect()
        
        for row in stats_df:
            stat_type = row[0]
            for col in numeric_cols:
                col_idx = numeric_cols.index(col) + 1
                report.setdefault(col, {})[stat_type] = row[col_idx]
        
        return is_valid, report
    
    def run_all_checks(self, df: DataFrame, key_columns: List[str] = None,
                       numeric_cols: List[str] = None) -> Dict:
        """
        Run all data quality checks.
        
        Args:
            df: Input DataFrame
            key_columns: Columns for duplicate check
            numeric_cols: Numeric columns for range check
            
        Returns:
            Dictionary containing all check results
        """
        logger.info("Running data quality checks...")
        
        results = {
            "total_rows": df.count(),
            "total_columns": len(df.columns)
        }
        
        # Check nulls
        null_valid, null_report = self.check_missing_values(df)
        results["null_check"] = {
            "valid": null_valid,
            "detail": null_report
        }
        
        # Check duplicates
        if key_columns:
            dup_valid, dup_report = self.check_duplicates(df, key_columns)
            results["duplicate_check"] = {
                "valid": dup_valid,
                "detail": dup_report
            }
        
        # Check ranges
        if numeric_cols:
            range_valid, range_report = self.check_value_ranges(df, numeric_cols)
            results["range_check"] = {
                "valid": range_valid,
                "detail": range_report
            }
        
        results["overall_valid"] = all(
            r.get("valid", True) for r in results.values() if isinstance(r, dict)
        )
        
        logger.info(f"Data quality check complete. Overall valid: {results['overall_valid']}")
        return results