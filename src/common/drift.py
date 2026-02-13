"""
Data and model drift detection utilities.
Implements statistical tests for drift detection.
"""
import logging
from typing import Tuple, Dict
from scipy.stats import ks_2samp, chi2_contingency
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def ks_drift(baseline: np.ndarray, current: np.ndarray, p_threshold: float = 0.05) -> bool:
    """
    Kolmogorov-Smirnov test for numerical drift.
    
    Args:
        baseline: Baseline distribution values
        current: Current distribution values
        p_threshold: P-value threshold for drift detection
        
    Returns:
        True if drift detected, False otherwise
    """
    if len(baseline) < 2 or len(current) < 2:
        logger.warning("Insufficient samples for KS test")
        return False
    
    statistic, p_value = ks_2samp(baseline, current)
    drift_detected = p_value < p_threshold
    
    if drift_detected:
        logger.warning(f"KS Drift detected: statistic={statistic:.4f}, p-value={p_value:.6f}")
    else:
        logger.info(f"No KS drift: statistic={statistic:.4f}, p-value={p_value:.6f}")
    
    return drift_detected


def chi_square_drift(baseline: pd.Series, current: pd.Series, p_threshold: float = 0.05) -> bool:
    """
    Chi-square test for categorical drift.
    
    Args:
        baseline: Baseline categorical distribution
        current: Current categorical distribution
        p_threshold: P-value threshold for drift detection
        
    Returns:
        True if drift detected, False otherwise
    """
    try:
        # Create contingency table
        all_categories = set(baseline.unique()) | set(current.unique())
        baseline_counts = baseline.value_counts().reindex(all_categories, fill_value=0)
        current_counts = current.value_counts().reindex(all_categories, fill_value=0)
        
        contingency_table = np.array([baseline_counts, current_counts])
        chi2, p_value, dof, expected = chi2_contingency(contingency_table)
        
        drift_detected = p_value < p_threshold
        
        if drift_detected:
            logger.warning(f"Chi-square drift detected: chi2={chi2:.4f}, p-value={p_value:.6f}")
        else:
            logger.info(f"No categorical drift: chi2={chi2:.4f}, p-value={p_value:.6f}")
        
        return drift_detected
    except Exception as e:
        logger.error(f"Chi-square test failed: {e}")
        return False


def jsd_distance(p: np.ndarray, q: np.ndarray) -> float:
    """
    Jensen-Shannon divergence between two distributions.
    Symmetric alternative to KL divergence.
    
    Args:
        p: First probability distribution
        q: Second probability distribution
        
    Returns:
        JSD distance (0-1)
    """
    p = np.array(p) / np.sum(p)
    q = np.array(q) / np.sum(q)
    
    m = 0.5 * (p + q)
    
    kl_p_m = np.sum(p * np.log(p / m + 1e-10))
    kl_q_m = np.sum(q * np.log(q / m + 1e-10))
    
    return np.sqrt(0.5 * (kl_p_m + kl_q_m))


def population_stability_index(baseline: pd.Series, current: pd.Series) -> float:

    """
    Population Stability Index (PSI) for numerical features.
    Measures shift in feature distribution.
    
    Args:
        baseline: Baseline feature values
        current: Current feature values
        
    Returns:
        PSI score (typically 0-0.25 is stable)
    """
    def calculate_psi(expected, actual, buckets=10):
        try:
            breakpoints = np.percentile(expected, np.linspace(0, 100, buckets + 1))
            breakpoints[0] = breakpoints[0] - 0.1
            breakpoints[-1] = breakpoints[-1] + 0.1
            
            expected_percents = np.histogram(expected, breakpoints)[0] / len(expected)
            actual_percents = np.histogram(actual, breakpoints)[0] / len(actual)
            
            psi = np.sum((actual_percents - expected_percents) * 
                        np.log((actual_percents + 0.0001) / (expected_percents + 0.0001)))
            
            return psi
        except Exception as e:
            logger.error(f"PSI calculation failed: {e}")
            return 0
    
    psi = calculate_psi(baseline.values, current.values)
    
    if psi > 0.25:
        logger.warning(f"High PSI detected: {psi:.4f}")
    else:
        logger.info(f"PSI: {psi:.4f}")
    
    return psi


class DriftDetector:
    """Multi-feature drift detector."""
    
    def __init__(self, p_threshold: float = 0.05):
        self.p_threshold = p_threshold
        self.numeric_features = []
        self.categorical_features = []
    
    def detect_drift(self, baseline: pd.DataFrame, current: pd.DataFrame) -> Dict:
        """
        Detect feature drift across all columns.
        
        Args:
            baseline: Baseline DataFrame
            current: Current DataFrame
            
        Returns:
            Dictionary with drift results per feature
        """
        results = {}
        
        for col in baseline.columns:
            if col not in current.columns:
                logger.warning(f"Column {col} missing in current dataset")
                continue
            
            if baseline[col].dtype in ['int64', 'float64']:
                # Numerical feature
                drift = ks_drift(
                    baseline[col].dropna().values,
                    current[col].dropna().values,
                    self.p_threshold
                )
                psi = population_stability_index(baseline[col], current[col])
                
                results[col] = {
                    "type": "numerical",
                    "ks_drift": drift,
                    "psi": psi,
                    "drift_detected": drift or psi > 0.25
                }
            else:
                # Categorical feature
                drift = chi_square_drift(
                    baseline[col],
                    current[col],
                    self.p_threshold
                )
                
                results[col] = {
                    "type": "categorical",
                    "chi2_drift": drift,
                    "drift_detected": drift
                }
        
        results["overall_drift"] = any(r.get("drift_detected") for r in results.values() 
                                      if isinstance(r, dict))
        
        return results
