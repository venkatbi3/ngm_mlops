from scipy.stats import ks_2samp

def ks_drift(baseline, current, p_threshold=0.05):
    _, p = ks_2samp(baseline, current)
    return p < p_threshold
