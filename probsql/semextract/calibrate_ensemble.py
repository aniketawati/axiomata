"""
Ensemble Confidence Calibration — Fits isotonic regression per path
so that P(correct | calibrated_confidence) is well-calibrated.

After calibration, confidence 0.5 from any path means "correct 50% of the time."
The ensemble can then compare calibrated scores directly.
"""

import json
from pathlib import Path

KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"


def isotonic_regression(values):
    """Pool Adjacent Violators algorithm for isotonic (monotonic) regression."""
    n = len(values)
    if n == 0:
        return []
    result = list(values)
    weights = [1.0] * n

    changed = True
    while changed:
        changed = False
        i = 0
        while i < len(result) - 1:
            if result[i] > result[i + 1]:
                total_w = weights[i] + weights[i + 1]
                pooled = (result[i] * weights[i] + result[i + 1] * weights[i + 1]) / total_w
                result[i] = pooled
                result[i + 1] = pooled
                weights[i] = total_w
                weights[i + 1] = total_w
                changed = True
            i += 1
    return result


def fit_calibration(data, n_bins=20):
    """Fit isotonic calibration from (confidence, correct) pairs.

    Returns list of (threshold, calibrated_accuracy) bins.
    """
    if not data:
        return []

    # Sort by confidence
    sorted_data = sorted(data, key=lambda x: x[0])

    # Bin
    bin_size = max(1, len(sorted_data) // n_bins)
    bins = []
    for i in range(0, len(sorted_data), bin_size):
        chunk = sorted_data[i:i + bin_size]
        if not chunk:
            continue
        max_conf = max(c for c, _ in chunk)
        accuracy = sum(1 for _, correct in chunk if correct) / len(chunk)
        bins.append({
            "threshold": max_conf,
            "accuracy": accuracy,
            "count": len(chunk),
            "avg_raw_conf": sum(c for c, _ in chunk) / len(chunk),
        })

    # Apply isotonic regression to accuracies
    accuracies = [b["accuracy"] for b in bins]
    calibrated = isotonic_regression(accuracies)
    for i, b in enumerate(bins):
        b["calibrated"] = calibrated[i]

    return bins


def calibrate_score(raw_confidence, calibration_bins):
    """Map a raw confidence to calibrated confidence using the fitted bins."""
    if not calibration_bins:
        return raw_confidence

    for i, b in enumerate(calibration_bins):
        if raw_confidence <= b["threshold"]:
            if i == 0:
                return b["calibrated"]
            # Linear interpolation
            prev = calibration_bins[i - 1]
            if b["threshold"] == prev["threshold"]:
                return b["calibrated"]
            ratio = (raw_confidence - prev["threshold"]) / (b["threshold"] - prev["threshold"])
            return prev["calibrated"] + ratio * (b["calibrated"] - prev["calibrated"])

    return calibration_bins[-1]["calibrated"]


def build_calibration():
    """Build calibration tables from collected data."""
    KNOWLEDGE_DIR.mkdir(parents=True, exist_ok=True)

    data_path = KNOWLEDGE_DIR / "calibration_data.json"
    if not data_path.exists():
        print("No calibration data. Run data collection first.")
        return

    with open(data_path) as f:
        cal_data = json.load(f)

    calibrations = {}
    for path_name in ["old_path", "semextract", "probprog"]:
        data = cal_data.get(path_name, [])
        data = [(c, bool(correct)) for c, correct in data]

        if not data:
            continue

        bins = fit_calibration(data, n_bins=15)
        calibrations[path_name] = bins

        n = len(data)
        accuracy = sum(1 for _, c in data if c) / n
        confs = [c for c, _ in data]

        print(f"\n{path_name} (n={n}, accuracy={accuracy:.1%}):")
        print(f"  Raw conf range: [{min(confs):.3f}, {max(confs):.3f}]")
        print(f"  Calibration bins:")
        for b in bins:
            print(f"    raw≤{b['threshold']:.3f} → calibrated={b['calibrated']:.3f} "
                  f"(actual_acc={b['accuracy']:.3f}, n={b['count']})")

    with open(KNOWLEDGE_DIR / "ensemble_calibration.json", "w") as f:
        json.dump(calibrations, f, indent=2)

    print(f"\nCalibration saved to {KNOWLEDGE_DIR / 'ensemble_calibration.json'}")
    return calibrations


if __name__ == "__main__":
    build_calibration()
