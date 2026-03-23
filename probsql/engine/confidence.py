"""
Confidence Calibration — Calibrates raw confidence scores so that
"confidence 0.8" means "correct ~80% of the time."

Uses isotonic regression (piecewise linear monotonic fit) implemented from scratch.
"""

import json
from pathlib import Path

KNOWLEDGE_DIR = Path(__file__).parent.parent / "knowledge" / "base"


class ConfidenceCalibrator:
    def __init__(self):
        self.calibration_table = []  # [(raw_threshold, calibrated_value), ...]
        self.num_bins = 20

    def load(self, knowledge_dir=None):
        """Load calibration table from JSON."""
        kdir = Path(knowledge_dir) if knowledge_dir else KNOWLEDGE_DIR
        path = kdir / "calibration_table.json"
        if path.exists():
            with open(path) as f:
                data = json.load(f)
                self.calibration_table = [
                    (entry["raw"], entry["calibrated"])
                    for entry in data.get("bins", [])
                ]

    def calibrate(self, raw_score):
        """Map raw confidence score to calibrated score.

        Args:
            raw_score: float 0.0-1.0

        Returns:
            float: calibrated confidence 0.0-1.0
        """
        if not self.calibration_table:
            return raw_score  # no calibration data, pass through

        # Find the bin
        for i, (threshold, cal_val) in enumerate(self.calibration_table):
            if raw_score <= threshold:
                if i == 0:
                    return cal_val
                # Linear interpolation between bins
                prev_thresh, prev_val = self.calibration_table[i - 1]
                if threshold == prev_thresh:
                    return cal_val
                ratio = (raw_score - prev_thresh) / (threshold - prev_thresh)
                return prev_val + ratio * (cal_val - prev_val)

        # Above all thresholds
        if self.calibration_table:
            return self.calibration_table[-1][1]
        return raw_score

    def fit(self, predictions):
        """Fit calibration from (raw_confidence, is_correct) pairs.

        Args:
            predictions: list of (raw_confidence, is_correct) tuples

        Returns:
            list of calibration bins
        """
        if not predictions:
            return []

        # Sort by raw confidence
        sorted_preds = sorted(predictions, key=lambda x: x[0])

        # Bin into equal-sized bins
        n = len(sorted_preds)
        bin_size = max(1, n // self.num_bins)
        bins = []

        for i in range(0, n, bin_size):
            bin_items = sorted_preds[i:i + bin_size]
            if not bin_items:
                continue
            avg_raw = sum(p[0] for p in bin_items) / len(bin_items)
            accuracy = sum(1 for p in bin_items if p[1]) / len(bin_items)
            max_raw = max(p[0] for p in bin_items)
            bins.append({
                "raw": max_raw,
                "calibrated": accuracy,
                "count": len(bin_items),
                "avg_raw": avg_raw,
            })

        # Apply isotonic regression (pool adjacent violators)
        calibrated_values = [b["calibrated"] for b in bins]
        isotonic = self._isotonic_regression(calibrated_values)
        for i, b in enumerate(bins):
            b["calibrated"] = isotonic[i]

        self.calibration_table = [(b["raw"], b["calibrated"]) for b in bins]
        return bins

    def _isotonic_regression(self, values):
        """Pool Adjacent Violators algorithm for isotonic regression."""
        n = len(values)
        if n == 0:
            return []

        # Make a copy
        result = list(values)
        weights = [1.0] * n

        # Pool adjacent violators
        changed = True
        while changed:
            changed = False
            i = 0
            while i < len(result) - 1:
                if result[i] > result[i + 1]:
                    # Pool: replace with weighted average
                    total_weight = weights[i] + weights[i + 1]
                    pooled = (result[i] * weights[i] + result[i + 1] * weights[i + 1]) / total_weight
                    result[i] = pooled
                    result[i + 1] = pooled
                    weights[i] = total_weight
                    weights[i + 1] = total_weight
                    changed = True
                i += 1

        return result

    def save(self, output_dir=None):
        """Save calibration table to JSON."""
        kdir = Path(output_dir) if output_dir else KNOWLEDGE_DIR
        kdir.mkdir(parents=True, exist_ok=True)
        path = kdir / "calibration_table.json"
        bins = [{"raw": r, "calibrated": c} for r, c in self.calibration_table]
        with open(path, "w") as f:
            json.dump({"bins": bins, "num_bins": self.num_bins}, f, indent=2)
