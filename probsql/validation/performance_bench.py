"""
Performance Benchmark — Measures runtime performance of the ProbSQL engine.

Metrics:
1. Latency: p50, p90, p99 over 1000 queries
2. Memory: RSS after loading knowledge
3. Throughput: queries per second
4. Startup time: time to load knowledge files
5. Artifact size: total size of JSON knowledge files

Usage: python phase4_validation/performance_bench.py
"""

import json
import os
import random
import resource
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from probsql.engine.engine import ProbSQLEngine

REPORTS_DIR = Path(__file__).parent / "reports"
KNOWLEDGE_DIR = Path(__file__).parent.parent / "knowledge" / "base"

DEMO_SCHEMA = {
    "tables": [
        {
            "name": "users",
            "columns": [
                {"name": "id", "type": "INT", "primary_key": True},
                {"name": "email", "type": "VARCHAR(255)"},
                {"name": "name", "type": "VARCHAR(100)"},
                {"name": "created_at", "type": "TIMESTAMP"},
                {"name": "is_active", "type": "BOOLEAN"},
                {"name": "status", "type": "VARCHAR(20)", "enum_values": ["active", "inactive", "suspended"]},
                {"name": "lifetime_value", "type": "DECIMAL(10,2)"},
                {"name": "last_login_at", "type": "TIMESTAMP"},
            ]
        },
        {
            "name": "orders",
            "columns": [
                {"name": "id", "type": "INT", "primary_key": True},
                {"name": "user_id", "type": "INT"},
                {"name": "total_amount", "type": "DECIMAL(10,2)"},
                {"name": "status", "type": "VARCHAR(20)", "enum_values": ["pending", "shipped", "delivered", "cancelled"]},
                {"name": "created_at", "type": "TIMESTAMP"},
                {"name": "shipped_at", "type": "TIMESTAMP"},
            ]
        },
        {
            "name": "products",
            "columns": [
                {"name": "id", "type": "INT", "primary_key": True},
                {"name": "name", "type": "VARCHAR(200)"},
                {"name": "price", "type": "DECIMAL(10,2)"},
                {"name": "category", "type": "VARCHAR(50)"},
                {"name": "stock_quantity", "type": "INT"},
                {"name": "is_active", "type": "BOOLEAN"},
                {"name": "rating", "type": "FLOAT"},
            ]
        },
    ]
}

TEST_PHRASES = [
    "active users",
    "orders over $100",
    "users who signed up last month",
    "cancelled orders",
    "products in stock",
    "users who haven't logged in recently",
    "expensive orders from this year",
    "verified users with high lifetime value",
    "pending orders",
    "products with low rating",
    "users from last week",
    "shipped orders",
    "inactive users",
    "products under $50",
    "orders between $50 and $200",
    "users who signed up this month and are active",
    "delivered orders from Q3 2024",
    "products that are not active",
    "recent orders",
    "users with missing email",
]


def measure_startup():
    """Measure time to create engine and load knowledge."""
    start = time.perf_counter()
    engine = ProbSQLEngine()
    if KNOWLEDGE_DIR.exists():
        engine.load_knowledge(str(KNOWLEDGE_DIR))
    elapsed = time.perf_counter() - start
    return engine, elapsed


def measure_latency(engine, n=1000):
    """Measure query latency over n queries."""
    rng = random.Random(42)
    latencies = []

    for i in range(n):
        phrase = rng.choice(TEST_PHRASES)
        start = time.perf_counter()
        engine.generate(phrase, DEMO_SCHEMA)
        elapsed = (time.perf_counter() - start) * 1000  # ms
        latencies.append(elapsed)

    latencies.sort()
    return {
        "count": n,
        "p50_ms": latencies[n // 2],
        "p90_ms": latencies[int(n * 0.9)],
        "p99_ms": latencies[int(n * 0.99)],
        "min_ms": latencies[0],
        "max_ms": latencies[-1],
        "mean_ms": sum(latencies) / n,
    }


def measure_memory():
    """Measure resident set size."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    rss_kb = usage.ru_maxrss
    # On macOS, ru_maxrss is in bytes; on Linux, it's in KB
    if sys.platform == "darwin":
        rss_mb = rss_kb / (1024 * 1024)
    else:
        rss_mb = rss_kb / 1024
    return rss_mb


def measure_throughput(engine, duration_sec=5):
    """Measure queries per second in a tight loop."""
    rng = random.Random(42)
    count = 0
    start = time.perf_counter()
    while time.perf_counter() - start < duration_sec:
        phrase = rng.choice(TEST_PHRASES)
        engine.generate(phrase, DEMO_SCHEMA)
        count += 1
    elapsed = time.perf_counter() - start
    return count / elapsed


def measure_artifact_size():
    """Measure total size of knowledge files."""
    total = 0
    if KNOWLEDGE_DIR.exists():
        for f in KNOWLEDGE_DIR.glob("*.json"):
            total += f.stat().st_size
    return total / (1024 * 1024)  # MB


def run_benchmark():
    """Run all benchmarks."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    print("Performance Benchmark")
    print("=" * 60)

    # Startup
    engine, startup_time = measure_startup()
    print(f"Startup time: {startup_time:.3f}s (target: <2s)")

    # Latency
    latency = measure_latency(engine, 1000)
    print(f"\nLatency (1000 queries):")
    print(f"  p50:  {latency['p50_ms']:.2f}ms")
    print(f"  p90:  {latency['p90_ms']:.2f}ms")
    print(f"  p99:  {latency['p99_ms']:.2f}ms (target: <10ms)")
    print(f"  mean: {latency['mean_ms']:.2f}ms")

    # Memory
    memory_mb = measure_memory()
    print(f"\nMemory (RSS): {memory_mb:.1f}MB (target: <50MB)")

    # Throughput
    qps = measure_throughput(engine, 5)
    print(f"\nThroughput: {qps:.0f} qps (target: >500)")

    # Artifact size
    artifact_mb = measure_artifact_size()
    print(f"\nArtifact size: {artifact_mb:.2f}MB (target: <30MB)")

    # Summary
    results = {
        "startup_time_sec": startup_time,
        "latency": latency,
        "memory_mb": memory_mb,
        "throughput_qps": qps,
        "artifact_size_mb": artifact_mb,
        "targets_met": {
            "startup_under_2s": startup_time < 2.0,
            "p99_under_10ms": latency["p99_ms"] < 10.0,
            "memory_under_50mb": memory_mb < 50.0,
            "throughput_over_500": qps > 500,
            "artifact_under_30mb": artifact_mb < 30.0,
        },
    }

    report_path = REPORTS_DIR / "performance_report.json"
    with open(report_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n{'=' * 60}")
    all_met = all(results["targets_met"].values())
    for target, met in results["targets_met"].items():
        status = "PASS" if met else "FAIL"
        print(f"  [{status}] {target}")
    print(f"\n{'ALL TARGETS MET' if all_met else 'SOME TARGETS MISSED'}")


if __name__ == "__main__":
    run_benchmark()
