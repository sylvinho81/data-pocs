# NYC Taxi Data Benchmark

This project benchmarks the performance of different query engines (Polars and DuckDB) with different data formats (Parquet and Vortex) for analyzing NYC taxi data.

## System Requirements

- **Python**: 3.11
- **Platform**: Linux (tested on Ubuntu/Debian-based systems)
- **Note**: Benchmark results are specific to the test environment. For reference, tests were run on a local PC with 16-core CPU and 32GB RAM.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Download the parquet files (if not already present):
```bash
python download_parquet_taxi_files.py
```

3. Generate vortex files from parquet:
```bash
python generate_vortex_from_parquet.py
```

4. (Optional) Create merged files for single-file benchmarks:
```bash
python fusion_files.py
```
This creates merged parquet and vortex files that combine all monthly files into single files. This is required for single-file mode benchmarks.

## Running the Benchmark

The benchmark can run in two modes. Use command-line arguments to choose:

### Command-Line Options

```bash
python benchmark.py [--mode {multi|single|both}] [options]
```

**Mode Options:**
- `--mode multi` - Run only multi-file mode (uses individual monthly files)
- `--mode single` - Run only single-file mode (uses merged files, requires step 4)
- `--mode both` - Run both modes (default)

**Additional Options:**
- `--parquet-dir DIR` - Directory containing parquet files (default: `ny_taxi_files`)
- `--vortex-dir DIR` - Directory containing vortex files (default: `ny_taxi_files/vortex`)
- `--taxi-type TYPE` - Type of taxi data: `yellow` or `green` (default: `yellow`)
- `--year YEAR` - Year of the data files (default: `2025`)
- `--num-runs N` - Number of runs per benchmark for averaging (default: `5`)

### Examples

**Run both modes (recommended):**
```bash
python benchmark.py
# or explicitly:
python benchmark.py --mode both
```

**Run only multi-file mode:**
```bash
python benchmark.py --mode multi
```

**Run only single-file mode (requires merged files from step 4):**
```bash
python benchmark.py --mode single
```

**Run with custom parameters:**
```bash
python benchmark.py --mode both --year 2024 --num-runs 5
```

### What Gets Generated

When running both modes:
- `BENCHMARK_MULTI.md` - Multi-file performance results
- `BENCHMARK_SINGLE.md` - Single-file performance results
- Separate histogram images for each mode

When running a single mode:
- `BENCHMARK_MULTI.md` or `BENCHMARK_SINGLE.md` (depending on mode)
- Histogram images for that mode

### Programmatic Usage

You can also use the benchmark class directly in Python:

```python
from benchmark import NYCBenchmark

# Multi-file mode
benchmark_multi = NYCBenchmark(mode="multi")
benchmark_multi.run_benchmark()

# Single-file mode (requires merged files)
benchmark_single = NYCBenchmark(mode="single")
benchmark_single.run_benchmark()
```

### What Gets Benchmarked

Each mode runs:
- 7 different analyses using 4 different method combinations:
  - Polars + Parquet
  - Polars + Vortex
  - DuckDB + Parquet
  - DuckDB + Vortex
- Each benchmark executes 5 times and reports average times with standard deviation
- **Timing includes**: End-to-end query execution time, including file I/O (reading files from disk) and query processing
- Generates detailed markdown reports with performance comparisons and histograms

## Analyses

The benchmark includes the following analyses from the Row Zero dataset:

1. **Trips by Day of Week** - Count trips grouped by day of week
2. **Payment Types** - Analyze payment types and revenue
3. **Passenger Count** - Distribution of passenger counts
4. **Rides by Month** - Monthly ride counts and congestion fee impact
5. **Airport Fee** - Percentage of rides with airport fees
6. **Rides by Location** - Top 20 pickup locations by trip count
7. **Random Access (50 Location IDs)** - Query specific rows matching 50 randomly selected location IDs (tests random access performance)

## Results

**Test Environment:**
- Python 3.11
- Linux system (Ubuntu/Debian-based)
- Benchmarks executed on a local PC (16-core CPU, 32GB RAM)
- Results are specific to this hardware and software configuration

After running the benchmark, results will be displayed in the console and saved to separate markdown files:

### Multi-File Mode (`BENCHMARK_MULTI.md`)
- Performance comparison table for all analyses using multiple files
- Fastest method identification for each analysis
- Overall performance summary
- Histogram visualizations

### Single-File Mode (`BENCHMARK_SINGLE.md`)
- Performance comparison table for all analyses using merged files
- Fastest method identification for each analysis
- Overall performance summary
- Histogram visualizations

**Note**: If merged files don't exist, only multi-file mode will run and results will be saved to `BENCHMARK_MULTI.md`.

## Important Notes

### Format Capabilities

**Parquet:**
- Has native multi-file support in both Polars (`pl.scan_parquet()`) and DuckDB (`read_parquet()`)
- Can read and optimize queries across multiple files simultaneously
- Generally more efficient for multi-file scenarios

**Vortex:**
- Requires opening files individually (no native multi-file reader in Polars)
- DuckDB has native Vortex support via `read_vortex()` extension
- Polars + Vortex uses lazy evaluation to optimize query plans, but still requires individual file access
- This is a real capability difference that will be reflected in benchmark results
- **Documentation**: [Vortex Python API - Input and Output](https://docs.vortex.dev/api/python/io)

### Understanding the Results vs. Vortex Claims

**Important Context:** [Vortex documentation](https://docs.vortex.dev/api/python/io) claims:
- 100x faster random access reads (vs. Parquet)
- 10-20x faster scans
- 5x faster write

**Why our benchmark may show different results:**

1. **Random Access vs. Sequential Scans**: Our benchmark includes **both**:
   - **Sequential scans**: Full table aggregations (GROUP BY, COUNT, SUM) that scan all data
   - **Random access test**: Queries specific rows using `WHERE PULocationID IN (...)` to test Vortex's claimed 100x random access advantage
   
   The "Random Access (50 Location IDs)" test specifically queries 50 randomly selected location IDs, simulating random access patterns where you're looking up specific records rather than scanning everything.

2. **Multi-file Operations**: Our benchmark processes **11 separate files**. Parquet has native multi-file support, while Vortex requires opening files individually. This overhead affects our results but may not reflect single-file performance.

3. **Query Type**: We're testing **aggregation queries** (GROUP BY, COUNT, SUM) that require reading all data. Vortex's scan performance claims may be for different query patterns or with different optimizations.

4. **Workload-Specific**: Our benchmark represents a **specific use case** (multi-file aggregations on NYC taxi data). Your results may vary based on:
   - Single file vs. multiple files
   - Random access patterns vs. sequential scans
   - Query complexity
   - Data characteristics

**Conclusion**: This benchmark tests **both single-file and multi-file aggregation scenarios**. The single-file mode better reflects Vortex's claimed performance advantages, while the multi-file mode shows real-world scenarios where Parquet's native multi-file support provides benefits.

The benchmark tests each format's **actual capabilities**, not artificially equalized conditions. This provides real-world performance insights for choosing between formats based on your use case.

### What Is Being Measured

The benchmark measures **end-to-end query execution time**, which includes:
- **File I/O**: Reading data files from disk (this is part of format performance)
- **Query Processing**: Data transformation, filtering, aggregations, and computations
- **Result Materialization**: Converting results to the final format

**Why include file I/O?**
- Different formats have different read performance characteristics
- Users care about total time from query start to results
- It reflects real-world usage patterns
- File I/O and query processing are often tightly integrated and hard to separate

**Note on caching**: The benchmark runs each query 5 times and reports the average. OS-level file caching may affect the first run, but subsequent runs help provide a more stable measurement.

### Benchmark Modes Explained

**Multi-File Mode**:
- Uses individual monthly files (e.g., `yellow_tripdata_2025-01.parquet`, `yellow_tripdata_2025-02.parquet`, etc.)
- Tests real-world scenarios with partitioned data
- Parquet has native multi-file support advantage
- Results saved to `BENCHMARK_MULTI.md`

**Single-File Mode**:
- Uses merged files (e.g., `yellow_tripdata_2025_merged.parquet`)
- Better reflects Vortex's claimed performance advantages
- Tests single-file scan performance
- Results saved to `BENCHMARK_SINGLE.md`

**Comparing both modes** helps you understand:
- How file organization affects performance
- When Vortex's single-file advantages apply
- When Parquet's multi-file support provides benefits

---

## References

- **Vortex Python API Documentation**: [Input and Output](https://docs.vortex.dev/api/python/io) - Official documentation for reading and writing Vortex files in Python
- **Row Zero NYC Taxi Dataset**: [Example Analyses](https://rowzero.com/datasets/nyc-taxi-data) - Source of inspiration for the benchmark analyses

---

*Note: Results will be generated in `BENCHMARK_MULTI.md` and `BENCHMARK_SINGLE.md` after running `python benchmark.py`*

