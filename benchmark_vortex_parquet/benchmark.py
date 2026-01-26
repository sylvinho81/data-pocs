import time
import statistics
import argparse
import random
from pathlib import Path
from typing import Dict, List, Tuple, Any
import polars as pl
import duckdb
import vortex as vx
import pyarrow as pa
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend


class NYCBenchmark:
    """
    Benchmark class to compare performance of different data formats and query engines
    for NYC taxi data analyses.
    
    Compares:
    - Polars with Parquet
    - Polars with Vortex
    - DuckDB with Parquet
    - DuckDB with Vortex
    
    Note: Parquet has native multi-file support (pl.scan_parquet() can read multiple files
    at once), while Vortex requires opening files individually. This is a real capability
    difference that will be reflected in the benchmark results.
    """
    
    def __init__(
        self,
        parquet_dir: str = "ny_taxi_files",
        vortex_dir: str = "ny_taxi_files/vortex",
        taxi_type: str = "yellow",
        year: int = 2025,
        num_runs: int = 5,
        mode: str = "multi"
    ):
        """
        Initialize the benchmark.
        
        Args:
            parquet_dir: Directory containing parquet files
            vortex_dir: Directory containing vortex files
            taxi_type: Type of taxi data ('yellow' or 'green')
            year: Year of the data files
            num_runs: Number of runs per benchmark for averaging
            mode: 'single' to use merged files, 'multi' to use multiple files (default: 'multi')
        """
        self.parquet_dir = Path(parquet_dir)
        self.vortex_dir = Path(vortex_dir)
        self.taxi_type = taxi_type
        self.year = year
        self.num_runs = num_runs
        self.mode = mode
        self.results = []
        self.analysis_results = {}  # Store actual analysis data for visualization
        
        if mode == "single":
            # Use merged files
            merged_parquet = self.parquet_dir / f"{taxi_type}_tripdata_{year}_merged.parquet"
            merged_vortex = self.vortex_dir / f"{taxi_type}_tripdata_{year}_merged.vortex"
            
            if not merged_parquet.exists():
                raise ValueError(f"Merged parquet file not found: {merged_parquet}. Run fusion_files.py first.")
            if not merged_vortex.exists():
                raise ValueError(f"Merged vortex file not found: {merged_vortex}. Run fusion_files.py first.")
            
            self.parquet_files = [merged_parquet]
            self.vortex_files = [merged_vortex]
        else:
            # Use multiple files
            self.parquet_files = sorted(self.parquet_dir.glob(f"{taxi_type}_tripdata_{year}-*.parquet"))
            self.vortex_files = sorted(self.vortex_dir.glob(f"{taxi_type}_tripdata_{year}-*.vortex"))
            
            if not self.parquet_files:
                raise ValueError(f"No parquet files found in {parquet_dir}")
            if not self.vortex_files:
                raise ValueError(f"No vortex files found in {vortex_dir}")
    
    def _time_function(self, func, *args, **kwargs) -> Tuple[float, Any]:
        """
        Time a function execution.
        
        Note: Times the end-to-end query execution, which includes:
        - File I/O (reading files from disk) - this is part of format performance
        - Query processing (transformations, aggregations, computations)
        - Result materialization
        
        Report generation and histogram creation are NOT included in the timing.
        
        Returns:
            Tuple of (time_in_seconds, result)
        """
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        return elapsed, result
    
    def _run_multiple_times(self, func, *args, **kwargs) -> Tuple[float, float, Any]:
        """
        Run a function multiple times and return average time, std dev, and last result.
        
        Returns:
            Tuple of (avg_time, std_dev, result)
        """
        times = []
        result = None
        for _ in range(self.num_runs):
            elapsed, result = self._time_function(func, *args, **kwargs)
            times.append(elapsed)
        return statistics.mean(times), statistics.stdev(times) if len(times) > 1 else 0.0, result
    
    # Analysis 1: Trips by Day of Week
    
    def analysis_trips_by_day_of_week_polars_parquet(self) -> pl.DataFrame:
        """Count trips by day of week using Polars with Parquet."""
        if self.mode == "single":
            # Single merged file - direct load
            return (
                pl.scan_parquet(str(self.parquet_files[0]))
                .with_columns(
                    pl.col("tpep_pickup_datetime").dt.strftime("%A").alias("day_of_week")
                )
                .group_by("day_of_week")
                .agg(pl.len().alias("trip_count"))
                .sort("day_of_week")
                .collect()
            )
        else:
            # Multi-file mode
            parquet_paths = [str(f) for f in self.parquet_files]
            return (
                pl.scan_parquet(parquet_paths)
                .with_columns(
                    pl.col("tpep_pickup_datetime").dt.strftime("%A").alias("day_of_week")
                )
                .group_by("day_of_week")
                .agg(pl.len().alias("trip_count"))
                .sort("day_of_week")
                .collect()
            )
    
    def analysis_trips_by_day_of_week_polars_vortex(self) -> pl.DataFrame:
        """Count trips by day of week using Polars with Vortex."""
        if self.mode == "single":
            # Single merged file - use to_polars() which returns a LazyFrame directly
            return (
                vx.open(str(self.vortex_files[0])).to_polars()
                .with_columns(pl.col("tpep_pickup_datetime").dt.strftime("%A").alias("day_of_week"))
                .group_by("day_of_week")
                .agg(pl.len().alias("trip_count"))
                .sort("day_of_week")
                .collect()
            )
        else:
            # Multi-file mode - Vortex doesn't support patterns/lists like Parquet
            # Must open each file individually (vx.open() only accepts single file path)
            # Use to_polars() which returns a LazyFrame directly (more efficient)
            print(f"  [Polars+Vortex] Opening {len(self.vortex_files)} files individually (no pattern support)...")
            lazy_frames = [
                vx.open(str(f)).to_polars()
                .with_columns(pl.col("tpep_pickup_datetime").dt.strftime("%A").alias("day_of_week"))
                for f in self.vortex_files
            ]
            print(f"  [Polars+Vortex] Concatenating {len(lazy_frames)} lazy frames...")
            return pl.concat(lazy_frames).group_by("day_of_week").agg(pl.len().alias("trip_count")).sort("day_of_week").collect()
    
    def analysis_trips_by_day_of_week_duckdb_parquet(self) -> Any:
        """Count trips by day of week using DuckDB with Parquet."""
        conn = duckdb.connect()
        
        if self.mode == "single":
            # Single merged file - direct load
            parquet_path = str(self.parquet_files[0])
            query = f"""
            SELECT 
                strftime(tpep_pickup_datetime, '%A') as day_of_week,
                COUNT(*) as trip_count
            FROM read_parquet('{parquet_path}')
            GROUP BY day_of_week
            ORDER BY day_of_week
            """
        else:
            # Multi-file mode
            parquet_paths = [str(f) for f in self.parquet_files]
            query = """
            SELECT 
                strftime(tpep_pickup_datetime, '%A') as day_of_week,
                COUNT(*) as trip_count
            FROM read_parquet($1)
            GROUP BY day_of_week
            ORDER BY day_of_week
            """
            return conn.execute(query, [parquet_paths]).arrow()
        
        return conn.execute(query).arrow()
    
    def analysis_trips_by_day_of_week_duckdb_vortex(self) -> Any:
        """Count trips by day of week using DuckDB with Vortex."""
        conn = duckdb.connect()
        conn.execute("INSTALL vortex")
        conn.execute("LOAD vortex")
        
        if self.mode == "single":
            # Single merged file - direct load without UNION ALL
            vortex_path = str(self.vortex_files[0])
            query = f"""
            SELECT 
                strftime(tpep_pickup_datetime, '%A') as day_of_week,
                COUNT(*) as trip_count
            FROM read_vortex('{vortex_path}')
            GROUP BY day_of_week
            ORDER BY day_of_week
            """
        else:
            # Multi-file mode - use glob pattern to read entire folder
            vortex_dir = str(self.vortex_dir)
            vortex_pattern = f"{vortex_dir}/{self.taxi_type}_tripdata_{self.year}-*.vortex"
            
            query = f"""
            SELECT 
                strftime(tpep_pickup_datetime, '%A') as day_of_week,
                COUNT(*) as trip_count
            FROM read_vortex('{vortex_pattern}')
            GROUP BY day_of_week
            ORDER BY day_of_week
            """
        
        return conn.execute(query).arrow()
    
    # Analysis 2: Payment Types
    def analysis_payment_types_polars_parquet(self) -> pl.DataFrame:
        """Analyze payment types using Polars with Parquet."""
        if self.mode == "single":
            return (
                pl.scan_parquet(str(self.parquet_files[0]))
                .group_by("payment_type")
                .agg(
                    pl.len().alias("count"),
                    pl.col("total_amount").sum().alias("total_revenue")
                )
                .sort("payment_type")
                .collect()
            )
        else:
            parquet_paths = [str(f) for f in self.parquet_files]
            return (
                pl.scan_parquet(parquet_paths)
                .group_by("payment_type")
                .agg(
                    pl.len().alias("count"),
                    pl.col("total_amount").sum().alias("total_revenue")
                )
                .sort("payment_type")
                .collect()
            )
    
    def analysis_payment_types_polars_vortex(self) -> pl.DataFrame:
        """Analyze payment types using Polars with Vortex."""
        if self.mode == "single":
            return (
                vx.open(str(self.vortex_files[0])).to_polars()
                .group_by("payment_type")
                .agg(
                    pl.len().alias("count"),
                    pl.col("total_amount").sum().alias("total_revenue")
                )
                .sort("payment_type")
                .collect()
            )
        else:
            # Multi-file mode - Vortex doesn't support patterns/lists like Parquet
            print(f"  [Polars+Vortex] Opening {len(self.vortex_files)} files individually (no pattern support)...")
            lazy_frames = [
                vx.open(str(f)).to_polars()
                for f in self.vortex_files
            ]
            print(f"  [Polars+Vortex] Concatenating {len(lazy_frames)} lazy frames...")
            return pl.concat(lazy_frames).group_by("payment_type").agg(
                pl.len().alias("count"),
                pl.col("total_amount").sum().alias("total_revenue")
            ).sort("payment_type").collect()
    
    def analysis_payment_types_duckdb_parquet(self) -> Any:
        """Analyze payment types using DuckDB with Parquet."""
        conn = duckdb.connect()
        
        if self.mode == "single":
            parquet_path = str(self.parquet_files[0])
            query = f"""
            SELECT 
                payment_type,
                COUNT(*) as count,
                SUM(total_amount) as total_revenue
            FROM read_parquet('{parquet_path}')
            GROUP BY payment_type
            ORDER BY payment_type
            """
        else:
            parquet_paths = [str(f) for f in self.parquet_files]
            query = """
            SELECT 
                payment_type,
                COUNT(*) as count,
                SUM(total_amount) as total_revenue
            FROM read_parquet($1)
            GROUP BY payment_type
            ORDER BY payment_type
            """
            return conn.execute(query, [parquet_paths]).arrow()
        
        return conn.execute(query).arrow()
    
    def analysis_payment_types_duckdb_vortex(self) -> Any:
        """Analyze payment types using DuckDB with Vortex."""
        conn = duckdb.connect()
        conn.execute("INSTALL vortex")
        conn.execute("LOAD vortex")
        
        if self.mode == "single":
            vortex_path = str(self.vortex_files[0])
            query = f"""
            SELECT 
                payment_type,
                COUNT(*) as count,
                SUM(total_amount) as total_revenue
            FROM read_vortex('{vortex_path}')
            GROUP BY payment_type
            ORDER BY payment_type
            """
        else:
            # Multi-file mode - use glob pattern to read entire folder
            vortex_dir = str(self.vortex_dir)
            vortex_pattern = f"{vortex_dir}/{self.taxi_type}_tripdata_{self.year}-*.vortex"
            
            query = f"""
            SELECT 
                payment_type,
                COUNT(*) as count,
                SUM(total_amount) as total_revenue
            FROM read_vortex('{vortex_pattern}')
            GROUP BY payment_type
            ORDER BY payment_type
            """
        
        return conn.execute(query).arrow()
    
    # Analysis 3: Passenger Count
    def analysis_passenger_count_polars_parquet(self) -> pl.DataFrame:
        """Analyze passenger count distribution using Polars with Parquet."""
        if self.mode == "single":
            return (
                pl.scan_parquet(str(self.parquet_files[0]))
                .group_by("passenger_count")
                .agg(pl.len().alias("trip_count"))
                .sort("passenger_count")
                .collect()
            )
        else:
            parquet_paths = [str(f) for f in self.parquet_files]
            return (
                pl.scan_parquet(parquet_paths)
                .group_by("passenger_count")
                .agg(pl.len().alias("trip_count"))
                .sort("passenger_count")
                .collect()
            )
    
    def analysis_passenger_count_polars_vortex(self) -> pl.DataFrame:
        """Analyze passenger count distribution using Polars with Vortex."""
        if self.mode == "single":
            return (
                vx.open(str(self.vortex_files[0])).to_polars()
                .group_by("passenger_count")
                .agg(pl.len().alias("trip_count"))
                .sort("passenger_count")
                .collect()
            )
        else:
            # Multi-file mode - Vortex doesn't support patterns/lists like Parquet
            print(f"  [Polars+Vortex] Opening {len(self.vortex_files)} files individually (no pattern support)...")
            lazy_frames = [
                vx.open(str(f)).to_polars()
                for f in self.vortex_files
            ]
            print(f"  [Polars+Vortex] Concatenating {len(lazy_frames)} lazy frames...")
            return pl.concat(lazy_frames).group_by("passenger_count").agg(
                pl.len().alias("trip_count")
            ).sort("passenger_count").collect()
    
    def analysis_passenger_count_duckdb_parquet(self) -> Any:
        """Analyze passenger count distribution using DuckDB with Parquet."""
        conn = duckdb.connect()
        
        if self.mode == "single":
            parquet_path = str(self.parquet_files[0])
            query = f"""
            SELECT 
                passenger_count,
                COUNT(*) as trip_count
            FROM read_parquet('{parquet_path}')
            GROUP BY passenger_count
            ORDER BY passenger_count
            """
        else:
            parquet_paths = [str(f) for f in self.parquet_files]
            query = """
            SELECT 
                passenger_count,
                COUNT(*) as trip_count
            FROM read_parquet($1)
            GROUP BY passenger_count
            ORDER BY passenger_count
            """
            return conn.execute(query, [parquet_paths]).arrow()
        
        return conn.execute(query).arrow()
    
    def analysis_passenger_count_duckdb_vortex(self) -> Any:
        """Analyze passenger count distribution using DuckDB with Vortex."""
        conn = duckdb.connect()
        conn.execute("INSTALL vortex")
        conn.execute("LOAD vortex")
        
        if self.mode == "single":
            vortex_path = str(self.vortex_files[0])
            query = f"""
            SELECT 
                passenger_count,
                COUNT(*) as trip_count
            FROM read_vortex('{vortex_path}')
            GROUP BY passenger_count
            ORDER BY passenger_count
            """
        else:
            # Multi-file mode - use glob pattern to read entire folder
            vortex_dir = str(self.vortex_dir)
            vortex_pattern = f"{vortex_dir}/{self.taxi_type}_tripdata_{self.year}-*.vortex"
            
            query = f"""
            SELECT 
                passenger_count,
                COUNT(*) as trip_count
            FROM read_vortex('{vortex_pattern}')
            GROUP BY passenger_count
            ORDER BY passenger_count
            """
        
        return conn.execute(query).arrow()
    
    # Analysis 4: Rides by Month (Congestion Fee Impact)
    def analysis_rides_by_month_polars_parquet(self) -> pl.DataFrame:
        """Analyze rides by month and congestion fee using Polars with Parquet."""
        if self.mode == "single":
            return (
                pl.scan_parquet(str(self.parquet_files[0]))
                .with_columns(
                    pl.col("tpep_pickup_datetime").dt.strftime("%Y-%m").alias("month")
                )
                .group_by("month")
                .agg(
                    pl.len().alias("total_rides"),
                    pl.col("cbd_congestion_fee").sum().alias("total_congestion_fee"),
                    (pl.col("cbd_congestion_fee") > 0).sum().alias("rides_with_congestion_fee")
                )
                .sort("month")
                .collect()
            )
        else:
            parquet_paths = [str(f) for f in self.parquet_files]
            return (
                pl.scan_parquet(parquet_paths)
                .with_columns(
                    pl.col("tpep_pickup_datetime").dt.strftime("%Y-%m").alias("month")
                )
                .group_by("month")
                .agg(
                    pl.len().alias("total_rides"),
                    pl.col("cbd_congestion_fee").sum().alias("total_congestion_fee"),
                    (pl.col("cbd_congestion_fee") > 0).sum().alias("rides_with_congestion_fee")
                )
                .sort("month")
                .collect()
            )
    
    def analysis_rides_by_month_polars_vortex(self) -> pl.DataFrame:
        """Analyze rides by month and congestion fee using Polars with Vortex."""
        if self.mode == "single":
            return (
                vx.open(str(self.vortex_files[0])).to_polars()
                .with_columns(pl.col("tpep_pickup_datetime").dt.strftime("%Y-%m").alias("month"))
                .group_by("month")
                .agg(
                    pl.len().alias("total_rides"),
                    pl.col("cbd_congestion_fee").sum().alias("total_congestion_fee"),
                    (pl.col("cbd_congestion_fee") > 0).sum().alias("rides_with_congestion_fee")
                )
                .sort("month")
                .collect()
            )
        else:
            # Multi-file mode - Vortex doesn't support patterns/lists like Parquet
            print(f"  [Polars+Vortex] Opening {len(self.vortex_files)} files individually (no pattern support)...")
            lazy_frames = [
                vx.open(str(f)).to_polars()
                .with_columns(pl.col("tpep_pickup_datetime").dt.strftime("%Y-%m").alias("month"))
                for f in self.vortex_files
            ]
            print(f"  [Polars+Vortex] Concatenating {len(lazy_frames)} lazy frames...")
            return pl.concat(lazy_frames).group_by("month").agg(
                pl.len().alias("total_rides"),
                pl.col("cbd_congestion_fee").sum().alias("total_congestion_fee"),
                (pl.col("cbd_congestion_fee") > 0).sum().alias("rides_with_congestion_fee")
            ).sort("month").collect()
    
    def analysis_rides_by_month_duckdb_parquet(self) -> Any:
        """Analyze rides by month and congestion fee using DuckDB with Parquet."""
        conn = duckdb.connect()
        
        if self.mode == "single":
            parquet_path = str(self.parquet_files[0])
            query = f"""
            SELECT 
                strftime(tpep_pickup_datetime, '%Y-%m') as month,
                COUNT(*) as total_rides,
                SUM(cbd_congestion_fee) as total_congestion_fee,
                SUM(CASE WHEN cbd_congestion_fee > 0 THEN 1 ELSE 0 END) as rides_with_congestion_fee
            FROM read_parquet('{parquet_path}')
            GROUP BY month
            ORDER BY month
            """
        else:
            parquet_paths = [str(f) for f in self.parquet_files]
            query = """
            SELECT 
                strftime(tpep_pickup_datetime, '%Y-%m') as month,
                COUNT(*) as total_rides,
                SUM(cbd_congestion_fee) as total_congestion_fee,
                SUM(CASE WHEN cbd_congestion_fee > 0 THEN 1 ELSE 0 END) as rides_with_congestion_fee
            FROM read_parquet($1)
            GROUP BY month
            ORDER BY month
            """
            return conn.execute(query, [parquet_paths]).arrow()
        
        return conn.execute(query).arrow()
    
    def analysis_rides_by_month_duckdb_vortex(self) -> Any:
        """Analyze rides by month and congestion fee using DuckDB with Vortex."""
        conn = duckdb.connect()
        conn.execute("INSTALL vortex")
        conn.execute("LOAD vortex")
        
        if self.mode == "single":
            vortex_path = str(self.vortex_files[0])
            query = f"""
            SELECT 
                strftime(tpep_pickup_datetime, '%Y-%m') as month,
                COUNT(*) as total_rides,
                SUM(cbd_congestion_fee) as total_congestion_fee,
                SUM(CASE WHEN cbd_congestion_fee > 0 THEN 1 ELSE 0 END) as rides_with_congestion_fee
            FROM read_vortex('{vortex_path}')
            GROUP BY month
            ORDER BY month
            """
        else:
            # Multi-file mode - use glob pattern to read entire folder
            vortex_dir = str(self.vortex_dir)
            vortex_pattern = f"{vortex_dir}/{self.taxi_type}_tripdata_{self.year}-*.vortex"
            
            query = f"""
            SELECT 
                strftime(tpep_pickup_datetime, '%Y-%m') as month,
                COUNT(*) as total_rides,
                SUM(cbd_congestion_fee) as total_congestion_fee,
                SUM(CASE WHEN cbd_congestion_fee > 0 THEN 1 ELSE 0 END) as rides_with_congestion_fee
            FROM read_vortex('{vortex_pattern}')
            GROUP BY month
            ORDER BY month
            """
        
        return conn.execute(query).arrow()
    
    # Analysis 5: Airport Fee Percentage
    def analysis_airport_fee_polars_parquet(self) -> pl.DataFrame:
        """Analyze airport fee percentage using Polars with Parquet."""
        if self.mode == "single":
            combined = pl.scan_parquet(str(self.parquet_files[0])).collect()
        else:
            parquet_paths = [str(f) for f in self.parquet_files]
            combined = pl.scan_parquet(parquet_paths).collect()
        
        total = len(combined)
        with_fee = combined.filter(pl.col("Airport_fee") > 0).height
        
        return pl.DataFrame({
            "total_rides": [total],
            "rides_with_airport_fee": [with_fee],
            "percentage_with_airport_fee": [with_fee / total * 100 if total > 0 else 0]
        })
    
    def analysis_airport_fee_polars_vortex(self) -> pl.DataFrame:
        """Analyze airport fee percentage using Polars with Vortex."""
        if self.mode == "single":
            # to_polars() returns a LazyFrame, collect to get DataFrame
            combined = vx.open(str(self.vortex_files[0])).to_polars().collect()
        else:
            # Multi-file mode - Vortex doesn't support patterns/lists like Parquet
            print(f"  [Polars+Vortex] Opening {len(self.vortex_files)} files individually (no pattern support)...")
            lazy_frames = [
                vx.open(str(f)).to_polars()
                for f in self.vortex_files
            ]
            print(f"  [Polars+Vortex] Concatenating {len(lazy_frames)} lazy frames...")
            combined = pl.concat(lazy_frames).collect()
        
        total = len(combined)
        with_fee = combined.filter(pl.col("Airport_fee") > 0).height
        
        return pl.DataFrame({
            "total_rides": [total],
            "rides_with_airport_fee": [with_fee],
            "percentage_with_airport_fee": [with_fee / total * 100 if total > 0 else 0]
        })
    
    def analysis_airport_fee_duckdb_parquet(self) -> Any:
        """Analyze airport fee percentage using DuckDB with Parquet."""
        conn = duckdb.connect()
        
        if self.mode == "single":
            parquet_path = str(self.parquet_files[0])
            query = f"""
            SELECT 
                COUNT(*) as total_rides,
                SUM(CASE WHEN Airport_fee > 0 THEN 1 ELSE 0 END) as rides_with_airport_fee,
                (SUM(CASE WHEN Airport_fee > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as percentage_with_airport_fee
            FROM read_parquet('{parquet_path}')
            """
        else:
            parquet_paths = [str(f) for f in self.parquet_files]
            query = """
            SELECT 
                COUNT(*) as total_rides,
                SUM(CASE WHEN Airport_fee > 0 THEN 1 ELSE 0 END) as rides_with_airport_fee,
                (SUM(CASE WHEN Airport_fee > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as percentage_with_airport_fee
            FROM read_parquet($1)
            """
            return conn.execute(query, [parquet_paths]).arrow()
        
        return conn.execute(query).arrow()
    
    def analysis_airport_fee_duckdb_vortex(self) -> Any:
        """Analyze airport fee percentage using DuckDB with Vortex."""
        conn = duckdb.connect()
        conn.execute("INSTALL vortex")
        conn.execute("LOAD vortex")
        
        if self.mode == "single":
            vortex_path = str(self.vortex_files[0])
            query = f"""
            SELECT 
                COUNT(*) as total_rides,
                SUM(CASE WHEN Airport_fee > 0 THEN 1 ELSE 0 END) as rides_with_airport_fee,
                (SUM(CASE WHEN Airport_fee > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as percentage_with_airport_fee
            FROM read_vortex('{vortex_path}')
            """
        else:
            # Multi-file mode - use glob pattern to read entire folder
            vortex_dir = str(self.vortex_dir)
            vortex_pattern = f"{vortex_dir}/{self.taxi_type}_tripdata_{self.year}-*.vortex"
            
            query = f"""
            SELECT 
                COUNT(*) as total_rides,
                SUM(CASE WHEN Airport_fee > 0 THEN 1 ELSE 0 END) as rides_with_airport_fee,
                (SUM(CASE WHEN Airport_fee > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as percentage_with_airport_fee
            FROM read_vortex('{vortex_pattern}')
            """
        
        return conn.execute(query).arrow()
    
    # Analysis 6: Rides by Pickup Location (Borough approximation)
    def analysis_rides_by_location_polars_parquet(self) -> pl.DataFrame:
        """Analyze rides by pickup location using Polars with Parquet."""
        if self.mode == "single":
            return (
                pl.scan_parquet(str(self.parquet_files[0]))
                .group_by("PULocationID")
                .agg(pl.len().alias("trip_count"))
                .sort("trip_count", descending=True)
                .head(20)
                .collect()
            )
        else:
            parquet_paths = [str(f) for f in self.parquet_files]
            return (
                pl.scan_parquet(parquet_paths)
                .group_by("PULocationID")
                .agg(pl.len().alias("trip_count"))
                .sort("trip_count", descending=True)
                .head(20)
                .collect()
            )
    
    def analysis_rides_by_location_polars_vortex(self) -> pl.DataFrame:
        """Analyze rides by pickup location using Polars with Vortex."""
        if self.mode == "single":
            return (
                vx.open(str(self.vortex_files[0])).to_polars()
                .group_by("PULocationID")
                .agg(pl.len().alias("trip_count"))
                .sort("trip_count", descending=True)
                .head(20)
                .collect()
            )
        else:
            # Multi-file mode - Vortex doesn't support patterns/lists like Parquet
            print(f"  [Polars+Vortex] Opening {len(self.vortex_files)} files individually (no pattern support)...")
            lazy_frames = [
                vx.open(str(f)).to_polars()
                for f in self.vortex_files
            ]
            print(f"  [Polars+Vortex] Concatenating {len(lazy_frames)} lazy frames...")
            return pl.concat(lazy_frames).group_by("PULocationID").agg(
                pl.len().alias("trip_count")
            ).sort("trip_count", descending=True).head(20).collect()
    
    def analysis_rides_by_location_duckdb_parquet(self) -> Any:
        """Analyze rides by pickup location using DuckDB with Parquet."""
        conn = duckdb.connect()
        
        if self.mode == "single":
            parquet_path = str(self.parquet_files[0])
            query = f"""
            SELECT 
                PULocationID,
                COUNT(*) as trip_count
            FROM read_parquet('{parquet_path}')
            GROUP BY PULocationID
            ORDER BY trip_count DESC
            LIMIT 20
            """
        else:
            parquet_paths = [str(f) for f in self.parquet_files]
            query = """
            SELECT 
                PULocationID,
                COUNT(*) as trip_count
            FROM read_parquet($1)
            GROUP BY PULocationID
            ORDER BY trip_count DESC
            LIMIT 20
            """
            return conn.execute(query, [parquet_paths]).arrow()
        
        return conn.execute(query).arrow()
    
    def analysis_rides_by_location_duckdb_vortex(self) -> Any:
        """Analyze rides by pickup location using DuckDB with Vortex."""
        conn = duckdb.connect()
        conn.execute("INSTALL vortex")
        conn.execute("LOAD vortex")
        
        if self.mode == "single":
            vortex_path = str(self.vortex_files[0])
            query = f"""
            SELECT 
                PULocationID,
                COUNT(*) as trip_count
            FROM read_vortex('{vortex_path}')
            GROUP BY PULocationID
            ORDER BY trip_count DESC
            LIMIT 20
            """
        else:
            # Multi-file mode - use glob pattern to read entire folder
            vortex_dir = str(self.vortex_dir)
            vortex_pattern = f"{vortex_dir}/{self.taxi_type}_tripdata_{self.year}-*.vortex"
            
            query = f"""
            SELECT 
                PULocationID,
                COUNT(*) as trip_count
            FROM read_vortex('{vortex_pattern}')
            GROUP BY PULocationID
            ORDER BY trip_count DESC
            LIMIT 20
            """
        
        return conn.execute(query).arrow()
    
    # Analysis 7: Random Access Test (Query specific location IDs)
    def _get_random_location_ids(self, num_samples: int = 50) -> List[int]:
        """
        Get a sample of random location IDs from the data for random access testing.
        This simulates looking up specific records rather than scanning everything.
        """
        # Use a quick scan to get unique location IDs, then sample
        if self.mode == "single":
            df = pl.scan_parquet(str(self.parquet_files[0])).select("PULocationID").unique().collect()
        else:
            parquet_paths = [str(f) for f in self.parquet_files]
            df = pl.scan_parquet(parquet_paths).select("PULocationID").unique().collect()
        
        location_ids = df["PULocationID"].to_list()
        # Sample random location IDs (or take first N if less available)
        sample_size = min(num_samples, len(location_ids))
        return random.sample(location_ids, sample_size) if len(location_ids) > sample_size else location_ids
    
    def analysis_random_access_polars_parquet(self) -> pl.DataFrame:
        """Random access test: Query specific location IDs using Polars with Parquet."""
        location_ids = self._get_random_location_ids(50)
        
        if self.mode == "single":
            df = (
                pl.scan_parquet(str(self.parquet_files[0]))
                .filter(pl.col("PULocationID").is_in(location_ids))
                .collect()
            )
            return pl.DataFrame({"count": [len(df)]})
        else:
            parquet_paths = [str(f) for f in self.parquet_files]
            df = (
                pl.scan_parquet(parquet_paths)
                .filter(pl.col("PULocationID").is_in(location_ids))
                .collect()
            )
            return pl.DataFrame({"count": [len(df)]})
    
    def analysis_random_access_polars_vortex(self) -> pl.DataFrame:
        """Random access test: Query specific location IDs using Polars with Vortex."""
        location_ids = self._get_random_location_ids(50)
        
        if self.mode == "single":
            print(f"  [Polars+Vortex] Random access: querying {len(location_ids)} specific location IDs...")
            # Collect first to avoid type casting issues with Vortex LazyFrame
            df = vx.open(str(self.vortex_files[0])).to_polars().collect()
            # Filter in eager mode
            filtered_df = df.filter(pl.col("PULocationID").is_in(location_ids))
            return pl.DataFrame({"count": [len(filtered_df)]})
        else:
            print(f"  [Polars+Vortex] Random access: querying {len(location_ids)} specific location IDs...")
            print(f"  [Polars+Vortex] Opening {len(self.vortex_files)} files individually (no pattern support)...")
            # Collect each file first, then filter in eager mode to avoid type casting issues
            dfs = []
            for f in self.vortex_files:
                df = vx.open(str(f)).to_polars().collect()
                filtered_df = df.filter(pl.col("PULocationID").is_in(location_ids))
                dfs.append(filtered_df)
            print(f"  [Polars+Vortex] Concatenating {len(dfs)} filtered dataframes...")
            combined_df = pl.concat(dfs)
            return pl.DataFrame({"count": [len(combined_df)]})
    
    def analysis_random_access_duckdb_parquet(self) -> Any:
        """Random access test: Query specific location IDs using DuckDB with Parquet."""
        conn = duckdb.connect()
        location_ids = self._get_random_location_ids(50)
        location_ids_str = ",".join(str(id) for id in location_ids)
        
        if self.mode == "single":
            parquet_path = str(self.parquet_files[0])
            query = f"""
            SELECT COUNT(*) as count
            FROM read_parquet('{parquet_path}')
            WHERE PULocationID IN ({location_ids_str})
            """
        else:
            parquet_paths = [str(f) for f in self.parquet_files]
            query = f"""
            SELECT COUNT(*) as count
            FROM read_parquet($1)
            WHERE PULocationID IN ({location_ids_str})
            """
            return conn.execute(query, [parquet_paths]).arrow()
        
        return conn.execute(query).arrow()
    
    def analysis_random_access_duckdb_vortex(self) -> Any:
        """Random access test: Query specific location IDs using DuckDB with Vortex."""
        conn = duckdb.connect()
        conn.execute("INSTALL vortex")
        conn.execute("LOAD vortex")
        
        location_ids = self._get_random_location_ids(50)
        location_ids_str = ",".join(str(id) for id in location_ids)
        
        if self.mode == "single":
            vortex_path = str(self.vortex_files[0])
            query = f"""
            SELECT COUNT(*) as count
            FROM read_vortex('{vortex_path}')
            WHERE PULocationID IN ({location_ids_str})
            """
        else:
            # Multi-file mode - use glob pattern to read entire folder
            vortex_dir = str(self.vortex_dir)
            vortex_pattern = f"{vortex_dir}/{self.taxi_type}_tripdata_{self.year}-*.vortex"
            
            query = f"""
            SELECT COUNT(*) as count
            FROM read_vortex('{vortex_pattern}')
            WHERE PULocationID IN ({location_ids_str})
            """
        
        return conn.execute(query).arrow()
    
    def run_benchmark(self) -> List[Dict[str, Any]]:
        """
        Run all benchmarks and collect results.
        
        Returns:
            List of benchmark results
        """
        print("=" * 80)
        print("NYC Taxi Data Benchmark Suite")
        print("=" * 80)
        print(f"Parquet files: {len(self.parquet_files)}")
        print(f"Vortex files: {len(self.vortex_files)}")
        print(f"Runs per benchmark: {self.num_runs}")
        print("=" * 80)
        print()
        
        analyses = [
            ("Trips by Day of Week", [
                ("Polars + Parquet", self.analysis_trips_by_day_of_week_polars_parquet),
                ("Polars + Vortex", self.analysis_trips_by_day_of_week_polars_vortex),
                ("DuckDB + Parquet", self.analysis_trips_by_day_of_week_duckdb_parquet),
                ("DuckDB + Vortex", self.analysis_trips_by_day_of_week_duckdb_vortex),
            ]),
            ("Payment Types", [
                ("Polars + Parquet", self.analysis_payment_types_polars_parquet),
                ("Polars + Vortex", self.analysis_payment_types_polars_vortex),
                ("DuckDB + Parquet", self.analysis_payment_types_duckdb_parquet),
                ("DuckDB + Vortex", self.analysis_payment_types_duckdb_vortex),
            ]),
            ("Passenger Count", [
                ("Polars + Parquet", self.analysis_passenger_count_polars_parquet),
                ("Polars + Vortex", self.analysis_passenger_count_polars_vortex),
                ("DuckDB + Parquet", self.analysis_passenger_count_duckdb_parquet),
                ("DuckDB + Vortex", self.analysis_passenger_count_duckdb_vortex),
            ]),
            ("Rides by Month", [
                ("Polars + Parquet", self.analysis_rides_by_month_polars_parquet),
                ("Polars + Vortex", self.analysis_rides_by_month_polars_vortex),
                ("DuckDB + Parquet", self.analysis_rides_by_month_duckdb_parquet),
                ("DuckDB + Vortex", self.analysis_rides_by_month_duckdb_vortex),
            ]),
            ("Airport Fee", [
                ("Polars + Parquet", self.analysis_airport_fee_polars_parquet),
                ("Polars + Vortex", self.analysis_airport_fee_polars_vortex),
                ("DuckDB + Parquet", self.analysis_airport_fee_duckdb_parquet),
                ("DuckDB + Vortex", self.analysis_airport_fee_duckdb_vortex),
            ]),
            ("Rides by Location", [
                ("Polars + Parquet", self.analysis_rides_by_location_polars_parquet),
                ("Polars + Vortex", self.analysis_rides_by_location_polars_vortex),
                ("DuckDB + Parquet", self.analysis_rides_by_location_duckdb_parquet),
                ("DuckDB + Vortex", self.analysis_rides_by_location_duckdb_vortex),
            ]),
            ("Random Access (50 Location IDs)", [
                ("Polars + Parquet", self.analysis_random_access_polars_parquet),
                ("Polars + Vortex", self.analysis_random_access_polars_vortex),
                ("DuckDB + Parquet", self.analysis_random_access_duckdb_parquet),
                ("DuckDB + Vortex", self.analysis_random_access_duckdb_vortex),
            ]),
        ]
        
        results = []
        
        for analysis_name, methods in analyses:
            print(f"\n{'=' * 80}")
            print(f"Analysis: {analysis_name}")
            print(f"{'=' * 80}")
            
            for method_name, method_func in methods:
                try:
                    print(f"  Running {method_name}...", end=" ", flush=True)
                    # Time only the analysis execution (not report generation)
                    avg_time, std_dev, result = self._run_multiple_times(method_func)
                    print(f"✓ {avg_time:.3f}s (±{std_dev:.3f}s)")
                    
                    # Store analysis result from the first successful method for visualization
                    # (This conversion is NOT included in the benchmark timing)
                    if analysis_name not in self.analysis_results:
                        # Convert result to pandas DataFrame for visualization
                        try:
                            import pandas as pd
                            if isinstance(result, pl.DataFrame):
                                self.analysis_results[analysis_name] = result.to_pandas()
                            elif isinstance(result, pa.Table):
                                self.analysis_results[analysis_name] = result.to_pandas()
                            elif hasattr(result, 'to_pandas'):
                                self.analysis_results[analysis_name] = result.to_pandas()
                            elif hasattr(result, 'to_arrow'):
                                arrow_table = result.to_arrow()
                                if isinstance(arrow_table, pa.Table):
                                    self.analysis_results[analysis_name] = arrow_table.to_pandas()
                            elif isinstance(result, pd.DataFrame):
                                self.analysis_results[analysis_name] = result
                        except Exception as e:
                            # If conversion fails, skip visualization for this analysis
                            pass
                    
                    results.append({
                        "analysis": analysis_name,
                        "method": method_name,
                        "avg_time_seconds": avg_time,
                        "std_dev_seconds": std_dev,
                        "result_rows": len(result) if hasattr(result, '__len__') else 1
                    })
                except Exception as e:
                    print(f"✗ Error: {e}")
                    results.append({
                        "analysis": analysis_name,
                        "method": method_name,
                        "avg_time_seconds": None,
                        "std_dev_seconds": None,
                        "result_rows": None,
                        "error": str(e)
                    })
        
        self.results = results
        return results
    
    def generate_markdown_table(self) -> str:
        """
        Generate a markdown table from benchmark results.
        
        Returns:
            Markdown formatted table string
        """
        if not self.results:
            return "No benchmark results available. Run run_benchmark() first."
        
        # Group by analysis
        analyses = {}
        for result in self.results:
            analysis = result["analysis"]
            if analysis not in analyses:
                analyses[analysis] = []
            analyses[analysis].append(result)
        
        mode_label = "Single-File" if self.mode == "single" else "Multi-File"
        markdown = f"# NYC Taxi Data Benchmark Results ({mode_label} Mode)\n\n"
        markdown += "This benchmark compares the performance of different query engines and data formats\n"
        markdown += "for analyzing NYC taxi data based on the [Row Zero example analyses](https://rowzero.com/datasets/nyc-taxi-data).\n\n"
        
        if self.mode == "multi":
            markdown += "## ⚠️ Important: Understanding These Results\n\n"
            markdown += "**This benchmark tests multi-file aggregation workloads (full table scans).**\n\n"
            markdown += "Vortex claims 100x faster **random access** and 10-20x faster scans, but those claims apply to:\n"
            markdown += "- **Random access patterns** (reading specific rows by index)\n"
            markdown += "- **Single-file operations** (not multi-file like this benchmark)\n"
            markdown += "- **Different query patterns** than full table aggregations\n\n"
            markdown += "Our benchmark shows performance for **multi-file sequential scans with aggregations**, which is a different workload.\n"
            markdown += "For random access or single-file scenarios, Vortex may perform better as claimed.\n\n"
        else:
            markdown += "## ⚠️ Important: Understanding These Results\n\n"
            markdown += "**This benchmark tests single-file aggregation workloads (full table scans).**\n\n"
            markdown += "This mode uses merged files, which better reflects Vortex's claimed performance advantages.\n"
            markdown += "Vortex claims 10-20x faster scans for single-file operations.\n\n"
        
        markdown += "## Benchmark Configuration\n\n"
        if self.mode == "single":
            markdown += f"- **Data Files**: 1 merged parquet file, 1 merged vortex file\n"
            markdown += f"- **File Names**: {self.parquet_files[0].name}, {self.vortex_files[0].name}\n"
        else:
            markdown += f"- **Data Files**: {len(self.parquet_files)} parquet files, {len(self.vortex_files)} vortex files\n"
        markdown += f"- **Runs per benchmark**: {self.num_runs}\n"
        markdown += f"- **Taxi Type**: {self.taxi_type}\n"
        markdown += f"- **Year**: {self.year}\n"
        markdown += f"- **Mode**: {mode_label}\n"
        markdown += f"- **Workload Type**: {'Single-file' if self.mode == 'single' else 'Multi-file'} sequential scans with aggregations\n\n"
        
        markdown += "## Results Summary\n\n"
        markdown += "| Analysis | Polars + Parquet | Polars + Vortex | DuckDB + Parquet | DuckDB + Vortex |\n"
        markdown += "|----------|------------------|-----------------|------------------|-----------------|\n"
        
        for analysis_name in sorted(analyses.keys()):
            results = analyses[analysis_name]
            row = [analysis_name]
            
            for method in ["Polars + Parquet", "Polars + Vortex", "DuckDB + Parquet", "DuckDB + Vortex"]:
                method_result = next((r for r in results if r["method"] == method), None)
                if method_result and method_result["avg_time_seconds"] is not None:
                    time_str = f"{method_result['avg_time_seconds']:.3f}s"
                    if method_result["std_dev_seconds"] > 0:
                        time_str += f" (±{method_result['std_dev_seconds']:.3f}s)"
                    row.append(time_str)
                elif method_result and "error" in method_result:
                    row.append(f"Error: {method_result['error'][:30]}")
                else:
                    row.append("N/A")
            
            markdown += "| " + " | ".join(row) + " |\n"
        
        # Find fastest method for each analysis
        markdown += "\n## Fastest Method by Analysis\n\n"
        markdown += "| Analysis | Fastest Method | Time |\n"
        markdown += "|----------|----------------|------|\n"
        
        for analysis_name in sorted(analyses.keys()):
            results = analyses[analysis_name]
            valid_results = [r for r in results if r.get("avg_time_seconds") is not None]
            if valid_results:
                fastest = min(valid_results, key=lambda x: x["avg_time_seconds"])
                markdown += f"| {analysis_name} | {fastest['method']} | {fastest['avg_time_seconds']:.3f}s |\n"
        
        # Overall summary
        markdown += "\n## Overall Performance Summary\n\n"
        all_valid = [r for r in self.results if r.get("avg_time_seconds") is not None]
        if all_valid:
            by_method = {}
            for result in all_valid:
                method = result["method"]
                if method not in by_method:
                    by_method[method] = []
                by_method[method].append(result["avg_time_seconds"])
            
            markdown += "| Method | Average Time | Total Time (All Analyses) |\n"
            markdown += "|--------|--------------|---------------------------|\n"
            
            for method, times in sorted(by_method.items()):
                avg = statistics.mean(times)
                total = sum(times)
                markdown += f"| {method} | {avg:.3f}s | {total:.3f}s |\n"
        
        return markdown
    
    def add_query_results_to_markdown(self) -> str:
        """
        Add the actual query results (data tables) to the markdown.
        
        Returns:
            Markdown formatted string with query results
        """
        if not self.analysis_results:
            return ""
        
        import pandas as pd
        
        markdown = "\n## Query Results\n\n"
        markdown += "Below are the actual results from each analysis query:\n\n"
        
        # Sort analyses for consistent output
        for analysis_name in sorted(self.analysis_results.keys()):
            df = self.analysis_results[analysis_name]
            
            if df is None or df.empty:
                continue
            
            try:
                markdown += f"### {analysis_name}\n\n"
                
                # Limit number of rows for readability (show first 50 rows)
                df_display = df.head(50).copy()
                
                # Create markdown table manually (no tabulate dependency needed)
                columns = df_display.columns.tolist()
                
                # Create header
                markdown += "| " + " | ".join(str(col) for col in columns) + " |\n"
                markdown += "| " + " | ".join(["---"] * len(columns)) + " |\n"
                
                # Create rows
                for _, row in df_display.iterrows():
                    # Format values, handling None and long numbers
                    formatted_values = []
                    for val in row.values:
                        if val is None:
                            formatted_values.append("")
                        elif isinstance(val, (int, float)):
                            # Format large numbers with commas
                            if isinstance(val, float):
                                formatted_values.append(f"{val:,.2f}" if val != int(val) else f"{int(val):,}")
                            else:
                                formatted_values.append(f"{val:,}")
                        else:
                            formatted_values.append(str(val))
                    markdown += "| " + " | ".join(formatted_values) + " |\n"
                
                if len(df) > 50:
                    markdown += f"\n\n*Showing first 50 of {len(df)} rows*\n"
                
                markdown += "\n\n"
                
            except Exception as e:
                # If markdown conversion fails, try a simpler format
                try:
                    markdown += f"### {analysis_name}\n\n"
                    markdown += "```\n"
                    markdown += str(df.head(20))
                    markdown += "\n```\n\n"
                except:
                    markdown += f"### {analysis_name}\n\n"
                    markdown += f"*Error displaying results: {str(e)}*\n\n"
        
        return markdown
    
    def generate_analysis_histograms(self, output_dir: Path = Path("."), suffix: str = "") -> List[str]:
        """
        Generate histograms for the actual analysis results (data visualizations).
        Similar to visualizations shown on Row Zero NYC Taxi Data page.
        
        Args:
            output_dir: Directory to save histogram images
            suffix: Suffix to add to image filenames
            
        Returns:
            List of generated image file paths
        """
        if not self.analysis_results:
            return []
        
        image_paths = []
        import pandas as pd
        
        # Generate histogram for each analysis
        for analysis_name, df in self.analysis_results.items():
            if df is None or df.empty:
                continue
                
            try:
                # Different visualization based on analysis type
                if "day_of_week" in str(df.columns).lower() or "day" in str(df.columns).lower():
                    # Trips by Day of Week - Bar chart sorted by day order
                    fig, ax = plt.subplots(figsize=(12, 7))
                    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                    
                    # Get column names
                    day_col = next((col for col in df.columns if "day" in col.lower()), df.columns[0])
                    count_col = next((col for col in df.columns if "trip_count" in col.lower() or "count" in col.lower()), df.columns[1])
                    
                    if count_col in df.columns:
                        # Sort by day order
                        df['day_order'] = df[day_col].apply(lambda x: day_order.index(x) if x in day_order else 99)
                        df_sorted = df.sort_values('day_order')
                        
                        bars = ax.bar(df_sorted[day_col], df_sorted[count_col], 
                                     alpha=0.8, color='#2E86AB', edgecolor='#1a5f7a', linewidth=1.5)
                        ax.set_ylabel('Number of Trips', fontsize=13, fontweight='bold')
                        ax.set_xlabel('Day of Week', fontsize=13, fontweight='bold')
                        ax.set_title(f'{analysis_name}\nTrips by Day of Week', fontsize=15, fontweight='bold', pad=20)
                        
                        # Add value labels on bars
                        for bar in bars:
                            height = bar.get_height()
                            ax.text(bar.get_x() + bar.get_width()/2., height,
                                   f'{int(height):,}',
                                   ha='center', va='bottom', fontsize=10, fontweight='bold')
                        
                        ax.grid(axis='y', alpha=0.3, linestyle='--')
                        ax.spines['top'].set_visible(False)
                        ax.spines['right'].set_visible(False)
                        plt.tight_layout()
                        
                        safe_name = analysis_name.lower().replace(" ", "_").replace("/", "_")
                        img_path = output_dir / f"analysis_{safe_name}{suffix}.png"
                        plt.savefig(img_path, dpi=150, bbox_inches='tight')
                        plt.close()
                        image_paths.append(str(img_path))
                
                elif "payment_type" in str(df.columns).lower():
                    # Payment Types - Bar chart with revenue
                    fig, ax = plt.subplots(figsize=(12, 7))
                    
                    payment_col = next((col for col in df.columns if "payment" in col.lower()), df.columns[0])
                    count_col = next((col for col in df.columns if "count" in col.lower()), None)
                    
                    if count_col and count_col in df.columns:
                        df_sorted = df.sort_values(count_col, ascending=False)
                        
                        x_pos = range(len(df_sorted))
                        bars = ax.bar(x_pos, df_sorted[count_col], alpha=0.8, 
                                    color=['#FF6B35', '#F7931E', '#FFD23F', '#06A77D', '#3498DB'][:len(df_sorted)],
                                    edgecolor='black', linewidth=1.5)
                        ax.set_xticks(x_pos)
                        ax.set_xticklabels(df_sorted[payment_col].astype(str), rotation=0)
                        ax.set_ylabel('Number of Rides', fontsize=13, fontweight='bold')
                        ax.set_xlabel('Payment Type', fontsize=13, fontweight='bold')
                        ax.set_title(f'{analysis_name}\nRides by Payment Type', fontsize=15, fontweight='bold', pad=20)
                        
                        # Add value labels
                        for i, bar in enumerate(bars):
                            height = bar.get_height()
                            ax.text(bar.get_x() + bar.get_width()/2., height,
                                   f'{int(height):,}',
                                   ha='center', va='bottom', fontsize=10, fontweight='bold')
                        
                        ax.grid(axis='y', alpha=0.3, linestyle='--')
                        ax.spines['top'].set_visible(False)
                        ax.spines['right'].set_visible(False)
                        plt.tight_layout()
                        
                        safe_name = analysis_name.lower().replace(" ", "_").replace("/", "_")
                        img_path = output_dir / f"analysis_{safe_name}{suffix}.png"
                        plt.savefig(img_path, dpi=150, bbox_inches='tight')
                        plt.close()
                        image_paths.append(str(img_path))
                
                elif "passenger_count" in str(df.columns).lower():
                    # Passenger Count - Bar chart
                    fig, ax = plt.subplots(figsize=(12, 7))
                    
                    passenger_col = next((col for col in df.columns if "passenger" in col.lower()), df.columns[0])
                    count_col = next((col for col in df.columns if "trip_count" in col.lower() or "count" in col.lower()), df.columns[1])
                    
                    if count_col in df.columns:
                        # Filter out NaN values and sort
                        df_clean = df.dropna(subset=[passenger_col, count_col]).copy()
                        df_sorted = df_clean.sort_values(passenger_col)
                        
                        # Convert passenger count to string, handling NaN and numeric values
                        passenger_labels = []
                        for val in df_sorted[passenger_col]:
                            if pd.isna(val):
                                passenger_labels.append("N/A")
                            elif isinstance(val, (int, float)):
                                # Convert to int first to handle float passenger counts like 1.0 -> "1"
                                passenger_labels.append(str(int(val)))
                            else:
                                passenger_labels.append(str(val))
                        
                        bars = ax.bar(passenger_labels, df_sorted[count_col], 
                                    alpha=0.8, color='#06A77D', edgecolor='#047857', linewidth=1.5)
                        ax.set_ylabel('Number of Trips', fontsize=13, fontweight='bold')
                        ax.set_xlabel('Number of Passengers', fontsize=13, fontweight='bold')
                        ax.set_title(f'{analysis_name}\nTrip Distribution by Passenger Count', 
                                   fontsize=15, fontweight='bold', pad=20)
                        
                        # Add value labels
                        for bar in bars:
                            height = bar.get_height()
                            if height > 0:
                                ax.text(bar.get_x() + bar.get_width()/2., height,
                                       f'{int(height):,}',
                                       ha='center', va='bottom', fontsize=10, fontweight='bold')
                        
                        ax.grid(axis='y', alpha=0.3, linestyle='--')
                        ax.spines['top'].set_visible(False)
                        ax.spines['right'].set_visible(False)
                        plt.tight_layout()
                        
                        safe_name = analysis_name.lower().replace(" ", "_").replace("/", "_")
                        img_path = output_dir / f"analysis_{safe_name}{suffix}.png"
                        plt.savefig(img_path, dpi=150, bbox_inches='tight')
                        plt.close()
                        image_paths.append(str(img_path))
                
                elif "month" in str(df.columns).lower():
                    # Rides by Month - Bar chart with trend
                    fig, ax = plt.subplots(figsize=(14, 7))
                    
                    month_col = next((col for col in df.columns if "month" in col.lower()), df.columns[0])
                    rides_col = next((col for col in df.columns if "total_rides" in col.lower() or "rides" in col.lower()), df.columns[1])
                    
                    if rides_col in df.columns:
                        df_sorted = df.sort_values(month_col)
                        
                        bars = ax.bar(df_sorted[month_col], df_sorted[rides_col], 
                                    alpha=0.8, color='#D32F2F', edgecolor='#B71C1C', linewidth=1.5)
                        ax.set_ylabel('Total Rides', fontsize=13, fontweight='bold')
                        ax.set_xlabel('Month', fontsize=13, fontweight='bold')
                        ax.set_title(f'{analysis_name}\nMonthly Ride Counts', fontsize=15, fontweight='bold', pad=20)
                        
                        # Add value labels
                        for bar in bars:
                            height = bar.get_height()
                            ax.text(bar.get_x() + bar.get_width()/2., height,
                                   f'{int(height):,}',
                                   ha='center', va='bottom', fontsize=9, fontweight='bold', rotation=90)
                        
                        plt.xticks(rotation=45, ha='right')
                        ax.grid(axis='y', alpha=0.3, linestyle='--')
                        ax.spines['top'].set_visible(False)
                        ax.spines['right'].set_visible(False)
                        plt.tight_layout()
                        
                        safe_name = analysis_name.lower().replace(" ", "_").replace("/", "_")
                        img_path = output_dir / f"analysis_{safe_name}{suffix}.png"
                        plt.savefig(img_path, dpi=150, bbox_inches='tight')
                        plt.close()
                        image_paths.append(str(img_path))
                
                elif "airport" in str(df.columns).lower() or "airport" in analysis_name.lower():
                    # Airport Fee - Pie chart or bar chart
                    fig, ax = plt.subplots(figsize=(10, 8))
                    
                    if "total_rides" in df.columns and "rides_with_airport_fee" in df.columns:
                        total = df.iloc[0]["total_rides"]
                        with_fee = df.iloc[0]["rides_with_airport_fee"]
                        without_fee = total - with_fee
                        percentage = df.iloc[0].get("percentage_with_airport_fee", (with_fee / total * 100) if total > 0 else 0)
                        
                        # Create pie chart
                        sizes = [with_fee, without_fee]
                        labels = [f'With Airport Fee\n{with_fee:,} rides\n({percentage:.1f}%)', 
                                f'Without Airport Fee\n{without_fee:,} rides\n({100-percentage:.1f}%)']
                        colors = ['#FF6B35', '#4ECDC4']
                        explode = (0.1, 0)  # explode the first slice
                        
                        wedges, texts, autotexts = ax.pie(sizes, labels=labels, colors=colors, 
                                                          autopct='', startangle=90, explode=explode,
                                                          textprops={'fontsize': 11, 'fontweight': 'bold'})
                        ax.set_title(f'{analysis_name}\nAirport Fee Distribution', 
                                   fontsize=15, fontweight='bold', pad=20)
                        plt.tight_layout()
                        
                        safe_name = analysis_name.lower().replace(" ", "_").replace("/", "_")
                        img_path = output_dir / f"analysis_{safe_name}{suffix}.png"
                        plt.savefig(img_path, dpi=150, bbox_inches='tight')
                        plt.close()
                        image_paths.append(str(img_path))
                
                elif "pulocationid" in str(df.columns).lower() or ("location" in str(df.columns).lower() and "pulocation" in str(df.columns).lower()):
                    # Rides by Location - Horizontal bar chart (Top 20)
                    fig, ax = plt.subplots(figsize=(12, 10))
                    
                    location_col = next((col for col in df.columns if "location" in col.lower()), df.columns[0])
                    count_col = next((col for col in df.columns if "trip_count" in col.lower() or "count" in col.lower()), df.columns[1])
                    
                    if count_col in df.columns:
                        df_sorted = df.sort_values(count_col, ascending=False).head(20)
                        
                        bars = ax.barh(df_sorted[location_col].astype(str), df_sorted[count_col], 
                                     alpha=0.8, color='#9B59B6', edgecolor='#7D3C98', linewidth=1.5)
                        ax.set_xlabel('Number of Trips', fontsize=13, fontweight='bold')
                        ax.set_ylabel('Pickup Location ID', fontsize=13, fontweight='bold')
                        ax.set_title(f'{analysis_name}\nTop 20 Pickup Locations', fontsize=15, fontweight='bold', pad=20)
                        
                        # Add value labels
                        for i, bar in enumerate(bars):
                            width = bar.get_width()
                            ax.text(width, bar.get_y() + bar.get_height()/2.,
                                   f' {int(width):,}',
                                   ha='left', va='center', fontsize=9, fontweight='bold')
                        
                        ax.grid(axis='x', alpha=0.3, linestyle='--')
                        ax.spines['top'].set_visible(False)
                        ax.spines['right'].set_visible(False)
                        plt.tight_layout()
                        
                        safe_name = analysis_name.lower().replace(" ", "_").replace("/", "_")
                        img_path = output_dir / f"analysis_{safe_name}{suffix}.png"
                        plt.savefig(img_path, dpi=150, bbox_inches='tight')
                        plt.close()
                        image_paths.append(str(img_path))
                
                elif "random_access" in analysis_name.lower() or (len(df.columns) == 1 and "count" in str(df.columns[0]).lower()):
                    # Random Access - Simple count display
                    fig, ax = plt.subplots(figsize=(10, 6))
                    count_value = df.iloc[0, 0] if len(df) > 0 else 0
                    
                    # Create a simple bar chart with the count
                    bars = ax.bar(['Count'], [count_value], 
                                alpha=0.8, color='#E74C3C', edgecolor='#C0392B', linewidth=2)
                    ax.set_ylabel('Number of Matching Records', fontsize=13, fontweight='bold')
                    ax.set_title(f'{analysis_name}\nRandom Access Query Result (50 Location IDs)', 
                               fontsize=15, fontweight='bold', pad=20)
                    
                    # Add value label
                    ax.text(0, count_value, f'{int(count_value):,}',
                           ha='center', va='bottom', fontsize=16, fontweight='bold')
                    
                    ax.set_ylim(0, count_value * 1.2)
                    ax.grid(axis='y', alpha=0.3, linestyle='--')
                    ax.spines['top'].set_visible(False)
                    ax.spines['right'].set_visible(False)
                    ax.spines['left'].set_visible(False)
                    ax.set_xticks([])
                    plt.tight_layout()
                    
                    safe_name = analysis_name.lower().replace(" ", "_").replace("/", "_")
                    img_path = output_dir / f"analysis_{safe_name}{suffix}.png"
                    plt.savefig(img_path, dpi=150, bbox_inches='tight')
                    plt.close()
                    image_paths.append(str(img_path))
                
                else:
                    # Generic bar chart for first two columns
                    if len(df.columns) >= 2:
                        fig, ax = plt.subplots(figsize=(12, 7))
                        bars = ax.bar(df.iloc[:, 0].astype(str), df.iloc[:, 1], 
                                    alpha=0.8, color='#3498DB', edgecolor='#2980B9', linewidth=1.5)
                        ax.set_ylabel(df.columns[1], fontsize=13, fontweight='bold')
                        ax.set_xlabel(df.columns[0], fontsize=13, fontweight='bold')
                        ax.set_title(f'{analysis_name}', fontsize=15, fontweight='bold', pad=20)
                        
                        # Add value labels
                        for bar in bars:
                            height = bar.get_height()
                            if height > 0:
                                ax.text(bar.get_x() + bar.get_width()/2., height,
                                       f'{int(height):,}',
                                       ha='center', va='bottom', fontsize=10, fontweight='bold')
                        
                        ax.grid(axis='y', alpha=0.3, linestyle='--')
                        ax.spines['top'].set_visible(False)
                        ax.spines['right'].set_visible(False)
                        plt.tight_layout()
                        
                        safe_name = analysis_name.lower().replace(" ", "_").replace("/", "_")
                        img_path = output_dir / f"analysis_{safe_name}{suffix}.png"
                        plt.savefig(img_path, dpi=150, bbox_inches='tight')
                        plt.close()
                        image_paths.append(str(img_path))
                
            except Exception as e:
                print(f"  Warning: Could not generate histogram for {analysis_name}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        return image_paths
    
    def generate_histograms(self, output_dir: Path = Path("."), suffix: str = "") -> List[str]:
        """
        Generate histogram visualizations of benchmark results.
        
        Args:
            output_dir: Directory to save histogram images
            suffix: Suffix to add to image filenames (e.g., "_multi" or "_single")
            
        Returns:
            List of generated image file paths
        """
        if not self.results:
            return []
        
        # Filter valid results
        valid_results = [r for r in self.results if r.get("avg_time_seconds") is not None]
        if not valid_results:
            return []
        
        image_paths = []
        
        # Group by analysis
        analyses = {}
        for result in valid_results:
            analysis = result["analysis"]
            if analysis not in analyses:
                analyses[analysis] = []
            analyses[analysis].append(result)
        
        # 1. Bar chart: Performance by Method (Average across all analyses)
        fig, ax = plt.subplots(figsize=(12, 6))
        methods = ["Polars + Parquet", "Polars + Vortex", "DuckDB + Parquet", "DuckDB + Vortex"]
        method_times = {method: [] for method in methods}
        
        for result in valid_results:
            method = result["method"]
            if method in method_times:
                method_times[method].append(result["avg_time_seconds"])
        
        method_avgs = {method: statistics.mean(times) if times else 0 for method, times in method_times.items()}
        method_stds = {method: statistics.stdev(times) if len(times) > 1 else 0 for method, times in method_times.items()}
        
        bars = ax.bar(method_avgs.keys(), method_avgs.values(), 
                     yerr=[method_stds[m] for m in method_avgs.keys()],
                     capsize=5, alpha=0.7, color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'])
        ax.set_ylabel('Average Time (seconds)', fontsize=12)
        ax.set_xlabel('Method', fontsize=12)
        ax.set_title('Average Performance by Method (All Analyses)', fontsize=14, fontweight='bold')
        ax.grid(axis='y', alpha=0.3)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        img_path = output_dir / f"benchmark_methods_comparison{suffix}.png"
        plt.savefig(img_path, dpi=150, bbox_inches='tight')
        plt.close()
        image_paths.append(str(img_path))
        
        # 2. Grouped bar chart: Performance by Analysis
        fig, ax = plt.subplots(figsize=(14, 8))
        analysis_names = sorted(analyses.keys())
        x = range(len(analysis_names))
        width = 0.2
        
        for i, method in enumerate(methods):
            values = []
            for analysis_name in analysis_names:
                method_result = next((r for r in analyses[analysis_name] if r["method"] == method), None)
                values.append(method_result["avg_time_seconds"] if method_result else 0)
            
            offset = (i - 1.5) * width
            ax.bar([xi + offset for xi in x], values, width, label=method, alpha=0.8)
        
        ax.set_ylabel('Time (seconds)', fontsize=12)
        ax.set_xlabel('Analysis', fontsize=12)
        ax.set_title('Performance by Analysis and Method', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(analysis_names, rotation=45, ha='right')
        ax.legend()
        ax.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        
        img_path = output_dir / f"benchmark_analyses_comparison{suffix}.png"
        plt.savefig(img_path, dpi=150, bbox_inches='tight')
        plt.close()
        image_paths.append(str(img_path))
        
        # 3. Horizontal bar chart: Total time per method (sum of all analyses)
        fig, ax = plt.subplots(figsize=(10, 6))
        method_totals = {}
        for result in valid_results:
            method = result["method"]
            if method not in method_totals:
                method_totals[method] = 0
            method_totals[method] += result["avg_time_seconds"]
        
        sorted_methods = sorted(method_totals.items(), key=lambda x: x[1])
        methods_sorted = [m[0] for m in sorted_methods]
        totals_sorted = [m[1] for m in sorted_methods]
        
        bars = ax.barh(methods_sorted, totals_sorted, alpha=0.7, color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'])
        ax.set_xlabel('Total Time (seconds)', fontsize=12)
        ax.set_title('Total Execution Time by Method (All Analyses Combined)', fontsize=14, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)
        
        # Add value labels on bars
        for i, (method, total) in enumerate(sorted_methods):
            ax.text(total, i, f' {total:.2f}s', va='center', fontsize=10)
        
        plt.tight_layout()
        
        img_path = output_dir / f"benchmark_total_time{suffix}.png"
        plt.savefig(img_path, dpi=150, bbox_inches='tight')
        plt.close()
        image_paths.append(str(img_path))
        
        return image_paths


def run_benchmark_mode(mode: str, parquet_dir: str, vortex_dir: str, taxi_type: str, year: int, num_runs: int):
    """Run benchmark for a specific mode and save results."""
    mode_label = "Single-File" if mode == "single" else "Multi-File"
    
    # Create output directories
    results_dir = Path("results")
    benchmark_dir = results_dir / "benchmark"
    analytics_dir = results_dir / "analytics"
    
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    analytics_dir.mkdir(parents=True, exist_ok=True)
    
    print("\n" + "=" * 80)
    print(f"Running {mode_label} Mode Benchmark")
    print("=" * 80)
    
    benchmark = NYCBenchmark(
        parquet_dir=parquet_dir,
        vortex_dir=vortex_dir,
        taxi_type=taxi_type,
        year=year,
        num_runs=num_runs,
        mode=mode
    )
    
    print(f"Starting {mode_label} benchmark suite...")
    results = benchmark.run_benchmark()
    
    print("\n" + "=" * 80)
    print(f"{mode_label} Benchmark Complete!")
    print("=" * 80)
    
    # Generate markdown report and histograms (NOT included in benchmark timing)
    # All timing measurements are complete at this point
    print(f"\nGenerating reports and visualizations (not timed)...")
    markdown = benchmark.generate_markdown_table()
    
    # Generate performance histograms (benchmark comparisons) - save to benchmark folder
    print(f"  Generating {mode_label} performance histograms...")
    suffix = f"_{mode}"  # e.g., "_multi" or "_single"
    image_paths = benchmark.generate_histograms(output_dir=benchmark_dir, suffix=suffix)
    
    # Generate analysis result histograms (actual data visualizations) - save to analytics folder
    print(f"  Generating {mode_label} analysis result histograms...")
    analysis_image_paths = benchmark.generate_analysis_histograms(output_dir=analytics_dir, suffix=suffix)
    
    # Add query results to markdown
    markdown += benchmark.add_query_results_to_markdown()
    
    # Add histogram references to markdown (using relative paths from Benchmark folder)
    if image_paths:
        markdown += "\n## Performance Visualizations\n\n"
        markdown += "### Average Performance by Method\n\n"
        markdown += f"![Method Comparison](./{Path(image_paths[0]).name})\n\n"
        markdown += "### Performance by Analysis and Method\n\n"
        markdown += f"![Analysis Comparison](./{Path(image_paths[1]).name})\n\n"
        markdown += "### Total Execution Time by Method\n\n"
        markdown += f"![Total Time](./{Path(image_paths[2]).name})\n\n"
    
    if analysis_image_paths:
        markdown += "\n## Analysis Results Visualizations\n\n"
        for img_path in analysis_image_paths:
            img_name = Path(img_path).name
            # Extract analysis name from filename
            analysis_display = img_name.replace("analysis_", "").replace(suffix, "").replace(".png", "").replace("_", " ").title()
            markdown += f"### {analysis_display}\n\n"
            markdown += f"![{analysis_display}](../analytics/{img_name})\n\n"
    
    # Save to mode-specific BENCHMARK file in results/Benchmark/
    benchmark_filename = f"BENCHMARK_{mode.upper()}.md"
    benchmark_path = benchmark_dir / benchmark_filename
    with open(benchmark_path, "w") as f:
        f.write(markdown)
    
    print(f"\nResults saved to {benchmark_path}")
    if image_paths:
        print(f"Performance histograms saved to {benchmark_dir}: {len(image_paths)} files")
    if analysis_image_paths:
        print(f"Analysis histograms saved to {analytics_dir}: {len(analysis_image_paths)} files")
    
    return benchmark_path, image_paths + analysis_image_paths


def main():
    """Main function to run benchmarks with command-line arguments."""
    parser = argparse.ArgumentParser(
        description="NYC Taxi Data Benchmark Suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run both modes (default)
  python benchmark.py
  
  # Run only multi-file mode
  python benchmark.py --mode multi
  
  # Run only single-file mode
  python benchmark.py --mode single
  
  # Run both modes explicitly
  python benchmark.py --mode both
        """
    )
    
    parser.add_argument(
        "--mode",
        type=str,
        choices=["multi", "single", "both"],
        default="both",
        help="Benchmark mode: 'multi' (multi-file), 'single' (single-file), or 'both' (default: both)"
    )
    
    parser.add_argument(
        "--parquet-dir",
        type=str,
        default="ny_taxi_files",
        help="Directory containing parquet files (default: ny_taxi_files)"
    )
    
    parser.add_argument(
        "--vortex-dir",
        type=str,
        default="ny_taxi_files/vortex",
        help="Directory containing vortex files (default: ny_taxi_files/vortex)"
    )
    
    parser.add_argument(
        "--taxi-type",
        type=str,
        default="yellow",
        help="Type of taxi data: 'yellow' or 'green' (default: yellow)"
    )
    
    parser.add_argument(
        "--year",
        type=int,
        default=2025,
        help="Year of the data files (default: 2025)"
    )
    
    parser.add_argument(
        "--num-runs",
        type=int,
        default=5,
        help="Number of runs per benchmark for averaging (default: 5)"
    )
    
    args = parser.parse_args()
    
    parquet_dir = args.parquet_dir
    vortex_dir = args.vortex_dir
    taxi_type = args.taxi_type
    year = args.year
    num_runs = args.num_runs
    mode = args.mode
    
    print("=" * 80)
    print("NYC Taxi Data Benchmark Suite")
    if mode == "both":
        print("Running in BOTH Single-File and Multi-File modes")
    else:
        mode_label = "Single-File" if mode == "single" else "Multi-File"
        print(f"Running in {mode_label} mode only")
    print("=" * 80)
    
    # Check if merged files exist (needed for single mode or both mode)
    merged_parquet = Path(parquet_dir) / f"{taxi_type}_tripdata_{year}_merged.parquet"
    merged_vortex = Path(vortex_dir) / f"{taxi_type}_tripdata_{year}_merged.vortex"
    
    if mode == "single":
        # Single-file mode requires merged files
        if not merged_parquet.exists() or not merged_vortex.exists():
            print("\n❌ Error: Merged files not found!")
            print(f"   Expected files:")
            print(f"   - {merged_parquet}")
            print(f"   - {merged_vortex}")
            print(f"\n   Run 'python fusion_files.py' first to create merged files.")
            return
        run_benchmark_mode("single", parquet_dir, vortex_dir, taxi_type, year, num_runs)
    
    elif mode == "multi":
        # Multi-file mode
        run_benchmark_mode("multi", parquet_dir, vortex_dir, taxi_type, year, num_runs)
    
    else:  # mode == "both"
        # Run both modes
        print("\nRunning Multi-File benchmark...")
        multi_path, multi_images = run_benchmark_mode("multi", parquet_dir, vortex_dir, taxi_type, year, num_runs)
        
        if not merged_parquet.exists() or not merged_vortex.exists():
            print("\n⚠️  Warning: Merged files not found. Single-file mode will be skipped.")
            print(f"   Expected files:")
            print(f"   - {merged_parquet}")
            print(f"   - {merged_vortex}")
            print(f"   Run 'python fusion_files.py' first to create merged files.")
            print(f"\n✅ Multi-file benchmark completed: {multi_path}")
        else:
            print("\n" + "=" * 80)
            print("Running Single-File benchmark...")
            single_path, single_images = run_benchmark_mode("single", parquet_dir, vortex_dir, taxi_type, year, num_runs)
            
            print("\n" + "=" * 80)
            print("All Benchmarks Complete!")
            print("=" * 80)
            print(f"\nResults saved to:")
            print(f"  - {multi_path} (Multi-File Mode)")
            print(f"  - {single_path} (Single-File Mode)")
            print(f"\nCompare the results to see how single-file vs multi-file affects performance!")


if __name__ == "__main__":
    main()

