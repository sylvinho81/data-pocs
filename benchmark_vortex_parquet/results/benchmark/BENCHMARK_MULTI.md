# NYC Taxi Data Benchmark Results (Multi-File Mode)

This benchmark compares the performance of different query engines and data formats
for analyzing NYC taxi data based on the [Row Zero example analyses](https://rowzero.com/datasets/nyc-taxi-data).

## ⚠️ Important: Understanding These Results

**This benchmark tests multi-file aggregation workloads (full table scans).**

Vortex claims 100x faster **random access** and 10-20x faster scans, but those claims apply to:
- **Random access patterns** (reading specific rows by index)
- **Single-file operations** (not multi-file like this benchmark)
- **Different query patterns** than full table aggregations

Our benchmark shows performance for **multi-file sequential scans with aggregations**, which is a different workload.
For random access or single-file scenarios, Vortex may perform better as claimed.

## Benchmark Configuration

- **Data Files**: 11 parquet files, 11 vortex files
- **Runs per benchmark**: 5
- **Taxi Type**: yellow
- **Year**: 2025
- **Mode**: Multi-File
- **Workload Type**: Multi-file sequential scans with aggregations

## Results Summary

| Analysis | Polars + Parquet | Polars + Vortex | DuckDB + Parquet | DuckDB + Vortex |
|----------|------------------|-----------------|------------------|-----------------|
| Airport Fee | 0.847s (±0.110s) | 1.205s (±0.206s) | 0.203s (±0.176s) | 0.181s (±0.002s) |
| Passenger Count | 0.098s (±0.003s) | 0.142s (±0.003s) | 0.083s (±0.006s) | 0.223s (±0.006s) |
| Payment Types | 0.161s (±0.008s) | 0.253s (±0.009s) | 0.133s (±0.005s) | 0.260s (±0.004s) |
| Random Access (50 Location IDs) | 0.895s (±0.024s) | 1.598s (±0.052s) | 0.583s (±0.167s) | 1.422s (±0.253s) |
| Rides by Location | 0.368s (±0.023s) | 0.363s (±0.010s) | 0.079s (±0.013s) | 0.226s (±0.009s) |
| Rides by Month | 2.066s (±0.066s) | 2.448s (±0.319s) | 1.063s (±0.011s) | 0.973s (±0.008s) |
| Trips by Day of Week | 1.497s (±0.042s) | 1.524s (±0.059s) | 0.851s (±0.017s) | 0.799s (±0.012s) |

## Fastest Method by Analysis

| Analysis | Fastest Method | Time |
|----------|----------------|------|
| Airport Fee | DuckDB + Vortex | 0.181s |
| Passenger Count | DuckDB + Parquet | 0.083s |
| Payment Types | DuckDB + Parquet | 0.133s |
| Random Access (50 Location IDs) | DuckDB + Parquet | 0.583s |
| Rides by Location | DuckDB + Parquet | 0.079s |
| Rides by Month | DuckDB + Vortex | 0.973s |
| Trips by Day of Week | DuckDB + Vortex | 0.799s |

## Overall Performance Summary

| Method | Average Time | Total Time (All Analyses) |
|--------|--------------|---------------------------|
| DuckDB + Parquet | 0.428s | 2.995s |
| DuckDB + Vortex | 0.583s | 4.084s |
| Polars + Parquet | 0.848s | 5.933s |
| Polars + Vortex | 1.076s | 7.533s |

## Query Results

Below are the actual results from each analysis query:

### Airport Fee

| total_rides | rides_with_airport_fee | percentage_with_airport_fee |
| --- | --- | --- |
| 44,417,596 | 2,927,748 | 6.59 |


### Passenger Count

| passenger_count | trip_count |
| --- | --- |
### Passenger Count

```
    passenger_count  trip_count
0               NaN    10416412
1               0.0      241235
2               1.0    26970031
3               2.0     4698059
4               3.0     1094572
5               4.0      739893
6               5.0      162666
7               6.0       94589
8               7.0          23
9               8.0          83
10              9.0          33
```

### Payment Types

| payment_type | count | total_revenue |
| --- | --- | --- |
| 0 | 10,416,412 | 219,129,364.70 |
| 1 | 28,435,228 | 857,227,389.37 |
| 2 | 4,253,326 | 97,353,492.34 |
| 3 | 287,559 | 2,153,866.42 |
| 4 | 1,025,068 | 2,278,248.58 |
| 5 | 3 | 71.99 |


### Random Access (50 Location IDs)

| count |
| --- |
| 5461206 |


### Rides by Location

| PULocationID | trip_count |
| --- | --- |
| 237 | 1929746 |
| 161 | 1927345 |
| 132 | 1897967 |
| 236 | 1697494 |
| 186 | 1407984 |
| 230 | 1394376 |
| 162 | 1377299 |
| 142 | 1253795 |
| 170 | 1187380 |
| 234 | 1180906 |
| 138 | 1179584 |
| 68 | 1143967 |
| 163 | 1130378 |
| 79 | 1117523 |
| 239 | 1106552 |
| 48 | 1052565 |
| 249 | 975342 |
| 164 | 946566 |
| 141 | 933624 |
| 107 | 837894 |


### Rides by Month

| month | total_rides | total_congestion_fee | rides_with_congestion_fee |
| --- | --- | --- | --- |
| 2007-12 | 1 | 0.75 | 1 |
| 2008-12 | 1 | 0 | 0 |
| 2009-01 | 6 | 3 | 4 |
| 2024-12 | 21 | 0 | 0 |
| 2025-01 | 3,475,234 | 1,679,977.50 | 2,246,523 |
| 2025-02 | 3,577,542 | 1,922,275.75 | 2,600,265 |
| 2025-03 | 4,145,229 | 2,223,608.25 | 3,011,889 |
| 2025-04 | 3,970,568 | 2,113,801.50 | 2,869,048 |
| 2025-05 | 4,591,844 | 2,423,911.25 | 3,284,506 |
| 2025-06 | 4,322,949 | 2,305,133.75 | 3,123,440 |
| 2025-07 | 3,898,971 | 2,088,469 | 2,835,653 |
| 2025-08 | 3,574,080 | 1,889,518.50 | 2,570,863 |
| 2025-09 | 4,251,019 | 2,271,439 | 3,080,640 |
| 2025-10 | 4,428,708 | 2,371,890.75 | 3,208,188 |
| 2025-11 | 4,181,423 | 2,237,072.25 | 3,014,907 |


### Trips by Day of Week

| day_of_week | trip_count | day_order |
| --- | --- | --- |
| Friday | 6,644,948 | 4 |
| Monday | 5,319,426 | 0 |
| Saturday | 6,944,524 | 5 |
| Sunday | 5,937,153 | 6 |
| Thursday | 6,890,345 | 3 |
| Tuesday | 6,076,894 | 1 |
| Wednesday | 6,604,306 | 2 |



## Performance Visualizations

### Average Performance by Method

![Method Comparison](./benchmark_methods_comparison_multi.png)

### Performance by Analysis and Method

![Analysis Comparison](./benchmark_analyses_comparison_multi.png)

### Total Execution Time by Method

![Total Time](./benchmark_total_time_multi.png)


## Analysis Results Visualizations

### Trips By Day Of Week

![Trips By Day Of Week](../analytics/analysis_trips_by_day_of_week_multi.png)

### Payment Types

![Payment Types](../analytics/analysis_payment_types_multi.png)

### Passenger Count

![Passenger Count](../analytics/analysis_passenger_count_multi.png)

### Rides By Month

![Rides By Month](../analytics/analysis_rides_by_month_multi.png)

### Airport Fee

![Airport Fee](../analytics/analysis_airport_fee_multi.png)

### Rides By Location

![Rides By Location](../analytics/analysis_rides_by_location_multi.png)

### Random Access (50 Location Ids)

![Random Access (50 Location Ids)](../analytics/analysis_random_access_(50_location_ids)_multi.png)

