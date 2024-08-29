# Read Replica Benchmark

The Read Replica Benchmark project is designed to evaluate the performance of read replicas
in database systems under a variety of simulated workloads. The benchmark focuses on measuring
the effectiveness of read replicas in handling complex read operations, ensuring data freshness,
and maintaining performance under load.

The full benchmark specification is available on [notion](https://www.notion.so/materialize/Read-Replica-Benchmark-90b60e455ba648c3a8c9a53297d09492).

This repository is organized as a Cargo workspace containing three crates:

* [rr-bench-core](rr-bench-core/): The core benchmark framework that provides the traits and utilities for defining and running benchmarks.
* [rr-data-gen](rr-bench-datagen/): A tool for generating the initial dataset used by the benchmark, including simulated customer, account, trade, and market data.
* [rr-bench-sqlite](rr-bench-sqlite): A reference implementation of the benchmark using SQLite, demonstrating how to implement the benchmark for a specific database system.
* [rr-bench-postgres](rr-bench-postgres): An implementation of the benchmark for Postgres and Materialize.
