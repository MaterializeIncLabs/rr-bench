# rr-bench-postgres

An implementation of the read-replica benchmark for Postgres. This implementation may also
be used for Materialize.

```shell
Usage: rr-bench-postgres [OPTIONS] --duration <DURATION> --writer-url <writer> --reader-url <reader>

Options:
  -d, --duration <DURATION>            The duration of the benchmark (e.g., 10s, 5m, 1h)
      --transactions-per-second <TPS>  The number of transactions per second to execute against the primary database [default: 10]
  -c, --concurrency <CONCURRENCY>      The number of concurrent clients to open against the read replica [default: 1]
      --writer-url <writer>            The URL to the writer node
      --reader-url <reader>            The URL to the reader node
  -h, --help                           Print help
```