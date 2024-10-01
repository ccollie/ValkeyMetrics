# ValkeyMetrics

ValkeyMetrics is a module providing a [Prometheus](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metadata)-like API over timeseries data.
Add your data and query it using [PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/).

Currently supported are [Instant Queries](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors) and [Range Queries](https://prometheus.io/docs/prometheus/latest/querying/basics/#range-vector-selectors),
as well as basic [Metadata](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metadata) lookups.

Substantially based on the [VictoriaMetrics](https://victoriametrics.com) project, this module is a work-in-progress and not yet ready for production use.

## Features
- In-memory storage for time series data
- Configurable data retention period
- Supports [PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/) queries
- Supports [Instant Queries](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors) and [Range Queries](https://prometheus.io/docs/prometheus/latest/querying/basics/#range-vector-selectors)
- Supports [Metadata](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metadata) like lookups
- Exposes an API similar to the Prometheus HTTP-API
- Over 200 supported [functions](https://docs.victoriametrics.com/MetricsQL.html#metricsql-functions) (Label, Aggregation, Rollup and Transformation)

## Caveats
- Is highly experimental and not yet ready for production use
- The library does up-front query optimization and caching, so one-off ad-hoc queries are not as fast as repeated queries. These behaviours will be made configurable in future releases.

## Quick Example

Here we'll create a time series representing sensor temperature measurements.
After you create the time series, you can send temperature measurements.
Then you can query the data for a time range on some aggregation rule.

### With `redis-cli`
```sh
$ redis-cli
127.0.0.1:6379> VM.CREATE temperature:3:east temperature{area_id="32",sensor_id="1",region="east"} RETENTION 2h
OK
127.0.0.1:6379> VM.CREATE temperature:3:west temperature{area_id="32",sensor_id="1",region="west"} RETENTION 2h
OK
127.0.0.1:6379> VM.ADD temperature:3:east 1548149181 30
OK
127.0.0.1:6379> VM.ADD temperature:3:west 1548149191 42
OK 
127.0.0.1:6379>  VM.QUERY-RANGE "avg(temperature) by(area_id)" START 1548149180 END 1548149210   
```

## Tests

The module includes a basic set of unit tests and integration tests.

**Unit tests**

To run all unit tests, follow these steps:

    $ cargo test


**Integration tests**

```bash
RLTest --module ./target/debug/libredis_promql.so --module-args
```

#### MacOSX
```bash
RLTest --module ./target/debug/libredis_promql.dylib --module-args
```

## Commands

Command names and option names are case-insensitive.

#### A note about keys
Since the general use-case for this module is querying across timeseries, it is a best practice to group related timeseries 
using "hash tags" in the key. This allows for more efficient querying across related timeseries. For example, if your
metrics are generally grouped by environment, you could use a key like 
`latency:api:{dev}` and `latency:frontend:{staging}`. If you are more likely to group by service, you could use
`latency:{api}:dev` and `latency:{frontend}:staging`.


https://tech.loveholidays.com/redis-cluster-multi-key-command-optimisation-with-hash-tags-8a2bd7ce12de

The following commands are supported

```aiignore
VM.CREATE-SERIES
VM.ALTER-SERIES
VM.ADD
VM.MADD
VM.DELETE-RANGE
VM.DELETE-KEY-RANGE
VM.LABELS
VM.QUERY
VM.QUERY-RANGE
VM.JOIN
VM.COLLATE
VM.GET
VM.MGET
VM.DELETE-SERIES
VM.MRANGE
VM.RANGE
VM.SERIES
VM.TOP-QUERIES
VM.ACTIVE-QUERIES
VM.CARDINALITY
VM.LABEL-NAMES
VM.LABEL-VALUES
VM.STATS
VM.RESET-ROLLUP-CACHE
```


## Acknowledgements
This underlying library this project uses originated as a heavily modded `rust` port of [VictoriaMetrics](https://victoriametrics.com).

## License
ValkeyMetrics is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).