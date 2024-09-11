# VKMetrics

VKMetrics is a module providing a [Prometheus](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metadata)-like API over timeseries data.
Add your data and query it using [PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/).

Currently supported are [Instant Queries](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors) and [Range Queries](https://prometheus.io/docs/prometheus/latest/querying/basics/#range-vector-selectors),
as well as basic [Metadata](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metadata) lookups.

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
127.0.0.1:6379> PROM.CREATE temperature:3:east RETENTION 60 LABELS sensor_id 1 area_id 32 __name__ temperature region east
OK
127.0.0.1:6379> PROM.CREATE temperature:3:west RETENTION 60 LABELS sensor_id 2 area_id 32 __name__ temperature region west
OK
127.0.0.1:6379> PROM.ADD temperature:3:east 1548149181 30
OK
127.0.0.1:6379> PROM.ADD temperature:3:west 1548149191 42
OK 
127.0.0.1:6379>  PROM.QUERY-RANGE "avg(temperature) by(area_id)" START 1548149180 END 1548149210   
```

**Note**
- The `__name__` label represents the name of the measurement, and it is required for ValkeyPromQL to work, and allows metric queries across Redis keys.

## Tests

The module includes a basic set of unit tests and integration tests.

**Unit tests**

To run all unit tests, follow these steps:

    $ cargo test


**Integration tests**

TODO

## Commands

Command names and option names are case-insensitive.

### PROM.QUERY

#### Syntax

```
PROM.QUERY query [TIME timestamp|rfc3339|+|*] [ROUNDING number]
```

**PROM.QUERY** evaluates an instant query at a single point in time.

#### Options

- **query**: Prometheus expression query string.
- **TIME**: evaluation timestamp. Optional. If not specified, use current server time.
- **ROUNDING**: Optional number of decimal places to round values.

#### Return

 TODO

#### Error

Return an error reply in the following cases:

- Query syntax errors.
- A metric references in the query is not found.
- Resource exhaustion / query timeout.

#### Examples

```
PROM.QUERY "sum(rate(process_io_storage_written_bytes_total)) by (job)" TIME 1587396550
```

### PROM.QUERY-RANGE

#### Syntax

```
PROM.QUERY-RANGE query [START timestamp|rfc3339|+|*] [END timestamp|rfc3339|+|*] [STEP duration|number] [ROUNDING number]
```

**PROM.QUERY-RANGE** evaluates an expression query over a range of time.

#### Options

- **query**: Prometheus expression query string.
- **START**: Start timestamp, inclusive. Optional.
- **END**: End timestamp, inclusive. Optional.
- **STEP**: Query resolution step width in duration format or float number of seconds.
- **ROUNDING**: Optional number of decimal places to round values.

#### Return

- TODO

#### Error

Return an error reply in the following cases:

TODO

#### Examples

```
PROM.QUERY-RANGE "sum(rate(rows_inserted_total[5m])) by (type,accountID) > 0" START 1587396550 END 1587396550 STEP 1m
```

### PROM.SERIES

#### Syntax

```
PROM.SERIES MATCH filterExpr... [START timestamp|rfc3339|+|*] [END timestamp|rfc3339|+|*]
```

**PROM.SERIES** returns the list of time series that match a certain label set.

#### Options

- **filterExpr**: Repeated series selector argument that selects the series to return. At least one match[] argument must be provided..
- **START**: Start timestamp, inclusive. Optional.
- **END**: End timestamp, inclusive. Optional.

#### Return

The data section of the query result consists of a list of objects that contain the label name/value pairs which identify 
each series.


#### Error

Return an error reply in the following cases:

TODO

#### Examples

The following example returns all series that match either of the selectors up or process_start_time_seconds{job="prometheus"}:

```
PROM.SERIES MATCH up process_start_time_seconds{job="prometheus"}
``` 
```json
{
   "status" : "success",
   "data" : [
      {
         "__name__" : "up",
         "job" : "prometheus",
         "instance" : "localhost:9090"
      },
      {
         "__name__" : "up",
         "job" : "node",
         "instance" : "localhost:9091"
      },
      {
         "__name__" : "process_start_time_seconds",
         "job" : "prometheus",
         "instance" : "localhost:9090"
      }
   ]
}
```

### PROM.CARDINALITY

#### Syntax

```
PROM.CARDINALITY MATCH filterExpr... [START timestamp|rfc3339|+|*] [END timestamp|rfc3339|+|*]
```

**PROM.SERIES** returns the number of unique time series that match a certain label set.

#### Options

- **filterExpr**: Repeated series selector argument that selects the series to return. At least one match[] argument must be provided..
- **START**: Start timestamp, inclusive. Optional.
- **END**: End timestamp, inclusive. Optional.

#### Return

[Integer number](https://redis.io/docs/reference/protocol-spec#resp-integers) of unique time series.
The data section of the query result consists of a list of objects that contain the label name/value pairs which identify
each series.


#### Error

Return an error reply in the following cases:

TODO

#### Examples
TODO


### PROM.LABELS

#### Syntax

```
PROM.LABELS MATCH filterExpr... [START timestamp|rfc3339|+|*] [END timestamp|rfc3339|+|*]
```

**PROM.LABELS** returns a list of label names.

#### Options

- **filterExpr**: Repeated series selector argument that selects the series to return. At least one match[] argument must be provided..
- **START**: Start timestamp, inclusive. Optional.
- **END**: End timestamp, inclusive. Optional.

#### Return

The data section of the JSON response is a list of string label names.

#### Error

Return an error reply in the following cases:

- Invalid options.
- TODO

#### Examples

```
PROM.LABELS MATCH up process_start_time_seconds{job="prometheus"}
```
```json
{
   "status" : "success",
   "data" : [
      "__name__",
      "instance",
      "job"
   ]
}
```

### PROM.LABEL_VALUES

#### Syntax

```
PROM.LABEL-VALUES label [START timestamp|rfc3339|+|*] [END timestamp|rfc3339|+|*]
```

**PROM.LABEL-VALUES** returns a list of label values for a provided label name.

#### Options

- **label**: The label name for which to retrieve values.
- **START**: Start timestamp, inclusive. Optional.
- **END**: End timestamp, inclusive. Optional.

#### Return

The data section of the JSON response is a list of string label values.

#### Error

Return an error reply in the following cases:

- Invalid options.
- TODO.

#### Examples

This example queries for all label values for the job label:
```
// Create a chat application with LLM model and vector store.
PROM.LABEL-VALUES job
```
```json
{
   "status" : "success",
   "data" : [
      "node",
      "prometheus"
   ]
}
```

## Acknowledgements
This underlying library this project uses originated as a heavily modded `rust` port of [VictoriaMetrics](https://victoriametrics.com).

## License
VKMetrics is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).