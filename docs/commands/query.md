
**VM.QUERY** evaluates an instant query at a single point in time.

```
VM.QUERY query [TIME timestamp|rfc3339|+|*] [ROUNDING number]
```

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
VM.QUERY "sum(rate(process_io_storage_written_bytes_total)) by (job)" TIME 1587396550
```