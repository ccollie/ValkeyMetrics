
**VM.QUERY** evaluates an instant query at a single point in time.

```
VM.QUERY timestamp query [ROUNDING decimals]
```

#### Options

- **query**: Prometheus expression query string.
- **timestamp**: evaluation timestamp. Optional. If not specified, use current server time.
- **decimals**: Optional number of decimal places to round mut values.

#### Return

TODO

#### Error

Return an error reply in the following cases:

- Query syntax errors.
- A metric references in the query is not found.
- Resource exhaustion / query timeout.

#### Examples

```
VM.QUERY 1587396550 "sum(rate(process_io_storage_written_bytes_total)) by (job)"
```