### VM.QUERY-RANGE

#### Syntax

```
VM.QUERY-RANGE query [START timestamp|rfc3339|+|*] [END timestamp|rfc3339|+|*] [STEP duration|number] [ROUNDING number]
```

**VM.QUERY-RANGE** evaluates an expression query over a range of time.

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
VM.QUERY-RANGE "sum(rate(rows_inserted_total[5m])) by (type,accountID) > 0" START 1587396550 END 1587396550 STEP 1m
```