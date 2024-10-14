### VM.QUERY-RANGE

#### Syntax

```
VM.QUERY-RANGE fromTimestamp toTimestamp query [STEP step] [ROUNDING decimals]
```
evaluates an expression query over a range of time.

### Required Arguments
<details open><summary><code>fromTimestamp</code></summary>

`fromTimestamp` is the first timestamp or relative delta from the current time of the request range.

</details>

<details open><summary><code>toTimestamp</code></summary>

`toTimestamp` is the last timestamp of the requested range, or a relative delta from `fromTimestamp`

</details>

<details open><summary><code>query</code></summary>
Prometheus expression query string.
</details>

### Optional Arguments
<details open><summary><code>step</code></summary>
Query resolution step width in duration format or float number of seconds.
</details>
<details open><summary><code>decimals</code></summary>
number of decimal places to round result mut values.
</details>

#### Return

- TODO

#### Error

Return an error reply in the following cases:

TODO

#### Examples

```
VM.QUERY-RANGE "sum(rate(rows_inserted_total[5m])) by (type,accountID) > 0" START 1587396550 END 1587396550 STEP 1m
```