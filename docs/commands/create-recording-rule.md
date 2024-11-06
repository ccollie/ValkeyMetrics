```aiignore
VM.CREATE-RECORDING-RULE groupKey destKey 
    NAME ruleName EXPR expression
    [LABELS [label value ..]]
    [MAX-ENTRIES maxEntries]
```
Creates a recording rule

### Required Arguments

<details open><summary><code>groupKey</code></summary>
the key of the group in which to create the rule
</details>

<details open><summary><code>destKey</code></summary>
the key of the timeseries in which to store resulting samples
</details>

<details open><summary><code>expression</code></summary>
The PromQL/MetricsQL expression to evaluate. Every evaluation cycle this is
evaluated at the current time, and the result recorded as a new set of
time series with the metric name as given by `ruleName`.
</details>

<details open><summary><code>ruleName</code></summary>
The name of the time series to output to. Must be a valid metric name.
</details>

### Optional Arguments 
<details open><summary><code>LABELS {label value} ...</code></summary>
Metadata labels to add or overwrite before storing the result.
</details>

<details open><summary><code>maxEntries</code></summary>
the maximum number of state entries to store.
</details>