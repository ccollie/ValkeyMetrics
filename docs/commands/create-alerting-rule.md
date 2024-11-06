```aiignore
VM.CREATE-ALERTING-RULE groupKey NAME ruleName EXPR expression
    [LABELS [label value ...]]
    [ANNOTATIONS [label value ...]]
    [FOR forDuration]
    [KEEP-FIRING-FOR firingDuration]
    [MAX-ENTRIES maxEntries]
```
Creates a alerting rule

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
<details open><summary><code>forDuration</code></summary>
Alerts are considered firing once they have been returned for this long.
Alerts which have not yet fired for long enough are considered pending.
</details>

<details open><summary><code>LABELS {label value} ...</code></summary>
Metadata labels to add or overwrite before storing the result.
</details>

<details open><summary><code>ANNOTATIONS {label value} ...</code></summary>
Annotations to add to each alert.
</details>

<details open><summary><code>firingDuration</code></summary>
How long an alert will continue firing after the condition that triggered it
has cleared.
</details>