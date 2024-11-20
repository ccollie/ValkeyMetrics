```aiignore
VM.REPLAY-GROUP groupKey fromTimestamp toTimestamp
    [RULES_DELAY evalDelay]
    [MAX_DATAPOINTS maxDataPoints]
    [RULE_RETRIES ruleRetries]
    [LABELS labelName labelValue..]
```
Backfill alerting and recording rules against the current db by replaying the rules in a group

### Required arguments

<details open><summary><code>groupKey</code></summary>
is key name for the Group being replayed.
</details>

<details open><summary><code>fromTimestamp</code></summary>

`fromTimestamp` is the first timestamp or relative delta from the current time of the request range.
</details>

<details open><summary><code>toTimestamp</code></summary>

`toTimestamp` is the last timestamp of the requested range, or a relative delta from `fromTimestamp`
</details>