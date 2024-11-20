```aiignore
VM.REPLAY-GROUP groupKey fromTimestamp toTimestamp
    [RULES_DELAY evalDelay]
    [MAX_DATAPOINTS maxDataPoints]
    [RULE_RETRIES ruleRetries]
    [LABELS labelName labelValue..]
```
Backfill alerting and recording rules against the current db by replaying the rules in a group