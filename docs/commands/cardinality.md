### VM.CARDINALITY

#### Syntax

```
VM.CARDINALITY FILTER selector... [START timestamp|rfc3339|+|*] [END timestamp|rfc3339|+|*]
```

**VM.CARDINALITY** returns the number of unique time series that match a certain label set.

#### Options

- **MATCH**: Repeated series selector argument that selects the series to return. At least one match[] argument must be provided..
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