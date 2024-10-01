### VM.SERIES

#### Syntax

```
VM.SERIES FILTER filterExpr... [START timestamp|rfc3339|+|*] [END timestamp|rfc3339|+|*]
```

**VM.SERIES** returns the list of time series that match a certain label set.

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

The following example returns all series that match either of the selectors `up` or `process_start_time_seconds{job="prometheus"}`:

```
VM.SERIES FILTER up process_start_time_seconds{job="prometheus"}
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