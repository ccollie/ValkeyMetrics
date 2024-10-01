### VM.LABEL-NAMES

#### Syntax

```
VM.LABEL-NAMES FILTER selector... [START fromTimestamp] [END toTimestamp]
```

**VM.LABELS** returns a list of label names.

#### Options

- **selector**: Repeated series selector argument that selects the series to return. At least one match[] argument must be provided..
- **START**: Start timestamp, inclusive. Optional.
- **END**: End timestamp, inclusive. Optional.

#### Return

The data section of the JSON response is a list of string label names.

#### Error

Return an error reply in the following cases:

- Invalid options.
- TODO

#### Examples

```
VM.LABELS FILTER up process_start_time_seconds{job="prometheus"}
```
```json
{
   "status" : "success",
   "data" : [
      "__name__",
      "instance",
      "job"
   ]
}
```