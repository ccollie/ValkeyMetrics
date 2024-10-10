### VM.DELETE-RANGE

#### Syntax

```
VM.DELETE-RANGE selector.. [START timestamp|rfc3339|+|*] [END timestamp|rfc3339|+|*]
```

**VM.DELETE-RANGE** deletes data for a selection of series in a time range. The timeseries itself is not deleted even if all samples are removed.

#### Options

- **selector**: one or more PromQL series selector.
- **START**: Start timestamp, inclusive. Optional.
- **END**: End timestamp, inclusive. Optional.

#### Return

- the number of samples deleted.

#### Error

Return an error reply in the following cases:

TODO

#### Examples

```
VM.DELETE-RANGE "http_requests{env='staging', status='200'}" START 1587396550 END 1587396550
```