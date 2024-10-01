### VM.ACTIVE-QUERIES

#### Syntax

```
VM.ACTIVE-QUERIES
```

**VM.ACTIVE-QUERIES** provides information on currently executing queries. It provides the following information per each query:

* The query itself, together with the time range and step args passed to /api/v1/query_range.
* The duration of the query execution.

```sh
$ redis-cli
127.0.0.1:6379> VM.ACTIVE-QUERIES
```

#### Return

The data section of the JSON response is a list of string label values.

#### Error

Return an error reply in the following cases:

- Invalid options.
- TODO.

#### Examples

This example queries for all label values for the job label:
```
127.0.0.1:6379> VM.ACTIVE-QUERIES
```
```json
{
  "status": "ok",
  "data": [
    {
      "duration": "0.103s",
      "id": "17F248B7DFEEB024",
      "query": "(\n  node_filesystem_avail_bytes{job=\"node-exporter\",fstype!=\"\"} / node_filesystem_size_bytes{job=\"node-exporter\",fstype!=\"\"} * 100 < 3\nand\n  node_filesystem_readonly{job=\"node-exporter\",fstype!=\"\"} == 0\n)",
      "start": 1726080900000,
      "end": 1726080900000,
      "step": 300000
    },
    {
      "duration": "0.077s",
      "id": "17F248B7DFEEB025",
      "remote_addr": "10.71.10.4:44162",
      "query": "(node_filesystem_files_free{fstype!=\"msdosfs\"} / node_filesystem_files{fstype!=\"msdosfs\"} * 100 < 10 and predict_linear(node_filesystem_files_free{fstype!=\"msdosfs\"}[1h], 24 * 3600) < 0 and ON (instance, device, mountpoint) node_filesystem_readonly{fstype!=\"msdosfs\"} == 0) * on(instance) group_left (nodename) node_uname_info{nodename=~\".+\"}",
      "start": 1726080900000,
      "end": 1726080900000,
      "step": 300000
    }
  ]
}
```