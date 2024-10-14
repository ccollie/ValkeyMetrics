### VM.TOP-QUERIES

#### Syntax

```
VM.TOP-QUERIES [TOP_K number] [MAX_LIFETIME duration]
```

**VM.TOP-QUERIES** provides information on the following query types:

* the most frequently executed queries - `topByCount`
* queries with the biggest average execution duration - `topByAvgDuration`
* queries that took the most time for execution - `topBySumDuration`

The number of returned queries can be limited via `TOP_K` argument. Old queries can be filtered out by `MAX_LIFETIME`.
For example,

```sh
$ redis-cli
127.0.0.1:6379> VM.TOP-QUERIES TOP_K 5 MAX_LIFETIME 30s
```

would return up to 5 queries per list, which were executed during the last 30 seconds.
The last `lastQueriesCount` queries with durations at least `minQueryDuration` can be
set via `lastQueriesCount` and `minQueryDuration` module arguments at startup.


#### Options

- **TOP_K**: the number of records to return per metric. Default 20.
- **MAX_LIFETIME**: period from the current timestamp to use for filtering.

#### Return

See example below...

#### Error

Return an error reply in the following cases:

- Invalid options.
- TODO.

#### Examples

This example queries for all label mut values for the job label:
```
127.0.0.1:6379> VM.TOP-QUERIES TOP_K 5 MAX_LIFETIME 30s
```
```json
{
  "topK": "5",
  "maxLifetime": "30s",
  "lastQueriesCount": 500,
  "minQueryDuration": "1ms",
  "topByCount": [
    {
      "query": "(node_nf_conntrack_entries / node_nf_conntrack_entries_limit) > 0.75",
      "timeRangeSeconds": 0,
      "count": 20
    },
    {
      "query": "(\n    max(slo:sli_error:ratio_rate5m{sloth_id=\"sandbox-vmcluster-requests-availability\", sloth_service=\"sandbox-vmcluster\", sloth_slo=\"requests-availability\"} > (14.4 * 0.2)) without (sloth_window)\n    and\n    max(slo:sli_error:ratio_rate1h{sloth_id=\"sandbox-vmcluster-requests-availability\", sloth_service=\"sandbox-vmcluster\", sloth_slo=\"requests-availability\"} > (14.4 * 0.2)) without (sloth_window)\n)\nor\n(\n    max(slo:sli_error:ratio_rate30m{sloth_id=\"sandbox-vmcluster-requests-availability\", sloth_service=\"sandbox-vmcluster\", sloth_slo=\"requests-availability\"} > (6 * 0.2)) without (sloth_window)\n    and\n    max(slo:sli_error:ratio_rate6h{sloth_id=\"sandbox-vmcluster-requests-availability\", sloth_service=\"sandbox-vmcluster\", sloth_slo=\"requests-availability\"} > (6 * 0.2)) without (sloth_window)\n)\n",
      "timeRangeSeconds": 0,
      "count": 20
    },
    {
      "query": "min(anomaly_score{preset=\"node-exporter\", for=\"host_network_transmit_errors\"}) without (model_alias, scheduler_alias)>=1.0",
      "timeRangeSeconds": 0,
      "count": 20
    },
    {
      "query": "rate(node_network_transmit_errs_total[2m]) / rate(node_network_transmit_packets_total[2m]) > 0.01",
      "timeRangeSeconds": 0,
      "count": 20
    },
    {
      "query": "(process_max_fds {namespace=\"monitoring\"}- process_open_fds{namespace=\"monitoring\"}) \u003c 100",
      "timeRangeSeconds": 0,
      "count": 20
    }
  ],
  "topByAvgDuration": [
    {
      "query": "(node_filesystem_files_free{fstype!=\"msdosfs\"} / node_filesystem_files{fstype!=\"msdosfs\"} * 100 \u003c 10 and predict_linear(node_filesystem_files_free{fstype!=\"msdosfs\"}[1h], 24 * 3600) \u003c 0 and ON (instance, device, mountpoint) node_filesystem_readonly{fstype!=\"msdosfs\"} == 0) * on(instance) group_left (nodename) node_uname_info{nodename=~\".+\"}",
      "timeRangeSeconds": 0,
      "avgDurationSeconds": 0.286,
      "count": 20
    },
    {
      "query": "(\n  node_filesystem_avail_bytes{job=\"node-exporter\",fstype!=\"\"} / node_filesystem_size_bytes{job=\"node-exporter\",fstype!=\"\"} * 100 \u003c 15\nand\n  predict_linear(node_filesystem_avail_bytes{job=\"node-exporter\",fstype!=\"\"}[6h], 4*60*60) \u003c 0\nand\n  node_filesystem_readonly{job=\"node-exporter\",fstype!=\"\"} == 0\n)",
      "timeRangeSeconds": 0,
      "avgDurationSeconds": 0.234,
      "count": 20
    },
    {
      "query": "(\n  node_filesystem_files_free{job=\"node-exporter\",fstype!=\"\"} / node_filesystem_files{job=\"node-exporter\",fstype!=\"\"} * 100 \u003c 40\nand\n  predict_linear(node_filesystem_files_free{job=\"node-exporter\",fstype!=\"\"}[6h], 24*60*60) \u003c 0\nand\n  node_filesystem_readonly{job=\"node-exporter\",fstype!=\"\"} == 0\n)",
      "timeRangeSeconds": 0,
      "avgDurationSeconds": 0.230,
      "count": 20
    },
    {
      "query": "(\n  node_filesystem_files_free{job=\"node-exporter\",fstype!=\"\"} / node_filesystem_files{job=\"node-exporter\",fstype!=\"\"} * 100 \u003c 20\nand\n  predict_linear(node_filesystem_files_free{job=\"node-exporter\",fstype!=\"\"}[6h], 4*60*60) \u003c 0\nand\n  node_filesystem_readonly{job=\"node-exporter\",fstype!=\"\"} == 0\n)",
      "timeRangeSeconds": 0,
      "avgDurationSeconds": 0.202,
      "count": 20
    },
    {
      "query": "(\n  node_filesystem_avail_bytes{job=\"node-exporter\",fstype!=\"\"} / node_filesystem_size_bytes{job=\"node-exporter\",fstype!=\"\"} * 100 \u003c 40\nand\n  predict_linear(node_filesystem_avail_bytes{job=\"node-exporter\",fstype!=\"\"}[6h], 24*60*60) \u003c 0\nand\n  node_filesystem_readonly{job=\"node-exporter\",fstype!=\"\"} == 0\n)",
      "timeRangeSeconds": 0,
      "avgDurationSeconds": 0.164,
      "count": 20
    }
  ],
  "topBySumDuration": [
    {
      "query": "(node_filesystem_files_free{fstype!=\"msdosfs\"} / node_filesystem_files{fstype!=\"msdosfs\"} * 100 \u003c 10 and predict_linear(node_filesystem_files_free{fstype!=\"msdosfs\"}[1h], 24 * 3600) \u003c 0 and ON (instance, device, mountpoint) node_filesystem_readonly{fstype!=\"msdosfs\"} == 0) * on(instance) group_left (nodename) node_uname_info{nodename=~\".+\"}",
      "timeRangeSeconds": 0,
      "sumDurationSeconds": 5.718,
      "count": 20
    },
    {
      "query": "(\n  node_filesystem_avail_bytes{job=\"node-exporter\",fstype!=\"\"} / node_filesystem_size_bytes{job=\"node-exporter\",fstype!=\"\"} * 100 \u003c 15\nand\n  predict_linear(node_filesystem_avail_bytes{job=\"node-exporter\",fstype!=\"\"}[6h], 4*60*60) \u003c 0\nand\n  node_filesystem_readonly{job=\"node-exporter\",fstype!=\"\"} == 0\n)",
      "timeRangeSeconds": 0,
      "sumDurationSeconds": 4.674,
      "count": 20
    },
    {
      "query": "(\n  node_filesystem_files_free{job=\"node-exporter\",fstype!=\"\"} / node_filesystem_files{job=\"node-exporter\",fstype!=\"\"} * 100 \u003c 40\nand\n  predict_linear(node_filesystem_files_free{job=\"node-exporter\",fstype!=\"\"}[6h], 24*60*60) \u003c 0\nand\n  node_filesystem_readonly{job=\"node-exporter\",fstype!=\"\"} == 0\n)",
      "timeRangeSeconds": 0,
      "sumDurationSeconds": 4.603,
      "count": 20
    },
    {
      "query": "(\n  node_filesystem_files_free{job=\"node-exporter\",fstype!=\"\"} / node_filesystem_files{job=\"node-exporter\",fstype!=\"\"} * 100 \u003c 20\nand\n  predict_linear(node_filesystem_files_free{job=\"node-exporter\",fstype!=\"\"}[6h], 4*60*60) \u003c 0\nand\n  node_filesystem_readonly{job=\"node-exporter\",fstype!=\"\"} == 0\n)",
      "timeRangeSeconds": 0,
      "sumDurationSeconds": 4.046,
      "count": 20
    },
    {
      "query": "(\n  node_filesystem_avail_bytes{job=\"node-exporter\",fstype!=\"\"} / node_filesystem_size_bytes{job=\"node-exporter\",fstype!=\"\"} * 100 \u003c 40\nand\n  predict_linear(node_filesystem_avail_bytes{job=\"node-exporter\",fstype!=\"\"}[6h], 24*60*60) \u003c 0\nand\n  node_filesystem_readonly{job=\"node-exporter\",fstype!=\"\"} == 0\n)",
      "timeRangeSeconds": 0,
      "sumDurationSeconds": 3.284,
      "count": 20
    }
  ]
}
```