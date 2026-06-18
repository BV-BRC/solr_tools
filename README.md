# solr_tools

Operational scripts for managing Apache Solr clusters at BV-BRC (Bacterial and Viral Bioinformatics Resource Center).

## Requirements

- Perl with the runtime at `/vol/patric3/cli/ubuntu-runtime/bin/perl`
- Perl modules: `JSON::XS`, `CBOR::XS`, `LWP::UserAgent`, `Furl`, `AnyEvent`, `EV`, `URI`, `Cache::File`, `File::Slurp`, `Filesys::Df`, `Getopt::Long::Descriptive`, `Class::Accessor`, `IO::Socket::SSL`
- A `.netrc` file with credentials for the Solr cluster hosts

## Authentication

All scripts read credentials from a `.netrc` file keyed by hostname:

```
machine bio-gp1.cels.anl.gov login <user> password <pass>
```

Pass the file with `--netrc ~/.netrc` or set the `$NETRC` environment variable. The default Solr endpoint is `https://bio-gp1.cels.anl.gov:15183/solr`; override with `--url`.

---

## Scripts

### Cluster health

**`st-solr-monitor`** — Ping all Solr nodes running locally, report shard health.

```
st-solr-monitor --netrc ~/.netrc
```

**`st-solr-node-stats`** — Collect disk, CPU, and memory metrics for the local node and write JSON output. Run via cron every 45 minutes.

```
st-solr-node-stats --dir /home/svcbvbrc/solr-status
```

**`st-solr-report`** — Aggregate per-node JSON stat files from multiple nodes into a cluster-wide summary.

```
st-solr-report <stats-dir> <output.json>
```

---

### Single-shard replica operations

These submit one async Solr Collections API request and poll until completion.

**`st-add-replica`** — Add a TLOG replica to a collection shard.

```
st-add-replica <collection> [destination-node] [--shard <shard>] [--netrc ~/.netrc]
```

**`st-delete-replica`** — Remove a replica from a specific node.

```
st-delete-replica <collection> <node> [--netrc ~/.netrc]
```

**`st-move-replica`** — Move a replica to another node using Solr's MOVEREPLICA action.

```
st-move-replica <collection> <node> [destination] [--netrc ~/.netrc]
```

**`st-move-follower`** — High-level: add a new follower on the destination, then remove the old one. Requires exactly one follower replica.

```
st-move-follower <collection> [destination] [--netrc ~/.netrc]
```

---

### Bulk node migration

**`st-replicate-shards`** — Copy all shards from a source host onto a destination host, distributing them evenly across the destination's Solr ports.

```
st-replicate-shards \
  --source butternut.cels.anl.gov \
  --dest   willow.cels.anl.gov \
  --ports  15183,15283,15383,15483 \
  --concurrency 4 \
  --netrc ~/.netrc \
  [--dry-run]
```

- Destination node names are resolved from the cluster's live node list (handles FQDNs automatically).
- Shards already present on the destination are counted toward the load-balancing so the script can resume a previously interrupted run.
- Replica type (NRT vs TLOG) is determined per shard: NRT is created if the source replica is NRT, or if the shard has no NRT replicas at all; otherwise TLOG.
- `--concurrency` caps the number of simultaneous index recoveries. A slot is held until the new replica reaches `active` state, not just until the async API key completes.

**`st-drain-node`** — Remove all replicas from a given node. Refuses to remove a replica unless at least 2 other active replicas of that shard exist elsewhere (`--force` overrides).

```
st-drain-node \
  --node butternut.cels.anl.gov \
  --concurrency 4 \
  --netrc ~/.netrc \
  [--dry-run] [--verbose] [--force]
```

- `--verbose` lists the other replicas for each shard in the plan output.
- Safe to re-run; already-removed replicas are simply not found and skipped.

---

### Replica type repair

**`st-fix-nrt-replicas`** — Find every shard that has no NRT replica and promote one of its TLOG replicas to NRT. For each such shard:

1. Creates a new NRT replica on the same node as the chosen TLOG.
2. Waits for the NRT replica to finish recovery and reach `active` state.
3. Deletes the TLOG replica it replaced.

Node selection minimises imbalance: the TLOG host with the fewest existing NRT replicas cluster-wide is chosen, and the running count is updated as tasks are assigned.

```
st-fix-nrt-replicas \
  --concurrency 2 \
  --netrc ~/.netrc \
  [--dry-run]
```

The dry-run output includes the current NRT distribution per node and the before→after count for each planned promotion.

---

### Utilities

**`benchmark.pl`** — Run a distributed query across all shards of a collection on a given node and measure throughput.

```
perl -I lib benchmark.pl <collection> <node>
```

**`pull-ids.pl`** — Extract all IDs from a collection to a folder of per-shard files.

```
perl -I lib pull-ids.pl <collection> <output-folder>
```

