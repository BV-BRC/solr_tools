# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

A collection of Perl scripts for managing Apache Solr clusters at BV-BRC (Bacterial and Viral Bioinformatics Resource Center). The tools handle replica management, cluster health monitoring, distributed data extraction, and node metrics collection.

## Running Scripts

Scripts use a specific Perl interpreter path and local lib:

```bash
# Scripts in scripts/ are executable and self-contained
scripts/st-solr-monitor --netrc ~/.netrc
scripts/st-solr-node-stats --dir /path/to/stats-dir
scripts/st-add-replica <collection> [destination-node]
scripts/st-delete-replica <collection> <node>
scripts/st-move-replica <collection> <node> [destination]
scripts/st-move-follower <collection> [destination]
scripts/st-solr-report <stats-dir> <aggr-output.json>
scripts/st-replicate-shards --source <src-host> --dest <dst-host> --ports <p1,p2,...>
scripts/st-drain-node --node <hostname>
scripts/st-fix-nrt-replicas

# Standalone scripts
perl -I lib benchmark.pl <collection> <node>
perl -I lib pull-ids.pl <collection> <folder>

```

All scripts accept `--netrc <file>` (or set `$NETRC` env var). Default Solr URL is `https://bio-gp1.cels.anl.gov:15183/solr`; override with `--url`.

## Architecture

### `lib/Solr.pm` — Core module

The central abstraction used by all scripts. Key design points:

- **Authentication**: Reads credentials from `.netrc` via `lib/Netrc.pm`, or accepts `[realm, user, pass]` directly. Injects credentials into both `LWP::UserAgent` (for admin API calls) and directly into shard URLs (for distributed queries via `Furl`). SSL verification is intentionally disabled for self-signed cluster certs.
- **Cluster status caching**: `cluster_status()` caches the result for 60 seconds using `Cache::File` at `/dev/shm/cache.$USER`. Call `get_cluster_status()` to bypass the cache.
- **Node discovery**: `cluster_nodes()` reads the filesystem layout at `/solr/cluster/*/PORT-*` to find locally running Solr instances. A `STOPPED` marker file suppresses a node; liveness is confirmed via `/proc/<pid>/cmdline`.
- **Distributed queries** (`distributed_query`): Fans out per-shard queries using `AnyEvent::Util::fork_call` (up to 40 processes by default). Uses cursor-based pagination (`cursorMark`) with 25,000 rows per chunk. Output can be written as JSON, CBOR, or tab-separated text. Retries up to 10 times on Solr timeout errors.
- **Async replica operations**: `st-add-replica`, `st-delete-replica`, `st-move-replica` submit async Solr Collections API requests and poll `REQUESTSTATUS` every 3 seconds until completion.

### `scripts/` — CLI tools (`st-` prefix)

| Script | Purpose |
|---|---|
| `st-solr-monitor` | Ping all local cluster nodes, report shard health |
| `st-solr-node-stats` | Collect disk/CPU/memory stats and write JSON (run via cron every 45 min) |
| `st-solr-report` | Aggregate JSON stat files from multiple nodes into a summary |
| `st-add-replica` | Add a single tlog replica to a collection shard (one at a time, polls to completion) |
| `st-delete-replica` | Remove a replica from a specific node |
| `st-move-replica` | Move a replica using Solr's MOVEREPLICA action |
| `st-move-follower` | High-level: adds a new follower then removes the old one |
| `st-replicate-shards` | Bulk-replicate all shards from a source host to a destination host across specified ports |
| `st-drain-node` | Remove all replicas from a given node |
| `st-fix-nrt-replicas` | Find shards missing an NRT replica and promote a TLOG replica to NRT |

### Bulk migration scripts (`st-replicate-shards`, `st-drain-node`, `st-fix-nrt-replicas`)

These three scripts share a common design that differs from the older single-operation tools:

- **Cluster status fallback**: Try the configured `--url` first; if unreachable, fall back to any live local node found via `cluster_nodes()`. Always call `get_cluster_status()` (not the cached `cluster_status()`) for fresh data.
- **Hostname resolution**: Destination node names are resolved from `cluster.live_nodes` (FQDN format `hostname:port_solr`), never constructed by concatenating a hostname string. This avoids silent failures from short-name vs FQDN mismatches.
- **Concurrency control** (`--concurrency`, default 2): Limits the number of simultaneous index recovery operations. A slot is held from ADDREPLICA submission until the replica reaches `active` state in cluster status — not just until the async API key completes. This prevents flooding the cluster with recoveries.
- **Resumability**: `st-replicate-shards` tracks already-placed replicas and seeds the load-balancing counts from them, so re-running after an abort picks up where it left off without skewing the distribution.
- **Replica type awareness**: `st-replicate-shards` inspects each source replica's type and the shard's existing replica set to decide whether to create NRT or TLOG. `st-fix-nrt-replicas` specifically targets shards with no NRT replica.

### `st-fix-nrt-replicas` — NRT promotion workflow

Three-phase operation per shard, all holding a concurrency slot:
1. `add_async` — submit ADDREPLICA (NRT) async, poll REQUESTSTATUS
2. `recovering` — async done, poll cluster status until NRT reaches `active`
3. `delete_async` — submit DELETEREPLICA for the replaced TLOG, poll REQUESTSTATUS

Node selection chooses the TLOG host with the fewest existing NRT replicas cluster-wide, updating the running count as tasks are assigned so the distribution stays balanced.

### Credentials and endpoints

- Credentials always come from a `.netrc` file keyed by hostname. Pass `--netrc <file>` or set `$NETRC`.
- The default cluster endpoint is `https://bio-gp1.cels.anl.gov:15183/solr`.


### Dependencies (Perl modules)

`JSON::XS`, `CBOR::XS`, `LWP::UserAgent`, `Furl`, `AnyEvent`, `EV`, `URI`, `Cache::File`, `File::Slurp`, `Filesys::Df`, `Getopt::Long::Descriptive`, `Class::Accessor`, `IO::Socket::SSL`
