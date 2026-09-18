# molot

Distributed executor for IX build graphs, dispatched through [gorn](https://github.com/pg83/gorn).

IX emits a full build graph — nodes with `in_dir`, `out_dir`, commands, pool — and passes it to a local executor (`assemble`). **molot** is a drop-in replacement that dispatches each node as a separate gorn task: the wrapper script downloads the node's inputs from S3, runs the command inside a `unshare`d mount namespace that exposes the inputs at the exact paths the graph uses, and uploads the output directory back to S3 as a single `zstd`-compressed tarball.

Node uid becomes the gorn task GUID, so S3 objects are content-addressed by build input hash. Re-dispatching an already-built node is an instant no-op (gorn's built-in `HEAD result.json` idempotency check).

## Usage

```sh
export GORN_API=http://gorn-control:7878
export S3_BUCKET=ix-artifacts
export S3_ENDPOINT=http://minio:9000
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# Produce a graph from IX, pipe into molot:
cd path/to/ix && IX_DUMP_GRAPH=1 IX_FLAGS='stalix=' ./ix build lib/c | molot

# Continue independent branches after failures (default is fail-fast):
cd path/to/ix && IX_DUMP_GRAPH=1 IX_FLAGS='stalix=' ./ix build set/ci | IX_KEEP_GOING=yes molot
```

Molot exits with status 2 as soon as the first direct node failure is
reported. Already-running remote gorn tasks may finish and populate the
content-addressed cache, but Molot stops waiting for the rest of the graph.
Set `IX_KEEP_GOING=yes` to keep traversing independent branches and report all
failures plus nodes broken by failed dependencies.

Debug the generated wrap scripts without touching gorn:

```sh
MOLOT_RESOLVE=127.0.0.1:1 MOLOT_GORN=/bin/true MOLOT_DUMP=1 ./molot < graph.json
```

Serve the shared package cache to local IX executors:

```sh
S3_BUCKET=molot S3_ENDPOINT=http://minio:9000 \
  AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  molot cache --listen 0.0.0.0:8054 \
    --kv-endpoint "$KV_ENDPOINT" --kv-bucket "$KV_BUCKET" --kv-timeout "$KV_TIMEOUT"
```

`POST /v1/resolve` accepts a JSON list of node uids and returns the
sub-list present in `s3://cix/complete`. Both resolve versions use only that
index, fetched as one object and cached in memory for 30 seconds.

`GET /v1/blob/<uid>` first reads the UID from KV and returns cached bytes on a
hit, even if the UID is absent from the index. A KV read error returns `500`
without an S3 request. Only a KV `404` falls through to the index: an unlisted
UID returns `404`, while an indexed UID is fetched from
`s3://$S3_BUCKET/molot/<uid>/result.zstd`.

Artifacts up to and including 64 MiB are written to KV before being returned.
Larger artifacts stream from S3 without being cached. A KV write failure is
logged and the already downloaded artifact is returned. Each KV request uses
the explicitly configured timeout.

KV uses the UID as the key and the raw archive as the value. Configure the
bucket in `kv back` with capacity greater than 64 MiB (KV also counts key
bytes). KV is mandatory. Its endpoint, bucket, and positive request timeout
must be set explicitly via the three flags above or the corresponding
`MOLOT_CACHE_KV_*` environment variables. None has a default; missing or
empty settings prevent startup.

Every resolve request's uid list is also queued to a single writer
goroutine which flushes whatever has accumulated as a jsonline chunk to
`s3://$S3_BUCKET/queue/<unix-ts>-<host>-<rand>` (one JSON-encoded uid
per line). `molot stats` — meant to run periodically as a singleton job
— folds all chunks into `s3://$S3_BUCKET/stats`, a JSON dict of
`uid -> last-use unix timestamp`, then deletes the consumed chunks.
Cleanup tooling will consume `stats` later.

## Environment

| Variable | Required | Purpose |
|---|---|---|
| `GORN_API` | yes | URL of `gorn control` (`--api` for each `gorn ignite`) |
| `S3_BUCKET` | yes | S3 bucket for both gorn (`gorn/<uid>/result.json` etc.) and molot artifacts (`gorn/<uid>/result.zstd`) |
| `S3_ENDPOINT` | yes | S3 endpoint URL, forwarded to worker; used to build `MC_HOST_molot` for `minio-client` |
| `AWS_ACCESS_KEY_ID` | yes | forwarded to worker |
| `AWS_SECRET_ACCESS_KEY` | yes | forwarded to worker |
| `AWS_REGION` | no | default `us-east-1` |
| `MOLOT_GORN` | no | path to `gorn` binary; default `gorn` |
| `MOLOT_DUMP` | no | if set, prints each node's wrap script to stderr before dispatching |
| `MOLOT_QUIET` | no | if set, don't stream per-node `gorn ignite` stdout/stderr; only dump them if a node fails |
| `MOLOT_RESOLVE` | yes (executor) | comma-separated `molot cache` endpoints; at startup all graph uids are batch-resolved via `/v1/resolve` and hits are skipped entirely — no gorn call, no dep traversal. Falls back to `IX_PACKAGE_CACHE` when unset; the graph executor refuses to start with an empty list. Misses are backstopped by a per-node S3 stat. Same list via `--resolve`. |
| `MOLOT_CACHE_KV_ENDPOINT` | yes (cache, unless set via CLI) | KV front URL. Same setting via `--kv-endpoint`; no default. |
| `MOLOT_CACHE_KV_BUCKET` | yes (cache, unless set via CLI) | KV bucket for artifact bytes. Same setting via `--kv-bucket`; no default. |
| `MOLOT_CACHE_KV_TIMEOUT` | yes (cache, unless set via CLI) | Positive timeout for a complete KV request, as a Go duration. Same setting via `--kv-timeout`; no default. |
| `IX_KEEP_GOING` | no | exact value `yes` continues independent graph branches after failures; anything else is fail-fast |

## Graph format

Same JSON as `ix/pkgs/bin/assemble/as.go` consumes:

```jsonc
{
  "nodes": [
    {
      "uid": "…",                          // content hash; used as gorn GUID
      "in_dir":  ["/ix/store/<uid>-…"],    // dependency store paths
      "out_dir": ["/ix/store/<uid>-…"],    // exactly one
      "cmd": [
        { "args": ["/path/to/prog", …], "stdin": "…", "env": { "PATH": "…", "out": "…" } }
      ],
      "pool": "threads|network|misc|slot|full"
    }
  ],
  "targets": ["/ix/store/<uid>-…/touch"],
  "pools": { "threads": N, "network": 16, "misc": 4, "slot": 4, "full": 1 }
}
```

`pools` is currently ignored — gorn's endpoint serialization is the only throttle.

## Worker requirements

Designed for stalix endpoints. Expected on `PATH`: `sh`, `tar`, `zstd`, `unzstd`, `minio-client`, `unshare`, `mount`, `mkdir`, `rm`, `mktemp`, `env`, `base64`, `printf`, `chmod`. Kernel must permit unprivileged user namespaces and overlayfs with `userxattr` (Linux 5.11+).

The graph **must** be generated with `IX_FLAGS='stalix='` so IX omits the `confine`/`tmpfs` wrapping around build cmds. Nested user namespaces (molot's outer ns + confine's inner ns) hit EACCES when overlayfs whiteouts are created from the inner ns; stripping the wrap at graph-gen time sidesteps that. molot itself mounts tmpfs on `/ix/build` inside its ns so `${tmp}` paths still resolve.

S3 auth is done via `MC_HOST_molot` (constructed from env vars inside the script) — no `~/.mc/config.json` state.

## See also

- [`CLAUDE.md`](CLAUDE.md) — rules and invariants for working in this repo
- [gorn](https://github.com/pg83/gorn) — the queue/dispatch layer
- [ix](https://github.com/stal-ix/ix) — the source of build graphs
