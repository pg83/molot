# molot

Distributed executor for IX build graphs, dispatched through [gorn](../gorn).

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
cd path/to/ix && IX_DUMP_GRAPH=1 ./ix build lib/c | molot
```

Debug the generated wrap scripts without touching gorn:

```sh
MOLOT_GORN=/bin/true MOLOT_DUMP=1 ./molot < graph.json
```

## Environment

| Variable | Required | Purpose |
|---|---|---|
| `GORN_API` | yes | URL of `gorn control` (`--api` for each `gorn ignite`) |
| `S3_BUCKET` | yes | S3 bucket for both gorn (`gorn/<uid>/result.json` etc.) and molot artifacts (`gorn/<uid>/result.zstd`) |
| `S3_ENDPOINT` | yes | S3 endpoint URL, forwarded to worker for `aws s3 cp --endpoint-url` |
| `AWS_ACCESS_KEY_ID` | yes | forwarded to worker |
| `AWS_SECRET_ACCESS_KEY` | yes | forwarded to worker |
| `AWS_REGION` | no | default `us-east-1` |
| `MOLOT_GORN` | no | path to `gorn` binary; default `gorn` |
| `MOLOT_DUMP` | no | if set, prints each node's wrap script to stderr before dispatching |

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

Designed for stalix endpoints. Expected on `PATH`: `sh`, `tar`, `zstd`, `unzstd`, `aws`, `unshare`, `mount`, `mkdir`, `rm`, `mktemp`, `env`, `base64`, `printf`, `chmod`. Kernel must permit unprivileged user namespaces. `/ix` must exist as a directory (we `mount --bind` a staging tree over it inside our private mount ns; the host's real `/ix` is unaffected).

## See also

- [`CLAUDE.md`](CLAUDE.md) — rules and invariants for working in this repo
- [`../gorn`](../gorn) — the queue/dispatch layer
- [`../ix`](../ix) — the source of build graphs
