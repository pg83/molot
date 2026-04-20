# molot — context for Claude

Distributed executor for IX build graphs on top of gorn. Reads the same JSON graph shape that `ix/pkgs/bin/assemble/as.go` consumes (from IX's `Ops.execute_graph` in `ix/core/ops_sys.py`), dispatches each node as a separate gorn task via `gorn ignite --wait`. Per-node wrapping: download all `in_dir` artifacts from S3, `unshare -r -U -m` + `mount --bind` to expose them at the exact paths the graph uses, run the node's commands, tar+zstd the `out_dir` and upload back to S3.

## Architecture

```
stdin: IX graph JSON
        │
        ▼
   main.go → readGraph → Executor (goroutines + sync.Once futures + visitAll)
        │
        ▼  per node
   dispatch.go: build shell script → base64 → gorn ignite --wait --guid <uid> -- sh -c '…'
        │
        ▼ (gorn queue → SSH → gorn wrap on worker)
   wrap shell on worker:
      export MC_HOST_molot="<scheme>://<key>:<secret>@<host>"   # from S3_ENDPOINT + AWS_*
      mktemp $T; trap rm
      for each in_dir:
        minio-client cp molot/<bucket>/gorn/<dep-uid>/result.zstd $T/dep.N.tar.zst
        tar --use-compress-program=unzstd -xf $T/dep.N.tar.zst -C $T<in_dir>
      mkdir -p $T<out_dir>
      unshare -r -U -m inner.sh:
        mount --bind $T/ix /ix
        per cmd: printf <stdin-b64> | base64 -d | env -i K=V… argv…
      tar --use-compress-program=zstd -cf $T/out.tar.zst -C $T<out_dir> .
      minio-client cp $T/out.tar.zst molot/<bucket>/gorn/<uid>/result.zstd
```

Idempotency: node uid **is** the gorn task GUID. `gorn wrap` already does `HEAD gorn/<uid>/result.json` — a re-submission of an already-built node returns `already-done` instantly, no rebuild, no re-upload.

## Non-negotiable rules

- **Error handling goes through `Throw` / `Try`** (`throw.go`). Same rules as gorn — see `gorn/STYLE.md`. Catch at boundaries (`main`, goroutine entries). `if err != nil { return err }` bubble-ups are forbidden.
- **Blank lines around `if`/`for`/`switch`/`select` and before `return`** unless first/last inside `{}`.
- **Flat layout.** All `.go` files at repo root. No `internal/`, `cmd/`, `pkg/`.
- **Never truncate output.** No `...(truncated)`, no `head -c N` on logs.
- **Nothing invented.** Every path the wrap script touches must come from the graph. No pre-created build tmp dirs, no magic locations. The graph is complete — trust it.
- **Single out_dir per node.** The IX grapher emits exactly one; `readGraph` enforces this. If IX ever emits more, revisit the S3 key layout (`gorn/<uid>/result.zstd` is a single blob).

## Env required at runtime

```
GORN_API               http://gorn-control:7878      # passed to `gorn ignite --api`
S3_BUCKET              bucket name
S3_ENDPOINT            http://minio:9000             # passed to worker as env, used by `aws s3 cp --endpoint-url`
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION             (default us-east-1)
MOLOT_GORN             gorn                          # path to gorn binary; default "gorn"
MOLOT_DUMP             (any)                         # dump the generated wrap scripts to stderr for debugging
```

The AWS/S3 env vars are forwarded to each task via `gorn ignite --env`, so the wrap script on the worker receives them.

## Graph-generation contract

The incoming graph **must** be produced with `IX_FLAGS='stalix='` (set in the environment of `./ix build ...`). That drops the `confine`/`tmpfs` wrapping around build cmds. Without it, nested unshare (molot's outer user ns + confine's inner user ns) triggers EACCES on overlayfs whiteouts and builds fail. See `die/sh0.sh:script_confine` for the branch we're suppressing. molot replaces the lost `tmpfs` step by mounting tmpfs on `/ix/build` itself inside the ns.

## Worker assumptions

- Full stalix toolchain available (`sh`, `tar`, `zstd`, `unzstd`, `minio-client`, `unshare`, `mount`, `mkdir`, `rm`, `printf`, `base64`, `env`, `mktemp`, `chmod`, `cat`).
- Kernel allows unprivileged user namespaces and overlayfs with `userxattr` (Linux 5.11+).
- `/ix` exists as a directory (so we can `mount --bind $T/ix /ix`). Its original contents are hidden inside our mount ns; the host's real `/ix` is not modified.
- S3 auth: the script builds `MC_HOST_molot="<scheme>://<key>:<secret>@<host>"` from the AWS_* env vars forwarded by the executor; no `~/.mc/config.json` on disk. Access keys must not contain `@` or `:` — if they do, switch to `minio-client alias set` (writes to `$HOME/.mc/`).

Fetch nodes (`pool: network`) run on the worker — worker needs outbound internet for tarballs.

## Entrypoints and files

- `main.go` — reads graph from stdin, runs executor.
- `graph.go` — `Graph`/`Node`/`Cmd` types; `readGraph` validates uid + single out_dir.
- `executor.go` — future-per-node, `sync.Once`, recursive `visitAll`. Mirrors `ix/pkgs/bin/assemble/as.go`.
- `dispatch.go` — generates the wrap shell script, base64-encodes, forks `gorn ignite --wait`.
- `config.go` — env-based config.
- `throw.go` — `Throw`/`Try` (copy of gorn's; do not diverge).

## Pools

Currently no client-side pool semaphores. gorn itself serializes at the endpoint-user level (one task per endpoint at a time) and that's the only throttle. If the graph's node count grows large enough to DoS gorn/S3, add pool semaphores keyed by `node.Pool` in `executor.go` — IX pools are `threads`, `network`, `misc`, `slot`, `full`.

## Cancellation

None. When a node fails, in-flight siblings keep running to their natural end. Downstream nodes that depend on the failed node will fail when their wrap script's `aws s3 cp` of the missing dep archive exits non-zero under `set -e`. That's our only propagation mechanism — intentional.

## Build / run

```
go build        # produces ./molot
MOLOT_DUMP=1 GORN_API=… S3_BUCKET=… … ./molot < graph.json
```

Integration test: generate a real graph with `IX_DUMP_GRAPH=1 ./ix build <pkg>` from the ix repo and pipe into molot.

## What's not done

- No client-side pool semaphores (relied on gorn for throttling).
- No live log streaming — build logs land in `s3://<bucket>/gorn/<uid>/{stdout,stderr}` via gorn, retrievable via `gorn control` API.
- No artifact return to local `/ix/store` — consumers must read from S3.
- No integration with IX yet. Next step: replace `Ops.execute_graph` in `ix/core/ops_sys.py:122` with a variant that invokes `molot` instead of `assemble`.
- No `predict` (checksum) handling on fetch nodes.

## Misc

- `guidPrefix = "molot-" + sha256(wrap.sh.tmpl)[:12] + "-"` — any byte change to `wrap.sh.tmpl` rotates every node's GUID, so the first run after a wrap edit rebuilds the graph from scratch. Knob-only changes (timeouts, flags) belong in `dispatch.go` / config, not in the template.
- Slots per node: 1 by default, `Config.FullSlots` when `node.Pool == "full"`. `node.Pool == "network"` controls network isolation, not slot count: pool=network cmds run as-is, everything else is wrapped in `/bin/unshare -r -n` inside `inner.sh` (matches `assemble.go`'s net-deny). `MOLOT_CPUS` (gorn-injected) is the `MAKE_THRS` source; don't re-derive.
- gorn speaks scripts, not argv: dispatch pipes the rendered `wrap.sh` directly to `gorn ignite` stdin — no `--stdin-cmd` flag, no outer `timeout sh -c` wrapper. The 2h cap lives on the inner `unshare` line of `wrap.sh.tmpl`.
