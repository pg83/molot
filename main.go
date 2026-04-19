package main

import (
	"fmt"
	"os"
)

const usage = `usage: molot < graph.json

Distributed executor for IX build graphs over gorn. Reads a JSON graph on
stdin (same shape as ix/pkgs/bin/assemble/as.go consumes), dispatches each
node as a separate gorn task via ` + "`gorn ignite --wait`" + `, with the IX
node uid as the gorn GUID. Artifacts are stored at
s3://$S3_BUCKET/gorn/<uid>/result.zstd (tar+zstd of the node's out_dir).

Required env:
  GORN_API                URL of gorn control API (e.g. http://gorn:7878)
  S3_BUCKET               bucket for artifacts
  S3_ENDPOINT             S3 endpoint URL
  AWS_ACCESS_KEY_ID       forwarded to worker, used in MC_HOST_molot
  AWS_SECRET_ACCESS_KEY   forwarded to worker, used in MC_HOST_molot

Optional env:
  AWS_REGION              default us-east-1
  MOLOT_GORN              path to gorn binary; default "gorn"
  MOLOT_DUMP              if set, prints each node's wrap script to stderr
                          before dispatching (for debugging)
  MOLOT_QUIET             if set, suppress per-node stdout/stderr from
                          "gorn ignite" in the live stream; only dump the
                          buffered logs for nodes that fail

Example:
  cd ix && IX_DUMP_GRAPH=1 ./ix build lib/c | molot
`

func main() {
	for _, a := range os.Args[1:] {
		if a == "-h" || a == "--help" {
			fmt.Fprint(os.Stderr, usage)
			os.Exit(0)
		}

		fmt.Fprintf(os.Stderr, "molot: unknown argument %q (try -h)\n", a)
		os.Exit(2)
	}

	exc := Try(func() {
		run()
	})

	exc.Catch(func(e *Exception) {
		fmt.Fprintln(os.Stderr, "molot: abort:", e.Error())
		os.Exit(1)
	})
}

func run() {
	cfg := loadConfig()
	g := readGraph(os.Stdin)

	newExecutor(g, cfg).visitAll(g.Targets)
}
