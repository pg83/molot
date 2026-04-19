package main

import (
	"fmt"
	"os"
)

const header = `usage: molot [flags] < graph.json

Distributed executor for IX build graphs over gorn. Reads a JSON graph on
stdin (same shape as ix/pkgs/bin/assemble/as.go consumes), dispatches each
node as a separate gorn task via "gorn ignite --wait", with the IX node
uid as the gorn GUID. Artifacts land at
  s3://$S3_BUCKET/gorn/molot-<tmpl-sha>-<uid>/result.zstd
(tar+zstd of the node's out_dir).

Settings precedence: CLI flags > env vars > --config JSON file > defaults.

Flags:
`

const footer = `
JSON config fields (any may be overridden by env or CLI):
  gorn_api, s3_bucket, s3_endpoint, aws_access_key_id,
  aws_secret_access_key, aws_region, gorn_bin, dump, quiet

Example:
  cd ix && IX_DUMP_GRAPH=1 IX_FLAGS='stalix=' ./ix build lib/c | molot

Debugging a single node (inputs must already be in S3):
  ./molot --uid <uid> < graph.json
`

func main() {
	for _, a := range os.Args[1:] {
		if a == "-h" || a == "--help" {
			fmt.Fprint(os.Stderr, header)
			_, fs := parseCLI([]string{})
			fs.SetOutput(os.Stderr)
			fs.PrintDefaults()
			fmt.Fprint(os.Stderr, footer)
			os.Exit(0)
		}
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
	cfg := loadConfig(os.Args[1:])
	g := readGraph(os.Stdin)
	ex := newExecutor(g, cfg)

	if cfg.UID != "" {
		n := findNode(g, cfg.UID)
		fmt.Fprintf(os.Stderr, "molot: --uid %s: dispatching single node, skipping dep traversal\n", cfg.UID)
		dispatchNode(ex, n)

		return
	}

	ex.visitAll(g.Targets)
}

func findNode(g *Graph, uid string) *Node {
	for i := range g.Nodes {
		if g.Nodes[i].UID == uid {
			return &g.Nodes[i]
		}
	}

	ThrowFmt("--uid %s: no node with that uid in the graph", uid)

	return nil
}
