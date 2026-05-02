package main

import (
	"fmt"
	"os"
	"time"
)

const header = `usage: molot [flags] < graph.json

Distributed executor for IX build graphs over gorn. Reads a JSON graph on
stdin (same shape as ix/pkgs/bin/assemble/as.go consumes), dispatches each
node as a separate gorn task via "gorn ignite --wait", with the IX node
uid as the gorn GUID. Artifacts land at
  s3://$S3_BUCKET/molot/<uid>/result.zstd
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
	// `molot hash` — print the uid-seed that IX's ops_molot.py mixes
	// into every uid. Any change to wrap.sh.tmpl shifts the hash, so
	// every uid naturally invalidates when the wrap logic changes.
	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "hash":
			fmt.Println(tmplHash)
			os.Exit(0)
		case "web":
			runSubcommand(func() { webMain(os.Args[2:]) })

			return
		}
	}

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

	runSubcommand(run)
}

func runSubcommand(fn func()) {
	exc := Try(fn)

	exc.Catch(func(e *Exception) {
		fmt.Fprintln(os.Stderr, clr(clrR, "abort: "+e.Error()))
		os.Exit(1)
	})
}

func run() {
	cfg := loadConfig(os.Args[1:])
	g := readGraph(os.Stdin)

	if cfg.UID != "" {
		ex := newExecutor(g, cfg, nil)
		n := findNode(g, cfg.UID)
		fmt.Fprintln(os.Stderr, clr(clrB, fmt.Sprintf("--uid %s: dispatching single node, skipping dep traversal", cfg.UID)))
		dispatchNode(ex, n)
		ex.cache.Add(n.UID)

		return
	}

	ledger := newLedger()
	started := time.Now()

	fmt.Fprintln(os.Stderr, "molot: started, ledger key=s3://"+cfg.S3Bucket+"/"+runKey(started))

	exc := Try(func() {
		uploadLedger(cfg, Run{StartedAt: started, Targets: g.Targets})
	})

	exc.Catch(func(e *Exception) {
		fmt.Fprintln(os.Stderr, clr(clrY, "ledger initial upload: "+e.Error()))
	})

	exc = Try(func() {
		uploadGraph(cfg, started, g)
	})

	exc.Catch(func(e *Exception) {
		fmt.Fprintln(os.Stderr, clr(clrY, "graph upload: "+e.Error()))
	})

	ex := newExecutor(g, cfg, ledger)
	ex.visitAll(g.Targets)

	recs := ledger.Close()

	failed := 0

	for _, r := range recs {
		if r.Failed {
			failed++
		}
	}

	r := Run{
		StartedAt: started,
		EndedAt:   time.Now(),
		Targets:   g.Targets,
		Failed:    failed > 0,
		Nodes:     recs,
	}

	exc = Try(func() {
		uploadLedger(cfg, r)
	})

	exc.Catch(func(e *Exception) {
		fmt.Fprintln(os.Stderr, clr(clrY, "ledger upload: "+e.Error()))
	})

	if r.Failed {
		fmt.Fprintln(os.Stderr, clr(clrR, fmt.Sprintf("molot: %d nodes failed (incl. broken-by-dep) — exit 2", failed)))
		os.Exit(2)
	}
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
