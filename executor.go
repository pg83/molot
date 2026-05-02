package main

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type future struct {
	f func()
	o sync.Once
}

func (fu *future) callOnce() {
	fu.o.Do(fu.f)
}

type Executor struct {
	g       *Graph
	cfg     *Config
	cache   *Cache
	byOut   map[string]*Node
	futures map[string]*future
	done    atomic.Uint64
	total   atomic.Uint64
	ledger  *Ledger
	started time.Time
}

func newExecutor(g *Graph, cfg *Config, ledger *Ledger) *Executor {
	ex := &Executor{
		g:       g,
		cfg:     cfg,
		cache:   openCache(cfg.CacheFile),
		byOut:   map[string]*Node{},
		futures: map[string]*future{},
		ledger:  ledger,
		started: time.Now(),
	}

	for i := range g.Nodes {
		n := &g.Nodes[i]

		for _, od := range n.OutDirs {
			tp := touchPath(od)

			if _, dup := ex.byOut[tp]; dup {
				ThrowFmt("executor: multiple nodes produce %s", tp)
			}

			ex.byOut[tp] = n
		}

		ex.futures[n.UID] = &future{f: func() {
			ex.executeNode(n)
		}}
	}

	for i := range g.Nodes {
		n := &g.Nodes[i]

		for _, in := range n.InDirs {
			if _, ok := ex.byOut[touchPath(in)]; !ok {
				ThrowFmt("executor: node %s depends on %s but no node produces it", n.UID, in)
			}
		}
	}

	return ex
}

func (ex *Executor) visitAll(outs []string) {
	wg := &sync.WaitGroup{}

	for _, o := range outs {
		n, ok := ex.byOut[o]

		if !ok {
			ThrowFmt("executor: no node produces target %s", o)
		}

		fu := ex.futures[n.UID]

		wg.Add(1)

		go func() {
			defer wg.Done()

			exc := Try(func() {
				fu.callOnce()
			})

			exc.Catch(func(e *Exception) {
				fmt.Fprintln(os.Stderr, clr(clrR, "node failed: "+e.Error()))
				ex.flushOnFailure()
				os.Exit(2)
			})
		}()
	}

	wg.Wait()
}

func (ex *Executor) executeNode(n *Node) {
	ex.total.Add(1)

	guid := n.UID
	out := n.OutDirs[0]
	rec := NodeRec{UID: guid, Out: out, StartedAt: time.Now()}

	if ex.cache.Has(guid) {
		rec.FinishedAt = time.Now()
		rec.Cached = true

		if ex.ledger != nil {
			ex.ledger.Add(rec)
		}

		ex.done.Add(1)
		fmt.Fprintln(os.Stderr, clr(clrG, ex.progress()+" CACHE "+out))

		return
	}

	ins := make([]string, 0, len(n.InDirs))

	for _, in := range n.InDirs {
		ins = append(ins, touchPath(in))
	}

	ex.visitAll(ins)

	fmt.Fprintln(os.Stderr, clr(clrB, ex.progress()+" ENTER "+out))

	exc := Try(func() {
		dispatchNode(ex, n)
	})

	rec.FinishedAt = time.Now()

	if exc != nil {
		rec.Failed = true

		if ex.ledger != nil {
			ex.ledger.Add(rec)
		}

		panic(exc)
	}

	if ex.ledger != nil {
		ex.ledger.Add(rec)
	}

	ex.cache.Add(guid)
	ex.done.Add(1)

	fmt.Fprintln(os.Stderr, clr(clrG, ex.progress()+" LEAVE "+out))
}

// flushOnFailure snapshots the ledger and uploads it before the process
// exits via os.Exit(2) — defers don't run on os.Exit, so the success-path
// upload in main.run() is bypassed on failure. Called from visitAll's
// per-goroutine Catch.
func (ex *Executor) flushOnFailure() {
	if ex.ledger == nil {
		return
	}

	run := Run{
		StartedAt: ex.started,
		EndedAt:   time.Now(),
		Targets:   ex.g.Targets,
		Failed:    true,
		Nodes:     ex.ledger.Snapshot(),
	}

	exc := Try(func() {
		uploadLedger(ex.cfg, run)
	})

	exc.Catch(func(e *Exception) {
		fmt.Fprintln(os.Stderr, clr(clrY, "ledger upload (failure path): "+e.Error()))
	})
}

// progress returns "{done+1/visited}" — the count of visited nodes so
// far as a rough parallel to assemble.go's complete().
func (ex *Executor) progress() string {
	return fmt.Sprintf("{%d/%d}", ex.done.Load()+1, ex.total.Load())
}
