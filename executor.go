package main

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Outcome int

const (
	OutcomeSuccess Outcome = iota
	OutcomeFailed
	OutcomeBroken
)

type future struct {
	f       func() Outcome
	o       sync.Once
	outcome Outcome
}

func (fu *future) callOnce() Outcome {
	fu.o.Do(func() {
		fu.outcome = fu.f()
	})

	return fu.outcome
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
}

func newExecutor(g *Graph, cfg *Config, ledger *Ledger) *Executor {
	ex := &Executor{
		g:       g,
		cfg:     cfg,
		cache:   openCache(cfg.CacheFile),
		byOut:   map[string]*Node{},
		futures: map[string]*future{},
		ledger:  ledger,
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

		ex.futures[n.UID] = &future{f: func() Outcome {
			return ex.executeNode(n)
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

// visitAll runs the futures for each touchPath in outs in parallel, waits
// for all, returns one Outcome per input. Programming errors (panics from
// inside executeNode) crash the process — node-level failures are
// captured as OutcomeFailed/OutcomeBroken without unwinding.
func (ex *Executor) visitAll(outs []string) []Outcome {
	wg := &sync.WaitGroup{}
	results := make([]Outcome, len(outs))

	for i, o := range outs {
		n, ok := ex.byOut[o]

		if !ok {
			ThrowFmt("executor: no node produces target %s", o)
		}

		fu := ex.futures[n.UID]

		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			results[idx] = fu.callOnce()
		}(i)
	}

	wg.Wait()

	return results
}

func (ex *Executor) executeNode(n *Node) Outcome {
	ex.total.Add(1)

	guid := n.UID
	out := n.OutDirs[0]
	rec := NodeRec{UID: guid, Out: out, StartedAt: time.Now()}

	if ex.cache.Has(guid) {
		rec.FinishedAt = time.Now()
		rec.Cached = true
		ex.recordRec(rec)

		ex.done.Add(1)
		fmt.Fprintln(os.Stderr, clr(clrG, ex.progress()+" CACHE "+out))

		return OutcomeSuccess
	}

	ins := make([]string, 0, len(n.InDirs))

	for _, in := range n.InDirs {
		ins = append(ins, touchPath(in))
	}

	depResults := ex.visitAll(ins)

	brokenBy := ""

	for i, o := range depResults {
		if o == OutcomeSuccess {
			continue
		}

		brokenBy = ex.byOut[ins[i]].UID

		break
	}

	if brokenBy != "" {
		rec.FinishedAt = time.Now()
		rec.Failed = true
		rec.BrokenBy = brokenBy
		ex.recordRec(rec)

		fmt.Fprintln(os.Stderr, clr(clrR, ex.progress()+" BROKEN BY DEP "+brokenBy+" "+out))

		return OutcomeBroken
	}

	fmt.Fprintln(os.Stderr, clr(clrB, ex.progress()+" ENTER "+out))

	exc := Try(func() {
		dispatchNode(ex, n)
	})

	rec.FinishedAt = time.Now()

	if exc != nil {
		rec.Failed = true
		rec.Error = exc.Error()
		ex.recordRec(rec)

		fmt.Fprintln(os.Stderr, clr(clrR, ex.progress()+" FAILED "+out+": "+exc.Error()))
		fmt.Fprintln(os.Stderr, clr(clrR, "node failed: "+exc.Error()))

		return OutcomeFailed
	}

	ex.recordRec(rec)
	ex.cache.Add(guid)
	ex.done.Add(1)

	fmt.Fprintln(os.Stderr, clr(clrG, ex.progress()+" LEAVE "+out))

	return OutcomeSuccess
}

func (ex *Executor) recordRec(r NodeRec) {
	if ex.ledger != nil {
		ex.ledger.Add(r)
	}
}

// progress returns "{done+1/visited}" — the count of visited nodes so
// far as a rough parallel to assemble.go's complete().
func (ex *Executor) progress() string {
	return fmt.Sprintf("{%d/%d}", ex.done.Load()+1, ex.total.Load())
}
