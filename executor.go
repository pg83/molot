package main

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
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
}

func (ex *Executor) threads() int {
	t := ex.g.Pools["threads"]

	if t <= 0 {
		return 1
	}

	return t
}

func newExecutor(g *Graph, cfg *Config) *Executor {
	ex := &Executor{
		g:       g,
		cfg:     cfg,
		cache:   openCache(cfg.CacheFile),
		byOut:   map[string]*Node{},
		futures: map[string]*future{},
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
				os.Exit(2)
			})
		}()
	}

	wg.Wait()
}

func (ex *Executor) executeNode(n *Node) {
	ex.total.Add(1)

	guid := gornGUID(n.UID)
	out := n.OutDirs[0]

	if ex.cache.Has(guid) {
		ex.done.Add(1)
		fmt.Fprintln(os.Stderr, clr(clrG, out))

		return
	}

	ins := make([]string, 0, len(n.InDirs))

	for _, in := range n.InDirs {
		ins = append(ins, touchPath(in))
	}

	ex.visitAll(ins)

	fmt.Fprintln(os.Stderr, clr(clrB, out))

	dispatchNode(ex, n)

	ex.cache.Add(guid)
	ex.done.Add(1)

	fmt.Fprintln(os.Stderr, clr(clrG, out))
}
