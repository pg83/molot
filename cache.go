package main

import (
	"os"
	"strings"
	"sync"
)

// Cache is a line-per-uid text file of completed gorn GUIDs. Lookup is
// in-memory; writes append to the file and sync, so a crash mid-run
// still remembers everything that finished before it.
type Cache struct {
	mu  sync.Mutex
	set map[string]bool
	f   *os.File
}

func openCache(path string) *Cache {
	c := &Cache{set: map[string]bool{}}

	if path == "" {
		return c
	}

	if data, err := os.ReadFile(path); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)

			if line != "" {
				c.set[line] = true
			}
		}
	} else if !os.IsNotExist(err) {
		Throw(err)
	}

	c.f = Throw2(os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644))

	return c
}

func (c *Cache) Has(key string) bool {
	if c.f == nil {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.set[key]
}

func (c *Cache) Add(key string) {
	if c.f == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.set[key] {
		return
	}

	c.set[key] = true
	Throw2(c.f.WriteString(key + "\n"))
	Throw(c.f.Sync())
}
