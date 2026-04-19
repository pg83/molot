package main

import (
	"fmt"
	"os"
)

func main() {
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
