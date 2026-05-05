package main

import (
	"encoding/json"
	"io"
)

type Cmd struct {
	Args  []string          `json:"args"`
	Stdin string            `json:"stdin,omitempty"`
	Env   map[string]string `json:"env,omitempty"`
}

type Predict struct {
	Path string `json:"path"`
	Sum  string `json:"sum"`
}

type Node struct {
	UID     string    `json:"uid"`
	InDirs  []string  `json:"in_dir"`
	OutDirs []string  `json:"out_dir"`
	Cmds    []Cmd     `json:"cmd"`
	Pool    string    `json:"pool"`
	Predict []Predict `json:"predict,omitempty"`
}

type Graph struct {
	Nodes   []Node         `json:"nodes"`
	Targets []string       `json:"targets"`
	Pools   map[string]int `json:"pools"`
}

func readGraph(r io.Reader) *Graph {
	g := &Graph{}

	Throw(json.NewDecoder(r).Decode(g))

	for i, n := range g.Nodes {
		if n.UID == "" {
			ThrowFmt("graph: node #%d missing uid (out_dir=%v)", i, n.OutDirs)
		}

		if len(n.OutDirs) != 1 {
			ThrowFmt("graph: node %s must have exactly one out_dir, got %d", n.UID, len(n.OutDirs))
		}
	}

	return g
}

func touchPath(dir string) string {
	return dir + "/touch"
}
