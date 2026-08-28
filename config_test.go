package main

import "testing"

func TestKeepGoingIsCLIOnlyAndDefaultsOff(t *testing.T) {
	o, _ := parseCLI(nil)

	if o.keepGoing {
		t.Fatal("keep-going must default to false")
	}

	o, _ = parseCLI([]string{"-k"})

	if !o.keepGoing {
		t.Fatal("-k must enable keep-going")
	}
}
