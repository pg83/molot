package main

import "os"

const (
	clrRst = "\x1b[0m"
	clrR   = "\x1b[91m" // red     — failures, aborts
	clrG   = "\x1b[92m" // green   — successes
	clrY   = "\x1b[93m" // yellow  — retryable / transient
	clrB   = "\x1b[94m" // blue    — info / in-progress
)

var useColor = func() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}

	if os.Getenv("TERM") == "dumb" {
		return false
	}

	fi, err := os.Stderr.Stat()

	if err != nil {
		return false
	}

	return (fi.Mode() & os.ModeCharDevice) != 0
}()

func clr(code, text string) string {
	if !useColor {
		return text
	}

	return code + text + clrRst
}
