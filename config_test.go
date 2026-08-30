package main

import "testing"

func TestKeepGoingComesOnlyFromIXEnv(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  bool
	}{
		{"", false},
		{"no", false},
		{"YES", false},
		{"yes", true},
	} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("IX_KEEP_GOING", tc.value)
			c := &Config{}
			overlayFromEnv(c)

			if c.KeepGoing != tc.want {
				t.Fatalf("IX_KEEP_GOING=%q: KeepGoing=%v, want %v", tc.value, c.KeepGoing, tc.want)
			}
		})
	}
}

func TestKeepGoingFlagWasRemoved(t *testing.T) {
	exc := Try(func() {
		parseCLI([]string{"-k"})
	})

	if exc == nil {
		t.Fatal("legacy -k flag was accepted")
	}
}
