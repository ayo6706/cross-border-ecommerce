package main

import (
	"errors"
	"strconv"
	"testing"
)

func TestParseCommand(t *testing.T) {
	valid := map[string]struct {
		args []string
		want command
	}{
		"up":             {[]string{"up"}, command{name: "up"}},
		"version":        {[]string{"version"}, command{name: "version"}},
		"down defaults":  {[]string{"down"}, command{name: "down", steps: 1}},
		"down with step": {[]string{"down", "3"}, command{name: "down", steps: 3}},
	}
	for name, tc := range valid {
		got, err := parseCommand(tc.args)
		if err != nil || got != tc.want {
			t.Errorf("%s: parseCommand(%v) = %+v, %v; want %+v, nil", name, tc.args, got, err, tc.want)
		}
	}

	for _, args := range [][]string{nil, {"sideways"}, {"down", "0"}, {"down", "-2"}} {
		if _, err := parseCommand(args); err == nil {
			t.Errorf("parseCommand(%v) = nil error; want an error before connecting", args)
		}
	}
}

// The strconv error is wrapped, not replaced by a message that hides it.
func TestParseCommand_KeepsStepParseError(t *testing.T) {
	_, err := parseCommand([]string{"down", "two"})
	if !errors.Is(err, strconv.ErrSyntax) {
		t.Fatalf("error = %v; want it to wrap strconv.ErrSyntax", err)
	}
}
