package main

import (
	"strings"
	"testing"
)

func TestParseNASSelectedVerifyRequestArgs(t *testing.T) {
	opts, err := parseNASSelectedVerifyRequestArgs([]string{"--clip-ids", "3,1", "--label", "chosen-28", "--expected-plan-sha256", strings.Repeat("a", 64)})
	if err != nil || len(opts.clipIDs) != 2 || opts.clipIDs[0] != 3 || opts.label != "chosen-28" {
		t.Fatalf("opts=%+v err=%v", opts, err)
	}
	for _, args := range [][]string{nil, {"--clip-ids", "1"}, {"--clip-ids", "x", "--label", "x"}, {"--clip-ids", "1", "--label", "Bad Label"}, {"--clip-ids", "1", "--label", "x", "--expected-plan-sha256", "bad"}} {
		if _, err := parseNASSelectedVerifyRequestArgs(args); err == nil {
			t.Errorf("accepted %v", args)
		}
	}
}
