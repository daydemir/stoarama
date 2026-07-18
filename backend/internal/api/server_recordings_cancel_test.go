package api

import "testing"

func TestUniqueRecordingIDs(t *testing.T) {
	ids, err := uniqueRecordingIDs([]int64{3, 1, 2})
	if err != nil || len(ids) != 3 || ids[0] != 3 {
		t.Fatalf("unexpected result: %v, %v", ids, err)
	}
	for _, invalid := range [][]int64{nil, {0}, {1, 1}, make([]int64, 51)} {
		if _, err := uniqueRecordingIDs(invalid); err == nil {
			t.Fatalf("expected error for %v", invalid)
		}
	}
}
