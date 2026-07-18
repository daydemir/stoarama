package api

import "testing"

func TestUniqueBatchStreamIDs(t *testing.T) {
	ids, err := uniqueBatchStreamIDs([]int64{9, 2, 5})
	if err != nil || len(ids) != 3 || ids[0] != 2 || ids[2] != 9 {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
	for _, bad := range [][]int64{{}, {1, 1}, {0}} {
		if _, err := uniqueBatchStreamIDs(bad); err == nil {
			t.Fatalf("accepted %v", bad)
		}
	}
	tooMany := make([]int64, 51)
	for i := range tooMany {
		tooMany[i] = int64(i + 1)
	}
	if _, err := uniqueBatchStreamIDs(tooMany); err == nil {
		t.Fatal("accepted 51 streams")
	}
}
