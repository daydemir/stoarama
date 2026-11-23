package api

import "time"

// Keep28Window is a read-only view of one stream's pinned 20-day cleanup
// window, for tools outside this package (stoaramactl r2-retire, cleanup
// preparation). Start and End are the UTC instants of the local midnights that
// bound [keep_start, keep_end_excl). The type name is historical: it is the
// widely used window shape, not a claim that the window is 28 days.
type Keep28Window struct {
	StreamID  int64
	Timezone  string
	BestStart string
	Start     time.Time
	End       time.Time
	// KeepEverything pins the whole stream as KEEP; it is never eligible.
	KeepEverything bool
}

// CleanupWindowPolicy returns the active 20-day cleanup selection exactly as
// the NAS cleanup validates it (pinned SHA, 109 streams, best14 shifted to a
// 20 local day window, keep-everything streams marked). Any validation failure
// is returned; callers must then keep everything.
func CleanupWindowPolicy() (sha string, windows map[int64]Keep28Window, err error) {
	p, err := cleanupEmbeddedPolicy()
	if err != nil {
		return "", nil, err
	}
	return p.SHA, exportCleanupWindows(p), nil
}

// Keep28Policy retains the superseded 28-day selection for lanes with a
// historical binding: the NAS delivery retirement fence, R2 copy
// fallback/refresh, keep28 retention, native stitch plans and the read-only
// backlog. It is exact and unchanged, and must never admit new cleanup work.
func Keep28Policy() (sha string, windows map[int64]Keep28Window, err error) {
	p, err := cleanupEmbeddedKeep28Policy()
	if err != nil {
		return "", nil, err
	}
	return p.SHA, exportCleanupWindows(p), nil
}

func exportCleanupWindows(p *nasCleanupPolicy) map[int64]Keep28Window {
	windows := make(map[int64]Keep28Window, len(p.Streams))
	for id, w := range p.Streams {
		windows[id] = Keep28Window{StreamID: id, Timezone: w.Timezone, BestStart: w.BestStart, Start: w.start, End: w.end, KeepEverything: w.KeepEverything}
	}
	return windows
}
