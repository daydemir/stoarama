package api

import "time"

// Keep28Window is a read-only view of one stream's pinned keep28 window, for
// tools outside this package (stoaramactl r2-retire). Start and End are the
// UTC instants of the local midnights that bound [keep_start, keep_end_excl).
type Keep28Window struct {
	StreamID  int64
	Timezone  string
	BestStart string
	Start     time.Time
	End       time.Time
}

// Keep28Policy returns the embedded keep28 selection exactly as the NAS cleanup
// validates it (pinned SHA, 109 streams, 28 local days around best14). Any
// validation failure is returned; callers must then keep everything.
func Keep28Policy() (sha string, windows map[int64]Keep28Window, err error) {
	p, err := cleanupEmbeddedPolicy()
	if err != nil {
		return "", nil, err
	}
	windows = make(map[int64]Keep28Window, len(p.Streams))
	for id, w := range p.Streams {
		windows[id] = Keep28Window{StreamID: id, Timezone: w.Timezone, BestStart: w.BestStart, Start: w.start, End: w.end}
	}
	return p.SHA, windows, nil
}
