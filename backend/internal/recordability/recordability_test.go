package recordability

import "testing"

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		obs  Observation
		want string
	}{
		{
			name: "clean full window is ok",
			obs:  Observation{Started: true, ValidRatio: 1.0},
			want: ResultOK,
		},
		{
			name: "mid-window reconnect gap (550/600=0.917) stays ok",
			obs:  Observation{Started: true, ValidRatio: 0.917},
			want: ResultOK,
		},
		{
			name: "exactly 0.9 is ok (boundary inclusive)",
			obs:  Observation{Started: true, ValidRatio: 0.9},
			want: ResultOK,
		},
		{
			name: "started then killed by connection reset is blocked",
			obs:  Observation{Started: true, ValidRatio: 0.067, FFmpegErr: "continuous ffmpeg exited: exit status 1 (Connection reset by peer)"},
			want: ResultBlocked,
		},
		{
			name: "started then TLS error mid-stream is blocked",
			obs:  Observation{Started: true, ValidRatio: 0.2, FFmpegErr: "error: SSL error: ... while reading"},
			want: ResultBlocked,
		},
		{
			name: "started but origin 503 is source_unstable not blocked",
			obs:  Observation{Started: true, ValidRatio: 0.3, FFmpegErr: "Server returned 503 Service Unavailable"},
			want: ResultSourceUnstable,
		},
		{
			name: "never connected (connection refused) is source_unstable",
			obs:  Observation{Started: false, ValidRatio: 0.0, FFmpegErr: "Connection refused"},
			want: ResultSourceUnstable,
		},
		{
			name: "started low ratio with no signature is source_unstable (conservative, not blocked)",
			obs:  Observation{Started: true, ValidRatio: 0.4, FFmpegErr: ""},
			want: ResultSourceUnstable,
		},
		{
			name: "not started with network-cut signature is NOT blocked (never delivered)",
			obs:  Observation{Started: false, ValidRatio: 0.0, FFmpegErr: "connection reset by peer"},
			want: ResultSourceUnstable,
		},
		{
			name: "our-side error is inconclusive even with low ratio",
			obs:  Observation{OurErr: "resolve error", Started: false, ValidRatio: 0.0},
			want: ResultInconclusive,
		},
		{
			name: "our-side error wins over an otherwise-ok ratio",
			obs:  Observation{OurErr: "probe context cancelled", ValidRatio: 1.0},
			want: ResultInconclusive,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.obs); got != tc.want {
				t.Fatalf("Classify(%+v) = %q, want %q", tc.obs, got, tc.want)
			}
		})
	}
}

func TestClassifySignatureSourceDownBeatsNetworkCut(t *testing.T) {
	// A refused/5xx origin that ALSO logs a socket reset while giving up must be
	// read as source-side (conservative: do not flag a provider off an origin blip).
	err := "Server returned 503 Service Unavailable; connection reset by peer"
	if got := classifySignature(err); got != sigSourceDown {
		t.Fatalf("classifySignature(%q) = %d, want sigSourceDown", err, got)
	}
}

func TestRouteNeedsRelay(t *testing.T) {
	cases := []struct {
		name          string
		streamResult  string
		streamHasRow  bool
		providerRelay bool
		want          bool
	}{
		{"empty tables -> cloud (inert dark default)", "", false, false, false},
		{"stream proven blocked -> relay", ResultBlocked, true, false, true},
		{"stream proven ok -> cloud even if provider flagged", ResultOK, true, true, false},
		{"stream proven blocked -> relay even if provider not flagged", ResultBlocked, true, false, true},
		{"untested stream, provider flagged -> relay", "", false, true, true},
		{"untested stream, provider clean -> cloud", "", false, false, false},
		{"transient stream row (source_unstable), provider flagged -> follow provider", ResultSourceUnstable, true, true, true},
		{"transient stream row (inconclusive), provider clean -> cloud", ResultInconclusive, true, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := RouteNeedsRelay(tc.streamResult, tc.streamHasRow, tc.providerRelay); got != tc.want {
				t.Fatalf("RouteNeedsRelay(%q,%t,%t) = %t, want %t", tc.streamResult, tc.streamHasRow, tc.providerRelay, got, tc.want)
			}
		})
	}
}
