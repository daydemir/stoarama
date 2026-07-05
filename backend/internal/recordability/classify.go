package recordability

import "strings"

// Probe result classes. Persisted in stream_recordability.result.
const (
	ResultOK             = "ok"
	ResultBlocked        = "blocked"
	ResultSourceUnstable = "source_unstable"
	ResultInconclusive   = "inconclusive"
)

// okThreshold is the minimum fraction of the ~600s window that must be decodable
// video for a stream to count as recordable. A single mid-window source drop that
// ffmpeg reconnects across (e.g. Seattle ~50s: 550/600 = 0.917) stays above it, so
// a recoverable transient does not push a recordable stream into blocked territory.
const okThreshold = 0.9

// Observation is everything the classifier needs, gathered by ProbeStream. It is a
// plain value so the classifier is pure and unit-tested without ffmpeg or a DB.
type Observation struct {
	// OurErr is a non-empty message when the failure is OUR side (resolve error,
	// image source, SSRF/guard reject, ffmpeg missing, ctx cancel, our timeout,
	// panic). Our-side failures are inconclusive and never flag a provider.
	OurErr string
	// Started is true when at least one segment carried decodable video (>0s).
	Started bool
	// ValidRatio is TOTAL decodable video seconds / windowSeconds. It is a sum of
	// every valid segment's duration (not the longest contiguous run), so a single
	// reconnect gap only subtracts that gap's seconds rather than tanking the ratio.
	ValidRatio float64
	// FFmpegErr is the error text ffmpeg exited with (empty when the probe ran to
	// our window deadline and ffmpeg was stopped cleanly). Its signature is what
	// discriminates a datacenter block from a source-side outage.
	FFmpegErr string
}

// signature classifies the ffmpeg exit error text.
type signature int

const (
	sigNone       signature = iota // clean / no error text
	sigNetworkCut                  // connection reset / TLS / mid-stream 4xx-after-200 / unexpected EOF while delivering
	sigSourceDown                  // origin never came up / origin 5xx / DNS / refused / 404
)

// networkCutMarkers are stderr fragments that indicate the far side (our
// datacenter IP's connection) was CUT while delivering: the exact "resolves fine
// then dies after ~40s" datacenter-block pattern. Lowercased match.
var networkCutMarkers = []string{
	"connection reset by peer",
	"connection reset",
	"reset by peer",
	"closed by peer",
	"broken pipe",
	"error in the pull function",
	"ssl error",
	"tls error",
	"sslv3",
	"decryption failed or bad record mac",
}

// sourceDownMarkers indicate a SOURCE-side problem (origin down/flaky), NOT our
// IP: the origin refused, timed out at open, or returned a 5xx / 404. These are
// re-probeable (source_unstable), never provider-flagging. Lowercased match.
var sourceDownMarkers = []string{
	"connection refused",
	"connection timed out",
	"name or service not known",
	"no route to host",
	"failed to resolve hostname",
	"temporary failure in name resolution",
	"404 not found",
	"500 internal server error",
	"502 bad gateway",
	"503 service unavailable",
	"504 gateway timeout",
	"server returned 5",
}

// classifySignature inspects ffmpeg stderr. Source-down markers win over
// network-cut markers when both appear (a refused/5xx origin is a source
// problem even if ffmpeg also logs a socket reset while giving up), keeping the
// block verdict conservative.
func classifySignature(ffmpegErr string) signature {
	s := strings.ToLower(ffmpegErr)
	if strings.TrimSpace(s) == "" {
		return sigNone
	}
	for _, m := range sourceDownMarkers {
		if strings.Contains(s, m) {
			return sigSourceDown
		}
	}
	for _, m := range networkCutMarkers {
		if strings.Contains(s, m) {
			return sigNetworkCut
		}
	}
	return sigNone
}

// Classify maps an Observation to a result class. Fail-fast, first match wins.
//
//   - inconclusive: our-side failure (never flags a provider, re-probed later).
//   - ok:           valid_ratio >= 0.9 (proven recordable; sticky, never re-probed).
//   - blocked:      started AND ratio<0.9 AND a NETWORK-cut signature (datacenter
//     block; sticky + flags the provider).
//   - source_unstable: everything else that connected-but-degraded or never
//     connected without a network-cut signature (origin problem; re-probed later).
func Classify(o Observation) string {
	if strings.TrimSpace(o.OurErr) != "" {
		return ResultInconclusive
	}
	if o.ValidRatio >= okThreshold {
		return ResultOK
	}
	if o.Started && classifySignature(o.FFmpegErr) == sigNetworkCut {
		return ResultBlocked
	}
	return ResultSourceUnstable
}
