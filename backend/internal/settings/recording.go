package settings

const (
	DefaultRecordingIntervalSec = 1
	DefaultClipDurationSec      = 30
	ExtendedClipDurationSec     = 90
	DefaultSampleIntervalMinSec = 4 * 60
	DefaultSampleIntervalMaxSec = 8 * 60
	DefaultSampleStaleGraceSec  = 5 * 60
	DefaultSampleStaleWindowSec = DefaultSampleIntervalMaxSec + DefaultSampleStaleGraceSec
)
