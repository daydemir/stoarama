package capture

import (
	"reflect"
	"testing"
)

func TestYTDLPResolveArgs(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Setenv("YT_DLP_FORMAT", "")
		t.Setenv("YT_DLP_FORMAT_SORT", "")
		got := ytDLPResolveArgs("https://www.youtube.com/watch?v=abc123")
		want := []string{"-g", "--no-warnings", "--no-playlist", "https://www.youtube.com/watch?v=abc123"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("ytDLPResolveArgs()=%v want=%v", got, want)
		}
	})

	t.Run("format and sort", func(t *testing.T) {
		t.Setenv("YT_DLP_FORMAT", "bestvideo[vcodec^=avc1]/bestvideo/best")
		t.Setenv("YT_DLP_FORMAT_SORT", "res,fps")
		got := ytDLPResolveArgs("https://www.youtube.com/watch?v=abc123")
		want := []string{
			"-g", "--no-warnings", "--no-playlist",
			"-f", "bestvideo[vcodec^=avc1]/bestvideo/best",
			"-S", "res,fps",
			"https://www.youtube.com/watch?v=abc123",
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("ytDLPResolveArgs()=%v want=%v", got, want)
		}
	})
}
