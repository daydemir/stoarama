package model

import "testing"

func TestIsKoreaRecordingProvider(t *testing.T) {
	for _, provider := range []string{"KBS", "SPATIC", "TOPIS", "GIGAEYES", " gigaeyes "} {
		if !IsKoreaRecordingProvider(provider) {
			t.Fatalf("provider %q should be preserved", provider)
		}
	}
	for _, provider := range []string{"YOUTUBE", "EU_SQUARES", "SDOT", "BELLEVUE_TRAFFICMAP", ""} {
		if IsKoreaRecordingProvider(provider) {
			t.Fatalf("provider %q should not be preserved", provider)
		}
	}
}

func TestStreamRequiresRelay(t *testing.T) {
	if !StreamRequiresRelay(" sdot ", "") {
		t.Fatal("SDOT should require relay")
	}
	if !StreamRequiresRelay("global-street-scores", "https://61e0c5d388c2e.streamlock.net/live/2_James_EW.stream/playlist.m3u8") {
		t.Fatal("Seattle stream host should require relay")
	}
	if StreamRequiresRelay("SPATIC", "https://example.com/live.m3u8") {
		t.Fatal("SPATIC should not require relay")
	}
}

func TestParseArchiveProvider(t *testing.T) {
	got, ok := ParseArchiveProvider(" AWS_S3 ")
	if !ok || got != ArchiveProviderAWSS3 {
		t.Fatalf("provider=%q ok=%t want aws_s3", got, ok)
	}
	if _, ok := ParseArchiveProvider("s3"); ok {
		t.Fatalf("loose archive provider should be rejected")
	}
}

func TestParseArchiveStatus(t *testing.T) {
	got, ok := ParseArchiveStatus(" SOURCE_DELETED ")
	if !ok || got != ArchiveStatusSourceDeleted {
		t.Fatalf("status=%q ok=%t want source_deleted", got, ok)
	}
	if !IsSourceDeletedArchiveStatus("source_deleted") {
		t.Fatalf("source_deleted should be source deleted")
	}
	if IsSourceDeletedArchiveStatus("verified") {
		t.Fatalf("verified should not be source deleted")
	}
	if _, ok := ParseArchiveStatus("deleted"); ok {
		t.Fatalf("loose archive status should be rejected")
	}
}

func TestParseArchiveStorageClass(t *testing.T) {
	got, ok := ParseArchiveStorageClass(" deep_archive ")
	if !ok || got != ArchiveStorageClassDeepArchive {
		t.Fatalf("storage class=%q ok=%t want DEEP_ARCHIVE", got, ok)
	}
	if _, ok := ParseArchiveStorageClass("glacier"); ok {
		t.Fatalf("loose storage class should be rejected")
	}
}
