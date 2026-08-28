package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRequestedRecordingEnrichmentIDsArePositiveUniqueAndBounded(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		want    []int64
		wantErr bool
	}{
		{name: "absent", query: "", want: nil},
		{name: "ordered batch", query: "recording_ids=9,7,8", want: []int64{9, 7, 8}},
		{name: "duplicate", query: "recording_ids=9,9", wantErr: true},
		{name: "zero", query: "recording_ids=0", wantErr: true},
		{name: "invalid", query: "recording_ids=9,nope", wantErr: true},
		{name: "over limit", query: "recording_ids=1,2,3,4,5,6,7,8,9,10,11,12,13", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/recordings/enrichment?"+test.query, nil)
			got, err := requestedRecordingEnrichmentIDs(req)
			if (err != nil) != test.wantErr {
				t.Fatalf("error=%v wantErr=%t", err, test.wantErr)
			}
			if test.wantErr {
				return
			}
			if len(got) != len(test.want) {
				t.Fatalf("ids=%v want %v", got, test.want)
			}
			for index := range got {
				if got[index] != test.want[index] {
					t.Fatalf("ids=%v want %v", got, test.want)
				}
			}
		})
	}
}

func TestRecordingEnrichmentRejectsCombinedSingleAndBatchScopes(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/recordings/enrichment?recording_id=9&recording_ids=9", nil)
	rec := httptest.NewRecorder()
	(&Server{}).handleSharedRecordingListEnrichment(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestRecordingMetricCacheBoundsConcurrentLazyDatabaseWork(t *testing.T) {
	var cache recordingMetricCache[int64]
	slots := make(chan struct{}, recordingMetricConcurrency)
	loaderStarted := make(chan struct{}, 8)
	release := make(chan struct{})
	var calls atomic.Int64
	var active atomic.Int64
	var maximum atomic.Int64
	loader := func(value int64) func(context.Context) (int64, error) {
		return func(ctx context.Context) (int64, error) {
			calls.Add(1)
			current := active.Add(1)
			for {
				previous := maximum.Load()
				if current <= previous || maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			loaderStarted <- struct{}{}
			defer active.Add(-1)
			select {
			case <-release:
				return value, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
	}

	type result struct {
		value int64
		err   error
	}
	const sameKeyCallers = 12
	results := make(chan result, sameKeyCallers+2)
	var wg sync.WaitGroup
	sameKey := recordingMetricCacheKey{AccountID: 47, Shared: true, Scope: "700"}
	for range sameKeyCallers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := loadRecordingMetricCached(context.Background(), &cache, sameKey, time.Minute, time.Minute, time.Second, slots, loader(47))
			results <- result{value: value, err: err}
		}()
	}
	<-loaderStarted
	time.Sleep(20 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("same-key loader calls=%d want 1", got)
	}

	foreignKey := recordingMetricCacheKey{AccountID: 99, Shared: true, Scope: "700"}
	wg.Add(1)
	go func() {
		defer wg.Done()
		value, err := loadRecordingMetricCached(context.Background(), &cache, foreignKey, time.Minute, time.Minute, time.Second, slots, loader(99))
		results <- result{value: value, err: err}
	}()
	<-loaderStarted
	thirdKey := recordingMetricCacheKey{AccountID: 47, Shared: false, Scope: "701"}
	wg.Add(1)
	go func() {
		defer wg.Done()
		value, err := loadRecordingMetricCached(context.Background(), &cache, thirdKey, time.Minute, time.Minute, time.Second, slots, loader(701))
		results <- result{value: value, err: err}
	}()
	time.Sleep(20 * time.Millisecond)
	if got := calls.Load(); got != 2 {
		t.Fatalf("loader calls before release=%d want 2; third key escaped the concurrency bound", got)
	}
	if got := maximum.Load(); got > recordingMetricConcurrency {
		t.Fatalf("maximum concurrent loaders=%d want <=%d", got, recordingMetricConcurrency)
	}

	close(release)
	wg.Wait()
	close(results)
	seen47, seen99, seen701 := 0, 0, 0
	for got := range results {
		if got.err != nil {
			t.Fatal(got.err)
		}
		switch got.value {
		case 47:
			seen47++
		case 99:
			seen99++
		case 701:
			seen701++
		default:
			t.Fatalf("unexpected cached value %d", got.value)
		}
	}
	if seen47 != sameKeyCallers || seen99 != 1 || seen701 != 1 {
		t.Fatalf("values account47=%d account99=%d third=%d", seen47, seen99, seen701)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("total loader calls=%d want 3", got)
	}

	value, err := loadRecordingMetricCached(context.Background(), &cache, sameKey, time.Minute, time.Minute, time.Second, slots, loader(-1))
	if err != nil || value != 47 || calls.Load() != 3 {
		t.Fatalf("cached reload value=%d err=%v calls=%d", value, err, calls.Load())
	}
}

func TestRecordingMetricCacheTimeoutIncludesAdmissionWait(t *testing.T) {
	var cache recordingMetricCache[int]
	slots := make(chan struct{}, 1)
	slots <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	called := false
	_, err := loadRecordingMetricCached(ctx, &cache, recordingMetricCacheKey{AccountID: 47, Scope: "700"}, time.Minute, time.Minute, time.Second, slots, func(context.Context) (int, error) {
		called = true
		return 1, nil
	})
	if err != context.DeadlineExceeded {
		t.Fatalf("error=%v want deadline exceeded", err)
	}
	if called {
		t.Fatal("loader ran without metric admission")
	}
	<-slots
	_, err = loadRecordingMetricCached(context.Background(), &cache, recordingMetricCacheKey{AccountID: 47, Scope: "700"}, time.Minute, time.Minute, time.Second, slots, func(context.Context) (int, error) {
		called = true
		return 1, nil
	})
	if err != context.DeadlineExceeded || called {
		t.Fatalf("brief failure cache error=%v loader_called=%t", err, called)
	}
}

func TestRecordingMetricFlightSurvivesLeaderDisconnect(t *testing.T) {
	var cache recordingMetricCache[int]
	slots := make(chan struct{}, 1)
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	loader := func(ctx context.Context) (int, error) {
		calls.Add(1)
		close(started)
		select {
		case <-release:
			return 47, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	key := recordingMetricCacheKey{AccountID: 47, Shared: true, Scope: "700"}
	type result struct {
		value int
		err   error
	}
	leaderResult := make(chan result, 1)
	leaderCtx, cancelLeader := context.WithTimeout(context.Background(), time.Second)
	go func() {
		value, err := loadRecordingMetricCached(leaderCtx, &cache, key, time.Second, time.Minute, time.Second, slots, loader)
		leaderResult <- result{value: value, err: err}
	}()
	<-started
	followerResult := make(chan result, 1)
	go func() {
		value, err := loadRecordingMetricCached(context.Background(), &cache, key, time.Second, time.Minute, time.Second, slots, loader)
		followerResult <- result{value: value, err: err}
	}()
	cancelLeader()
	if got := <-leaderResult; got.err != context.Canceled {
		t.Fatalf("leader error=%v want canceled", got.err)
	}
	close(release)
	if got := <-followerResult; got.err != nil || got.value != 47 {
		t.Fatalf("follower value=%d err=%v", got.value, got.err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("shared loader calls=%d want 1", got)
	}
	value, err := loadRecordingMetricCached(context.Background(), &cache, key, time.Second, time.Minute, time.Second, slots, loader)
	if err != nil || value != 47 || calls.Load() != 1 {
		t.Fatalf("cached value=%d err=%v calls=%d", value, err, calls.Load())
	}
}
