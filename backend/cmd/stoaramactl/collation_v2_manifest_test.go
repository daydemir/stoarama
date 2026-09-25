package main

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/smithy-go"
)

func TestFetchCollationManifestRace(t *testing.T) {
	notFound := &smithy.GenericAPIError{Code: "NoSuchKey"}
	boom := errors.New("connection reset")
	ctx := context.Background()
	for name, tc := range map[string]struct {
		gets      []error
		exists    bool
		existsErr error
		wantFound bool
		wantErr   bool
		wantGetsN int
	}{
		"present":                 {gets: []error{nil}, wantFound: true, wantGetsN: 1},
		"absent":                  {gets: []error{notFound}, exists: false, wantGetsN: 1},
		"appears after 404":       {gets: []error{notFound, nil}, exists: true, wantFound: true, wantGetsN: 2},
		"still 404 after head":    {gets: []error{notFound, notFound}, exists: true, wantGetsN: 2},
		"transient then present":  {gets: []error{boom, nil}, exists: true, wantFound: true, wantGetsN: 2},
		"persistent read failure": {gets: []error{boom, boom}, exists: true, wantErr: true, wantGetsN: 2},
		"exists check fails":      {gets: []error{notFound}, existsErr: boom, wantErr: true, wantGetsN: 1},
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			get := func(context.Context, string) ([]byte, error) {
				err := tc.gets[calls]
				calls++
				if err != nil {
					return nil, err
				}
				return []byte(`{}`), nil
			}
			exists := func(context.Context, string) (bool, error) { return tc.exists, tc.existsErr }
			body, found, err := fetchCollationManifest(ctx, "k", get, exists)
			if (err != nil) != tc.wantErr || found != tc.wantFound || calls != tc.wantGetsN {
				t.Fatalf("found=%v err=%v gets=%d, want found=%v err=%v gets=%d", found, err, calls, tc.wantFound, tc.wantErr, tc.wantGetsN)
			}
			if found && string(body) != `{}` {
				t.Fatalf("body=%q", body)
			}
		})
	}
}
