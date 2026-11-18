package r2

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestListPrefixContinuationPreservesR2OrderThroughRetry(t *testing.T) {
	const token = "opaque+/=token"
	const sig = "relay-releases/latest-05739dfe.json.sig"
	const manifest = "relay-releases/latest-05739dfe.json"
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls++
		q := req.URL.Query()
		if req.Method != http.MethodGet || q.Get("list-type") != "2" || q.Get("prefix") != "relay-releases/" || q.Has("start-after") {
			t.Errorf("unexpected listing request: %s %s", req.Method, req.URL)
		}
		switch calls {
		case 1:
			if q.Has("continuation-token") {
				t.Error("first request has a continuation token")
			}
			fmt.Fprintf(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Contents><Key>%s</Key><Size>89</Size></Contents><IsTruncated>true</IsTruncated><NextContinuationToken>%s</NextContinuationToken></ListBucketResult>`, sig, token)
		case 2, 3:
			if q.Get("continuation-token") != token {
				t.Errorf("continuation token changed: %q", q.Get("continuation-token"))
			}
			if calls == 2 {
				w.WriteHeader(http.StatusInternalServerError)
				io.WriteString(w, `<Error><Code>InternalError</Code></Error>`)
				return
			}
			fmt.Fprintf(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Contents><Key>%s</Key><Size>3661</Size></Contents><IsTruncated>false</IsTruncated></ListBucketResult>`, manifest)
		default:
			t.Error("unexpected extra page")
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	client, err := New(context.Background(), Config{Bucket: "bucket", AccessKey: "key", SecretKey: "secret", Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	var objects []ObjectInfo
	err = client.ListPrefix(context.Background(), "relay-releases/", 0, func(o ObjectInfo) error {
		objects = append(objects, o)
		return nil
	})
	if err != nil || calls != 3 || !reflect.DeepEqual(objects, []ObjectInfo{{Key: sig, SizeBytes: 89}, {Key: manifest, SizeBytes: 3661}}) {
		t.Fatalf("calls=%d objects=%+v error=%v", calls, objects, err)
	}
}

func TestListingsRefuseNonAdvancingContinuation(t *testing.T) {
	for _, roots := range []bool{false, true} {
		for _, next := range []string{"", "<NextContinuationToken></NextContinuationToken>", "<NextContinuationToken>same</NextContinuationToken>"} {
			t.Run(fmt.Sprintf("roots=%t/next=%s", roots, next), func(t *testing.T) {
				calls := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					calls++
					if calls == 1 {
						io.WriteString(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>same</NextContinuationToken></ListBucketResult>`)
						return
					}
					io.WriteString(w, `<ListBucketResult><IsTruncated>true</IsTruncated>`+next+`</ListBucketResult>`)
				}))
				defer server.Close()
				client, err := New(context.Background(), Config{Bucket: "bucket", AccessKey: "key", SecretKey: "secret", Endpoint: server.URL})
				if err != nil {
					t.Fatal(err)
				}
				if roots {
					_, _, err = client.ListRoots(context.Background())
				} else {
					err = client.ListPrefix(context.Background(), "prefix", 0, func(ObjectInfo) error { return nil })
				}
				if err == nil || !strings.Contains(err.Error(), "no advancing continuation token") || calls != 2 {
					t.Fatalf("calls=%d error=%v", calls, err)
				}
			})
		}
	}
}
