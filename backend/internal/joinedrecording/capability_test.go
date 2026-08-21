package joinedrecording

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type memoryCapabilityClient struct{ objects map[string][]byte }

func (*memoryCapabilityClient) joinedRedirectSafe() {}

type errorCapabilityClient struct{ err error }

func (*errorCapabilityClient) joinedRedirectSafe()                        {}
func (c *errorCapabilityClient) Do(*http.Request) (*http.Response, error) { return nil, c.err }

func (c *memoryCapabilityClient) Do(request *http.Request) (*http.Response, error) {
	key := request.URL.Query().Get("key")
	if strings.ToLower(request.Method) != request.URL.Query().Get("op") || key == "" {
		return capabilityResponse(http.StatusForbidden, nil, "", ""), nil
	}
	switch request.Method {
	case http.MethodPut:
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(body)
		if request.Header.Get("If-None-Match") != "*" || request.ContentLength != int64(len(body)) || request.URL.Query().Get("X-Amz-Checksum-Sha256") != base64.StdEncoding.EncodeToString(sum[:]) {
			return capabilityResponse(http.StatusForbidden, nil, "", ""), nil
		}
		if _, exists := c.objects[key]; exists {
			return capabilityResponse(http.StatusPreconditionFailed, nil, "", ""), nil
		}
		c.objects[key] = append([]byte(nil), body...)
		return capabilityResponse(http.StatusOK, nil, objectETag(body), "version"), nil
	case http.MethodHead, http.MethodGet:
		body, exists := c.objects[key]
		if !exists || responseETag(request.Header.Get("If-Match")) != objectETag(body) {
			return capabilityResponse(http.StatusPreconditionFailed, nil, "", ""), nil
		}
		if request.Method == http.MethodHead {
			return capabilityResponse(http.StatusOK, make([]byte, len(body)), objectETag(body), "version"), nil
		}
		return capabilityResponse(http.StatusOK, body, objectETag(body), "version"), nil
	default:
		return capabilityResponse(http.StatusMethodNotAllowed, nil, "", ""), nil
	}
}

func objectETag(body []byte) string {
	sum := sha256.Sum256(body)
	return "etag-" + hex.EncodeToString(sum[:4])
}

func capabilityResponse(status int, body []byte, etag, version string) *http.Response {
	header := make(http.Header)
	header.Set("Content-Length", strconv.Itoa(len(body)))
	if etag != "" {
		header.Set("ETag", `"`+etag+`"`)
	}
	if version != "" {
		header.Set("x-amz-version-id", version)
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body))}
}

func signedRequest(method, bucket, key string, headers map[string]string, version string, lifetime time.Duration) SignedRequest {
	signedAt := time.Now().UTC().Truncate(time.Second)
	values := url.Values{"key": {key}, "op": {strings.ToLower(method)}, "X-Amz-Date": {signedAt.Format("20060102T150405Z")}, "X-Amz-Expires": {strconv.FormatInt(int64(lifetime/time.Second), 10)}}
	if version != "" {
		values.Set("versionId", version)
	}
	parsed := &url.URL{Scheme: "https", Host: testSourceAuthority, Path: "/" + bucket + "/" + key, RawQuery: values.Encode()}
	return SignedRequest{Method: method, URL: parsed.String(), Scheme: "https", Authority: testSourceAuthority, EscapedPath: parsed.EscapedPath(), RawQuery: parsed.RawQuery, RequiredHeaders: headers}
}

func createCapability(id int64, bucket, key, contentType string, body []byte) ObjectCreateCapability {
	sum := sha256.Sum256(body)
	return createCapabilityIdentity(id, bucket, key, contentType, int64(len(body)), hex.EncodeToString(sum[:]))
}

func createCapabilityIdentity(id int64, bucket, key, contentType string, size int64, sha string) ObjectCreateCapability {
	headers := map[string]string{"Content-Length": strconv.FormatInt(size, 10), "Content-Type": contentType, "If-None-Match": "*"}
	request := signedRequest(http.MethodPut, bucket, key, headers, "", maxPutCapabilityLifetime)
	parsed, _ := url.Parse(request.URL)
	query := parsed.Query()
	query.Set("X-Amz-Checksum-Sha256", base64.StdEncoding.EncodeToString(mustDecodeHex(sha)))
	parsed.RawQuery = query.Encode()
	request.URL, request.RawQuery = parsed.String(), parsed.RawQuery
	expiresAt, _ := signedRequestExpiry(request)
	return ObjectCreateCapability{ProtocolVersion: JoinedProtocolVersion, ArtifactID: id, ObjectKey: key, ContentType: contentType, SizeBytes: size, SHA256: sha, ExpiresAt: expiresAt, Request: request}
}

func sourceReadCapability(key, etag, version, operation string) SourceReadCapability {
	headers := map[string]string{"If-Match": quotedETag(etag)}
	method := map[string]string{"head": http.MethodHead, "get": http.MethodGet}[operation]
	request := signedRequest(method, "recordings", key, headers, version, 4*time.Minute)
	expiresAt, _ := signedRequestExpiry(request)
	return SourceReadCapability{ProtocolVersion: JoinedProtocolVersion, Operation: operation, ObjectKey: key, SizeBytes: 20, SHA256: strings.Repeat("a", 64), ETag: etag, VersionID: version, ExpiresAt: expiresAt, Request: request}
}

func readCapability(id int64, bucket, key string, body []byte) ObjectReadCapability {
	sum := sha256.Sum256(body)
	etag := objectETag(body)
	headers := map[string]string{"If-Match": quotedETag(etag)}
	request := signedRequest(http.MethodGet, bucket, key, headers, "version", 4*time.Minute)
	expiresAt, _ := signedRequestExpiry(request)
	return ObjectReadCapability{ProtocolVersion: JoinedProtocolVersion, ArtifactID: id, ObjectKey: key, SizeBytes: int64(len(body)), SHA256: hex.EncodeToString(sum[:]), ETag: etag, VersionID: "version", ExpiresAt: expiresAt, Request: request}
}

func signedRequestExpiry(request SignedRequest) (time.Time, error) {
	parsed, err := url.Parse(request.URL)
	if err != nil {
		return time.Time{}, err
	}
	signedAt, err := time.Parse("20060102T150405Z", parsed.Query().Get("X-Amz-Date"))
	if err != nil {
		return time.Time{}, err
	}
	seconds, err := strconv.ParseInt(parsed.Query().Get("X-Amz-Expires"), 10, 64)
	return signedAt.Add(time.Duration(seconds) * time.Second), err
}

func TestTwoStageCreateThenExactFullReread(t *testing.T) {
	body, bucket, key := []byte("expected"), "recordings", "joined/key.mp4"
	client := &memoryCapabilityClient{objects: map[string][]byte{}}
	create := createCapability(7, bucket, key, "video/mp4", body)
	observation, err := putCreateOnlyCapability(context.Background(), client, testSourceAuthority, bucket, 7, key, "video/mp4", int64(len(body)), create.SHA256, create, bytes.NewReader(body))
	if err != nil || !observation.Created || observation.ETag == "" {
		t.Fatalf("observation=%+v err=%v", observation, err)
	}
	read := readCapability(7, bucket, key, body)
	head, err := reconcileExactCapability(context.Background(), client, testSourceAuthority, bucket, 7, key, int64(len(body)), read.SHA256, read.ETag, read.VersionID, read)
	if err != nil || head.SizeBytes != int64(len(body)) {
		t.Fatalf("head=%+v err=%v", head, err)
	}
}

func TestCreateCapabilityRejectsCoordinateHeaderAndBodyMutation(t *testing.T) {
	body, bucket, key := []byte("expected"), "recordings", "joined/key.mp4"
	original := createCapability(7, bucket, key, "video/mp4", body)
	for _, mutate := range []func(*ObjectCreateCapability){
		func(c *ObjectCreateCapability) { c.ObjectKey += "-other" },
		func(c *ObjectCreateCapability) { c.ContentType = "text/plain" },
		func(c *ObjectCreateCapability) { c.SizeBytes++ },
		func(c *ObjectCreateCapability) { c.SHA256 = strings.Repeat("0", 64) },
		func(c *ObjectCreateCapability) { c.Request.Method = http.MethodDelete },
		func(c *ObjectCreateCapability) { c.Request.Scheme = "http" },
		func(c *ObjectCreateCapability) { c.Request.Authority = "evil.test" },
		func(c *ObjectCreateCapability) { c.Request.EscapedPath += "/other" },
		func(c *ObjectCreateCapability) { c.Request.RawQuery += "&changed=1" },
		func(c *ObjectCreateCapability) { delete(c.Request.RequiredHeaders, "If-None-Match") },
		func(c *ObjectCreateCapability) { c.Request.RequiredHeaders["If-None-Match"] = "changed" },
		func(c *ObjectCreateCapability) { c.Request.RequiredHeaders["Content-Length"] = "999" },
		func(c *ObjectCreateCapability) { c.Request.RequiredHeaders["Content-Type"] = "text/plain" },
		func(c *ObjectCreateCapability) { c.Request.RawQuery += "&X-Amz-Checksum-Sha256=changed" },
		func(c *ObjectCreateCapability) {
			parsed, _ := url.Parse(c.Request.URL)
			parsed.Scheme = "http"
			c.Request.URL = parsed.String()
		},
		func(c *ObjectCreateCapability) {
			parsed, _ := url.Parse(c.Request.URL)
			parsed.Host = "cap.test:444"
			c.Request.URL = parsed.String()
		},
		func(c *ObjectCreateCapability) {
			parsed, _ := url.Parse(c.Request.URL)
			parsed.User = url.User("attacker")
			c.Request.URL = parsed.String()
		},
		func(c *ObjectCreateCapability) {
			parsed, _ := url.Parse(c.Request.URL)
			parsed.Path += "/other"
			c.Request.URL = parsed.String()
		},
		func(c *ObjectCreateCapability) {
			parsed, _ := url.Parse(c.Request.URL)
			query := parsed.Query()
			query.Set("changed", "1")
			parsed.RawQuery = query.Encode()
			c.Request.URL = parsed.String()
		},
	} {
		capability := createCapability(7, bucket, key, "video/mp4", body)
		mutate(&capability)
		if _, err := putCreateOnlyCapability(context.Background(), &memoryCapabilityClient{objects: map[string][]byte{}}, testSourceAuthority, bucket, 7, key, "video/mp4", int64(len(body)), original.SHA256, capability, bytes.NewReader(body)); err == nil {
			t.Fatal("mutated capability succeeded")
		}
	}
	capability := createCapability(7, bucket, key, "video/mp4", body)
	if _, err := putCreateOnlyCapability(context.Background(), &memoryCapabilityClient{objects: map[string][]byte{}}, testSourceAuthority, bucket, 7, key, "video/mp4", int64(len(body)), capability.SHA256, capability, bytes.NewReader([]byte("altered!"))); err == nil {
		t.Fatal("altered body succeeded")
	}
}

func TestCapabilitiesRejectUnexpectedVersionAndSelfDescribedReconciliation(t *testing.T) {
	source := testSource(1, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC))
	capability := sourceReadCapability(source.Object.Key, source.Object.ETag, "surprise", "get")
	capability.SizeBytes, capability.SHA256 = source.Object.SizeBytes, source.Object.SHA256
	if capability.Validate(source, "get", testSourceAuthority, time.Now()) == nil {
		t.Fatal("unfrozen version query accepted")
	}
	capability = sourceReadCapability(source.Object.Key, source.Object.ETag, source.Object.VersionID, "get")
	capability.SizeBytes, capability.SHA256 = source.Object.SizeBytes, source.Object.SHA256
	wrongEndpoint := source
	wrongEndpoint.Endpoint = "https://other.test"
	if capability.Validate(wrongEndpoint, "get", testSourceAuthority, time.Now()) == nil {
		t.Fatal("capability authority was not bound to frozen endpoint")
	}
	body, bucket, key := []byte("expected"), "recordings", "joined/key.mp4"
	read := readCapability(7, bucket, key, body)
	client := &memoryCapabilityClient{objects: map[string][]byte{key: body}}
	if _, err := reconcileExactCapability(context.Background(), client, testSourceAuthority, bucket, 7, key+"-different", int64(len(body)), read.SHA256, read.ETag, read.VersionID, read); err == nil {
		t.Fatal("read capability fields substituted for independently sealed key")
	}
}

func TestCapabilitiesRejectOverlongOrExpiredLifetime(t *testing.T) {
	now := time.Now().UTC()
	body, bucket, key := []byte("expected"), "recordings", "joined/key.mp4"
	create := createCapability(7, bucket, key, "video/mp4", body)
	create.ExpiresAt = now.Add(maxPutCapabilityLifetime + time.Second)
	if create.Validate(7, bucket, key, "video/mp4", int64(len(body)), create.SHA256, testSourceAuthority, now) == nil {
		t.Fatal("overlong create capability accepted")
	}
	create = createCapability(7, bucket, key, "video/mp4", body)
	parsedCreate, _ := url.Parse(create.Request.URL)
	createQuery := parsedCreate.Query()
	createQuery.Set("X-Amz-Date", now.Add(30*time.Minute).Format("20060102T150405Z"))
	createQuery.Set("X-Amz-Expires", strconv.FormatInt(int64(maxPutCapabilityLifetime/time.Second), 10))
	parsedCreate.RawQuery = createQuery.Encode()
	create.Request.URL, create.Request.RawQuery = parsedCreate.String(), parsedCreate.RawQuery
	create.ExpiresAt, _ = signedRequestExpiry(create.Request)
	if create.Validate(7, bucket, key, "video/mp4", int64(len(body)), create.SHA256, testSourceAuthority, now) == nil {
		t.Fatal("future-skewed create capability exceeded the local one-hour authority bound")
	}
	read := readCapability(7, bucket, key, body)
	read.ExpiresAt = now.Add(maxReadCapabilityLifetime + time.Second)
	if read.Validate(7, bucket, key, int64(len(body)), read.SHA256, read.ETag, read.VersionID, testSourceAuthority, now) == nil {
		t.Fatal("overlong read capability accepted")
	}
	read = readCapability(7, bucket, key, body)
	read.ExpiresAt = read.ExpiresAt.Add(-time.Minute)
	if read.Validate(7, bucket, key, int64(len(body)), read.SHA256, read.ETag, read.VersionID, testSourceAuthority, now) == nil {
		t.Fatal("reported expiry hid a longer signed read capability")
	}
	source := testSource(1, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC))
	sourceCap := sourceReadCapability(source.Object.Key, source.Object.ETag, source.Object.VersionID, "get")
	sourceCap.SizeBytes, sourceCap.SHA256 = source.Object.SizeBytes, source.Object.SHA256
	sourceCap.ExpiresAt = now
	if sourceCap.Validate(source, "get", testSourceAuthority, now) == nil {
		t.Fatal("expired source capability accepted")
	}
}

func TestFrozenObjectIdentityRejectsMalformedETagAndVersion(t *testing.T) {
	source := testSource(1, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC))
	for _, etag := range []string{`"quoted"`, "W/weak", "with space", "line\nfeed", ""} {
		mutated := source
		mutated.Object.ETag = etag
		if err := validatePreflightSource(mutated, source.RecordingID); err == nil {
			t.Fatalf("malformed ETag %q accepted", etag)
		}
	}
	for _, version := range []string{"with space", "line\nfeed", strings.Repeat("v", 1025)} {
		mutated := source
		mutated.Object.VersionID = version
		if err := validatePreflightSource(mutated, source.RecordingID); err == nil {
			t.Fatalf("malformed version %q accepted", version)
		}
	}
	if responseETag(`"valid-etag"`) != "valid-etag" || responseETag(`valid-etag`) != "" || responseETag(`"bad"quote"`) != "" {
		t.Fatal("HTTP ETag canonicalization accepted malformed form")
	}
}

func TestCanonicalSourceEndpointV1Vectors(t *testing.T) {
	var vectors struct {
		Valid []struct {
			Endpoint  string `json:"endpoint"`
			Authority string `json:"authority"`
		} `json:"valid"`
		Invalid []string `json:"invalid"`
	}
	raw, err := os.ReadFile("testdata/source_endpoint_v1_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	if len(vectors.Valid) != 1 || len(vectors.Invalid) == 0 {
		t.Fatal("source endpoint v1 vectors are incomplete")
	}
	for _, vector := range vectors.Valid {
		authority, err := CanonicalSourceEndpointAuthority(vector.Endpoint)
		if err != nil || authority != vector.Authority {
			t.Fatalf("canonical endpoint %q differs: authority=%q err=%v", vector.Endpoint, authority, err)
		}
	}
	for _, endpoint := range vectors.Invalid {
		if authority, err := CanonicalSourceEndpointAuthority(endpoint); err == nil {
			t.Fatalf("noncanonical endpoint %q returned authority %q", endpoint, authority)
		}
		mutated := testSource(1, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC))
		mutated.Endpoint = endpoint
		if validatePreflightSource(mutated, mutated.RecordingID) == nil || validateSource(mutated, mutated.RecordingID) == nil {
			t.Fatalf("noncanonical source endpoint %q passed source validation", endpoint)
		}
	}
}

func TestCapabilityHTTPClientNeverFollowsRedirect(t *testing.T) {
	var targetRequests atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { targetRequests.Add(1) }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Redirect(w, &http.Request{}, target.URL, http.StatusFound)
	}))
	defer source.Close()
	response, err := NewCapabilityHTTPClient().Get(source.URL)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusFound || targetRequests.Load() != 0 {
		t.Fatalf("redirect followed: status=%d target=%d", response.StatusCode, targetRequests.Load())
	}
}

func TestCapabilityTransportErrorNeverLeaksSignedURL(t *testing.T) {
	const secretQuery = "X-Amz-Signature=do-not-log-this"
	request := SignedRequest{Method: http.MethodGet, URL: testSourceEndpoint + "/recordings/key?" + secretQuery, Scheme: "https", Authority: testSourceAuthority, EscapedPath: "/recordings/key", RawQuery: secretQuery, RequiredHeaders: map[string]string{}}
	_, err := capabilityRequest(context.Background(), &errorCapabilityClient{err: errors.New("Get " + request.URL + ": transport failed")}, request, nil, 0)
	if err == nil || strings.Contains(err.Error(), "do-not-log-this") || strings.Contains(err.Error(), request.URL) {
		t.Fatalf("capability URL leaked through transport error: %v", err)
	}
}
