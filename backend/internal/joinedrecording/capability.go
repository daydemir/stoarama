package joinedrecording

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
)

// StorageCapabilityError reports only the immutable artifact coordinate and
// transport status needed to diagnose a failed publication. Cause remains
// available to errors.Is/errors.As, but Error deliberately never renders it.
type StorageCapabilityError struct {
	Operation         string
	Reason            string
	StatusCode        int
	ArtifactID        int64
	Ordinal           int
	Attempts          int
	RequestID         StorageRequestIDEvidence
	ExtendedRequestID StorageRequestIDEvidence
	RayID             StorageRequestIDEvidence
	Cause             error
}

type StorageRequestIDEvidence struct {
	SHA256 string
	Length int
}

func (e *StorageCapabilityError) Error() string {
	if e == nil {
		return "storage capability failed"
	}
	operation := e.Operation
	if operation != "put" && operation != "reread_capability" && operation != "reread" {
		operation = "unknown"
	}
	status := e.StatusCode
	if status < 0 || status > 599 {
		status = 0
	}
	reason := e.Reason
	if reason != "capability" && reason != "transport" && reason != "status" && reason != "identity" && reason != "hash" {
		reason = "unknown"
	}
	message := fmt.Sprintf("storage capability operation=%s reason=%s status=%d artifact_id=%d ordinal=%d",
		operation, reason, status, e.ArtifactID, e.Ordinal)
	if e.Attempts > 0 && e.Attempts <= len(putRetryDelays)+1 {
		message += fmt.Sprintf(" attempts=%d", e.Attempts)
	}
	if e.RequestID.valid() {
		message += fmt.Sprintf(" request_id_sha256=%s request_id_length=%d", e.RequestID.SHA256, e.RequestID.Length)
	}
	if e.ExtendedRequestID.valid() {
		message += fmt.Sprintf(" extended_request_id_sha256=%s extended_request_id_length=%d",
			e.ExtendedRequestID.SHA256, e.ExtendedRequestID.Length)
	}
	if e.RayID.valid() {
		message += fmt.Sprintf(" ray_id_sha256=%s ray_id_length=%d", e.RayID.SHA256, e.RayID.Length)
	}
	return message
}

func (e *StorageCapabilityError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e StorageRequestIDEvidence) valid() bool {
	return e.Length > 0 && lowerHex64(e.SHA256)
}

func storageRequestIDEvidence(raw string) StorageRequestIDEvidence {
	if raw == "" {
		return StorageRequestIDEvidence{}
	}
	sum := sha256.Sum256([]byte(raw))
	return StorageRequestIDEvidence{SHA256: hex.EncodeToString(sum[:]), Length: len(raw)}
}

const SourceEndpointV1Pattern = `^https://[0-9a-f]{32}\.r2\.cloudflarestorage\.com$`

var sourceEndpointV1 = regexp.MustCompile(SourceEndpointV1Pattern)

type CapabilityHTTPClient interface {
	Do(*http.Request) (*http.Response, error)
	joinedRedirectSafe()
}

type capabilityHTTPClient struct{ *http.Client }

const (
	maxReadCapabilityLifetime = 15 * time.Minute
	maxPutCapabilityLifetime  = time.Hour
	capabilityHTTPTimeout     = 70 * time.Minute
)

func (*capabilityHTTPClient) joinedRedirectSafe() {}

func NewCapabilityHTTPClient() *capabilityHTTPClient {
	return &capabilityHTTPClient{&http.Client{Timeout: capabilityHTTPTimeout, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}}
}

type SignedRequest struct {
	Method          string            `json:"method"`
	URL             string            `json:"url"`
	Scheme          string            `json:"scheme"`
	Authority       string            `json:"authority"`
	EscapedPath     string            `json:"escaped_path"`
	RawQuery        string            `json:"raw_query"`
	RequiredHeaders map[string]string `json:"required_headers"`
}

type SourceReadCapability struct {
	ProtocolVersion int           `json:"protocol_version"`
	Operation       string        `json:"operation"`
	ObjectKey       string        `json:"object_key"`
	SizeBytes       int64         `json:"size_bytes"`
	SHA256          string        `json:"sha256"`
	ETag            string        `json:"etag"`
	VersionID       string        `json:"version_id"`
	ExpiresAt       time.Time     `json:"expires_at"`
	Request         SignedRequest `json:"request"`
}

type ObjectCreateCapability struct {
	ProtocolVersion int           `json:"protocol_version"`
	ArtifactID      int64         `json:"artifact_id"`
	ObjectKey       string        `json:"object_key"`
	ContentType     string        `json:"content_type"`
	SizeBytes       int64         `json:"size_bytes"`
	SHA256          string        `json:"sha256"`
	ExpiresAt       time.Time     `json:"expires_at"`
	Request         SignedRequest `json:"request"`
}

type ObjectReadCapability struct {
	ProtocolVersion int           `json:"protocol_version"`
	ArtifactID      int64         `json:"artifact_id"`
	ObjectKey       string        `json:"object_key"`
	SizeBytes       int64         `json:"size_bytes"`
	SHA256          string        `json:"sha256"`
	ETag            string        `json:"etag"`
	VersionID       string        `json:"version_id"`
	ExpiresAt       time.Time     `json:"expires_at"`
	Request         SignedRequest `json:"request"`
}

type putObservation struct {
	Created   bool
	ETag      string
	VersionID string
}

func (r SignedRequest) validate(method, authority, escapedPath string) error {
	parsed, err := url.Parse(r.URL)
	if err != nil || r.Method != method || r.Scheme != "https" || r.Authority != authority || parsed.Scheme != r.Scheme || parsed.Host != r.Authority || parsed.EscapedPath() != r.EscapedPath || r.EscapedPath != escapedPath || parsed.RawQuery != r.RawQuery || parsed.User != nil || parsed.Fragment != "" {
		return fmt.Errorf("signed storage request coordinates differ")
	}
	return nil
}

func canonicalObjectEscapedPath(bucket, key string) (string, error) {
	if strings.TrimSpace(bucket) == "" || !safeObjectKey(key) {
		return "", fmt.Errorf("invalid storage object coordinates")
	}
	return (&url.URL{Path: "/" + bucket + "/" + key}).EscapedPath(), nil
}

func (c SourceReadCapability) Validate(source SourceClip, operation, authority string, now time.Time) error {
	objectPath, err := canonicalObjectEscapedPath(source.Bucket, source.Object.Key)
	frozenAuthority, endpointErr := CanonicalSourceEndpointAuthority(source.Endpoint)
	wantHeaders := map[string]string{"If-Match": quotedETag(source.Object.ETag)}
	method := map[string]string{"head": http.MethodHead, "get": http.MethodGet}[operation]
	if err != nil || endpointErr != nil || frozenAuthority != authority || c.ProtocolVersion != JoinedProtocolVersion || method == "" || c.Operation != operation || !validObjectIdentity(c.ETag, c.VersionID) || !validSignedCapabilityExpiry(c.Request, c.ExpiresAt, now, maxReadCapabilityLifetime) || c.ObjectKey != source.Object.Key || c.SizeBytes != source.Object.SizeBytes || c.SHA256 != source.Object.SHA256 || c.ETag != source.Object.ETag || c.VersionID != source.Object.VersionID || !sameHeaders(c.Request.RequiredHeaders, wantHeaders) || c.Request.validate(method, authority, objectPath) != nil {
		return fmt.Errorf("source capability differs from frozen object")
	}
	if source.Object.VersionID != "" && singleQueryValue(c.Request.URL, "versionId") != source.Object.VersionID {
		return fmt.Errorf("source capability lacks exact object version")
	}
	if source.Object.VersionID == "" && hasQueryKey(c.Request.URL, "versionId") {
		return fmt.Errorf("source capability has unexpected object version")
	}
	return nil
}

// CanonicalSourceEndpointAuthority validates the exact protocol-v1 Cloudflare
// R2 endpoint bytes shared by the backend, worker, and NAS client.
func CanonicalSourceEndpointAuthority(endpoint string) (string, error) {
	if !sourceEndpointV1.MatchString(endpoint) {
		return "", fmt.Errorf("invalid frozen storage endpoint")
	}
	return strings.TrimPrefix(endpoint, "https://"), nil
}

func (c ObjectCreateCapability) Validate(artifactID int64, bucket, key, contentType string, size int64, sha, authority string, now time.Time) error {
	objectPath, err := canonicalObjectEscapedPath(bucket, key)
	wantHeaders := map[string]string{"Content-Length": strconv.FormatInt(size, 10), "Content-Type": contentType, "If-None-Match": "*"}
	wantChecksum := base64.StdEncoding.EncodeToString(mustDecodeHex(sha))
	if err != nil || c.ProtocolVersion != JoinedProtocolVersion || artifactID <= 0 || c.ArtifactID != artifactID || !validSignedCapabilityExpiry(c.Request, c.ExpiresAt, now, maxPutCapabilityLifetime) || c.ObjectKey != key || c.ContentType != contentType || c.SizeBytes != size || c.SHA256 != sha || size <= 0 || size > r2.MaxConditionalPutBytes || !lowerHex64(sha) || !sameHeaders(c.Request.RequiredHeaders, wantHeaders) || singleQueryValue(c.Request.URL, "X-Amz-Checksum-Sha256") != wantChecksum || c.Request.validate(http.MethodPut, authority, objectPath) != nil {
		return fmt.Errorf("create capability differs from sealed artifact")
	}
	return nil
}

func (c ObjectReadCapability) Validate(artifactID int64, bucket, key string, size int64, sha, etag, versionID, authority string, now time.Time) error {
	objectPath, err := canonicalObjectEscapedPath(bucket, key)
	wantHeaders := map[string]string{"If-Match": quotedETag(etag)}
	if err != nil || c.ProtocolVersion != JoinedProtocolVersion || !validObjectIdentity(etag, versionID) || !validObjectIdentity(c.ETag, c.VersionID) || artifactID <= 0 || c.ArtifactID != artifactID || !validSignedCapabilityExpiry(c.Request, c.ExpiresAt, now, maxReadCapabilityLifetime) || c.ObjectKey != key || c.SizeBytes != size || c.SHA256 != sha || c.ETag != etag || c.VersionID != versionID || !sameHeaders(c.Request.RequiredHeaders, wantHeaders) || c.Request.validate(http.MethodGet, authority, objectPath) != nil {
		return fmt.Errorf("read capability differs from observed sealed artifact")
	}
	if versionID != "" && singleQueryValue(c.Request.URL, "versionId") != versionID {
		return fmt.Errorf("read capability lacks exact object version")
	}
	if versionID == "" && hasQueryKey(c.Request.URL, "versionId") {
		return fmt.Errorf("read capability has unexpected object version")
	}
	return nil
}

func validCapabilityExpiry(expiresAt, now time.Time, maximum time.Duration) bool {
	return expiresAt.After(now) && !expiresAt.After(now.Add(maximum))
}

func validSignedCapabilityExpiry(request SignedRequest, reported, now time.Time, maximum time.Duration) bool {
	parsed, err := url.Parse(request.URL)
	if err != nil {
		return false
	}
	query := parsed.Query()
	dateValues, expiresValues := query["X-Amz-Date"], query["X-Amz-Expires"]
	if len(dateValues) != 1 || len(expiresValues) != 1 {
		return false
	}
	signedAt, dateErr := time.Parse("20060102T150405Z", dateValues[0])
	seconds, expiresErr := strconv.ParseInt(expiresValues[0], 10, 64)
	if dateErr != nil || expiresErr != nil || seconds <= 0 || seconds > int64(maximum/time.Second) {
		return false
	}
	signedExpiry := signedAt.Add(time.Duration(seconds) * time.Second)
	return reported.Equal(signedExpiry) && validCapabilityExpiry(signedExpiry, now, maximum)
}

func verifyExactSourceHeadCapability(ctx context.Context, client CapabilityHTTPClient, authority string, source SourceClip, capability SourceReadCapability) error {
	if client == nil || capability.Validate(source, "head", authority, time.Now().UTC()) != nil {
		return fmt.Errorf("exact source HEAD capability is required")
	}
	return verifyCapabilityHead(ctx, client, capability.Request, capability.ExpiresAt, source.Object.SizeBytes, source.Object.ETag, source.Object.VersionID)
}

func openExactCapability(ctx context.Context, client CapabilityHTTPClient, authority string, source SourceClip, capability SourceReadCapability) (io.ReadCloser, error) {
	if client == nil || capability.Validate(source, "get", authority, time.Now().UTC()) != nil {
		return nil, fmt.Errorf("exact source capability is required")
	}
	response, err := capabilityRequest(ctx, client, capability.Request, capability.ExpiresAt, nil, 0)
	if err != nil {
		return nil, err
	}
	if responseETag(response.Header.Get("ETag")) != source.Object.ETag || response.StatusCode != http.StatusOK || responseSize(response) != source.Object.SizeBytes || (source.Object.VersionID != "" && response.Header.Get("x-amz-version-id") != source.Object.VersionID) {
		response.Body.Close()
		return nil, fmt.Errorf("source capability GET identity differs")
	}
	return response.Body, nil
}

func putCreateOnlyCapability(ctx context.Context, client CapabilityHTTPClient, authority, bucket string, artifactID int64, key, contentType string, size int64, sha string, capability ObjectCreateCapability, body io.Reader) (putObservation, error) {
	if client == nil || capability.Validate(artifactID, bucket, key, contentType, size, sha, authority, time.Now().UTC()) != nil {
		return putObservation{}, &StorageCapabilityError{Operation: "put", Reason: "capability", ArtifactID: artifactID,
			Cause: errors.New("exact create capability is required")}
	}
	response, err := capabilityRequest(ctx, client, capability.Request, capability.ExpiresAt, body, size)
	if err != nil {
		return putObservation{}, &StorageCapabilityError{Operation: "put", Reason: "transport", ArtifactID: artifactID, Cause: err}
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	response.Body.Close()
	created := response.StatusCode >= 200 && response.StatusCode < 300
	if !created && response.StatusCode != http.StatusPreconditionFailed {
		return putObservation{}, &StorageCapabilityError{Operation: "put", Reason: "status", StatusCode: response.StatusCode,
			ArtifactID: artifactID, RequestID: storageRequestIDEvidence(response.Header.Get("x-amz-request-id")),
			ExtendedRequestID: storageRequestIDEvidence(response.Header.Get("x-amz-id-2")),
			RayID:             storageRequestIDEvidence(response.Header.Get("cf-ray"))}
	}
	return putObservation{Created: created, ETag: responseETag(response.Header.Get("ETag")), VersionID: response.Header.Get("x-amz-version-id")}, nil
}

func reconcileExactCapability(ctx context.Context, client CapabilityHTTPClient, authority, bucket string, artifactID int64, key string, size int64, sha, etag, versionID string, capability ObjectReadCapability) (r2.ObjectHead, error) {
	if client == nil || capability.Validate(artifactID, bucket, key, size, sha, etag, versionID, authority, time.Now().UTC()) != nil {
		return r2.ObjectHead{}, &StorageCapabilityError{Operation: "reread", Reason: "capability", ArtifactID: artifactID,
			Cause: errors.New("exact read capability is required")}
	}
	response, err := capabilityRequest(ctx, client, capability.Request, capability.ExpiresAt, nil, 0)
	if err != nil {
		return r2.ObjectHead{}, &StorageCapabilityError{Operation: "reread", Reason: "transport", ArtifactID: artifactID, Cause: err}
	}
	defer response.Body.Close()
	requestID := storageRequestIDEvidence(response.Header.Get("x-amz-request-id"))
	extendedRequestID := storageRequestIDEvidence(response.Header.Get("x-amz-id-2"))
	rayID := storageRequestIDEvidence(response.Header.Get("cf-ray"))
	if response.StatusCode != http.StatusOK {
		return r2.ObjectHead{}, &StorageCapabilityError{Operation: "reread", Reason: "status", StatusCode: response.StatusCode,
			ArtifactID: artifactID, RequestID: requestID, ExtendedRequestID: extendedRequestID, RayID: rayID}
	}
	if responseSize(response) != capability.SizeBytes || responseETag(response.Header.Get("ETag")) != capability.ETag || (capability.VersionID != "" && response.Header.Get("x-amz-version-id") != capability.VersionID) {
		return r2.ObjectHead{}, &StorageCapabilityError{Operation: "reread", Reason: "identity", StatusCode: response.StatusCode,
			ArtifactID: artifactID, RequestID: requestID, ExtendedRequestID: extendedRequestID, RayID: rayID}
	}
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(response.Body, capability.SizeBytes+1))
	if err != nil {
		return r2.ObjectHead{}, &StorageCapabilityError{Operation: "reread", Reason: "transport", StatusCode: response.StatusCode,
			ArtifactID: artifactID, RequestID: requestID, ExtendedRequestID: extendedRequestID, RayID: rayID, Cause: err}
	}
	if n != capability.SizeBytes || subtle.ConstantTimeCompare(hash.Sum(nil), mustDecodeHex(capability.SHA256)) != 1 {
		return r2.ObjectHead{}, &StorageCapabilityError{Operation: "reread", Reason: "hash", StatusCode: response.StatusCode,
			ArtifactID: artifactID, RequestID: requestID, ExtendedRequestID: extendedRequestID, RayID: rayID}
	}
	return r2.ObjectHead{ETag: capability.ETag, VersionID: capability.VersionID, SizeBytes: capability.SizeBytes}, nil
}

func verifyCapabilityHead(ctx context.Context, client CapabilityHTTPClient, request SignedRequest, expiresAt time.Time, size int64, etag, versionID string) error {
	response, err := capabilityRequest(ctx, client, request, expiresAt, nil, 0)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || responseSize(response) != size || responseETag(response.Header.Get("ETag")) != etag || (versionID != "" && response.Header.Get("x-amz-version-id") != versionID) {
		return fmt.Errorf("exact HEAD identity differs")
	}
	return nil
}

func capabilityRequest(ctx context.Context, client CapabilityHTTPClient, signed SignedRequest, expiresAt time.Time, body io.Reader, contentLength int64) (*http.Response, error) {
	requestCtx, cancel := context.WithDeadline(ctx, expiresAt)
	request, err := http.NewRequestWithContext(requestCtx, signed.Method, signed.URL, body)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("construct storage capability request")
	}
	request.Header.Set("Accept-Encoding", "identity")
	for key, value := range signed.RequiredHeaders {
		request.Header.Set(key, value)
	}
	if signed.Method == http.MethodPut {
		request.ContentLength = contentLength
	}
	response, err := client.Do(request)
	if err != nil {
		ctxErr := requestCtx.Err()
		cancel()
		if ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("execute storage capability request")
	}
	response.Body = cancelOnCloseBody{ReadCloser: response.Body, cancel: cancel}
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		response.Body.Close()
		return nil, fmt.Errorf("storage capability redirect is forbidden")
	}
	if response.Request != nil && response.Request.URL.String() != request.URL.String() {
		response.Body.Close()
		return nil, fmt.Errorf("storage capability redirect is forbidden")
	}
	return response, nil
}

type cancelOnCloseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b cancelOnCloseBody) Close() error {
	b.cancel()
	return b.ReadCloser.Close()
}

func responseSize(response *http.Response) int64 {
	if response.ContentLength >= 0 {
		return response.ContentLength
	}
	size, _ := strconv.ParseInt(response.Header.Get("Content-Length"), 10, 64)
	return size
}

func responseETag(raw string) string {
	if len(raw) < 3 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return ""
	}
	value := raw[1 : len(raw)-1]
	if !validObjectIdentity(value, "") {
		return ""
	}
	return value
}

func quotedETag(raw string) string { return `"` + raw + `"` }

func mustDecodeHex(raw string) []byte {
	decoded, _ := hex.DecodeString(raw)
	return decoded
}

func singleQueryValue(rawURL, key string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil || len(parsed.Query()[key]) != 1 {
		return ""
	}
	return parsed.Query()[key][0]
}

func hasQueryKey(rawURL, key string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return true
	}
	_, ok := parsed.Query()[key]
	return ok
}

func sameHeaders(got, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for key, value := range want {
		if got[key] != value {
			return false
		}
	}
	return true
}
