package joinedauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/google/uuid"
)

const Audience = "recording-joined-worker-v1"

var safeBatchID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

const (
	KindClaim     = "claim"
	KindOperation = "operation"

	SubjectHour       = "hour"
	SubjectLedger     = "ledger"
	SubjectBatchIndex = "batch_index"

	OperationPreflight = "preflight"
	OperationPublish   = "publish"
)

type Claims struct {
	Version     int    `json:"v"`
	Audience    string `json:"aud"`
	ExpiresAt   int64  `json:"exp"`
	Kind        string `json:"kind"`
	BatchID     string `json:"batch_id"`
	SubjectKind string `json:"subject_kind,omitempty"`
	SubjectID   string `json:"subject_id,omitempty"`
	LeaseToken  string `json:"lease_token,omitempty"`
	Operation   string `json:"operation,omitempty"`
	joinedrecording.WorkScopeIdentity
}

func MintClaim(signingKey, batchID string, workScope joinedrecording.WorkScopeIdentity, expiresAt time.Time) (string, error) {
	claims := Claims{Version: 1, Audience: Audience, ExpiresAt: expiresAt.UTC().Unix(), Kind: KindClaim,
		BatchID: batchID, WorkScopeIdentity: workScope}
	if !validCommon(signingKey, claims, expiresAt) || workScope.Validate(batchID) != nil {
		return "", errors.New("invalid joined claim token scope")
	}
	return mint(signingKey, claims)
}

func MintOperation(signingKey, batchID, subjectKind, subjectID string, leaseToken uuid.UUID, operation string, expiresAt time.Time) (string, error) {
	claims := Claims{Version: 1, Audience: Audience, ExpiresAt: expiresAt.UTC().Unix(), Kind: KindOperation,
		BatchID: batchID, SubjectKind: subjectKind, SubjectID: strings.TrimSpace(subjectID),
		LeaseToken: leaseToken.String(), Operation: operation}
	if !validCommon(signingKey, claims, expiresAt) || leaseToken == uuid.Nil || claims.SubjectID == "" || len(claims.SubjectID) > 256 ||
		(subjectKind != SubjectHour && subjectKind != SubjectLedger && subjectKind != SubjectBatchIndex) ||
		(operation != OperationPreflight && operation != OperationPublish) {
		return "", errors.New("invalid joined operation token scope")
	}
	return mint(signingKey, claims)
}

func validCommon(signingKey string, claims Claims, expiresAt time.Time) bool {
	return strings.TrimSpace(signingKey) != "" && safeBatchID.MatchString(claims.BatchID) && expiresAt.Unix() > 0
}

func mint(signingKey string, claims Claims) (string, error) {
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	return encoded + "." + signature(signingKey, encoded), nil
}

func Verify(signingKey, token string, now time.Time) (Claims, error) {
	var claims Claims
	parts := strings.Split(strings.TrimSpace(token), ".")
	if strings.TrimSpace(signingKey) == "" || len(parts) != 2 || !hmac.Equal([]byte(parts[1]), []byte(signature(signingKey, parts[0]))) {
		return claims, errors.New("invalid joined worker token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || json.Unmarshal(payload, &claims) != nil || claims.Version != 1 || claims.Audience != Audience ||
		!safeBatchID.MatchString(claims.BatchID) || claims.ExpiresAt <= now.UTC().Unix() ||
		claims.ExpiresAt > now.UTC().Add(time.Hour).Unix() {
		return Claims{}, errors.New("invalid or expired joined worker token")
	}
	switch claims.Kind {
	case KindClaim:
		if claims.SubjectKind != "" || claims.SubjectID != "" || claims.LeaseToken != "" || claims.Operation != "" ||
			claims.WorkScopeIdentity.Validate(claims.BatchID) != nil {
			return Claims{}, errors.New("invalid joined claim token")
		}
	case KindOperation:
		if claims.SubjectID == "" || len(claims.SubjectID) > 256 || uuid.Validate(claims.LeaseToken) != nil ||
			(claims.SubjectKind != SubjectHour && claims.SubjectKind != SubjectLedger && claims.SubjectKind != SubjectBatchIndex) ||
			(claims.Operation != OperationPreflight && claims.Operation != OperationPublish) || claims.WorkScope != "" ||
			len(claims.CanaryHourIDs) != 0 || claims.CanaryHourIDsSHA256 != "" {
			return Claims{}, errors.New("invalid joined operation token")
		}
	default:
		return Claims{}, errors.New("invalid joined token kind")
	}
	return claims, nil
}

func signature(key, payload string) string {
	mac := hmac.New(sha256.New, []byte(key))
	_, _ = mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func LeaseID(leaseToken uuid.UUID) string {
	sum := sha256.Sum256([]byte("stoarama-joined-lease-id-v1\x00" + leaseToken.String()))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}
