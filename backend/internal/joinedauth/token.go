package joinedauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/google/uuid"
)

const Audience = "recording-joined-worker-v1"

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
	Version                 int    `json:"v"`
	Audience                string `json:"aud"`
	ExpiresAt               int64  `json:"exp"`
	Kind                    string `json:"kind"`
	BatchID                 string `json:"batch_id"`
	SubjectKind             string `json:"subject_kind,omitempty"`
	SubjectID               string `json:"subject_id,omitempty"`
	LeaseToken              string `json:"lease_token,omitempty"`
	Operation               string `json:"operation,omitempty"`
	WorkScope               string `json:"work_scope,omitempty"`
	WorkScopeIdentitySHA256 string `json:"work_scope_identity_sha256,omitempty"`
	// CanaryHourIDs fields are decode-only compatibility for claim tokens
	// minted before compact scope digests. New tokens always omit them.
	CanaryHourIDs       []string `json:"canary_hour_ids,omitempty"`
	CanaryHourIDsSHA256 string   `json:"canary_hour_ids_sha256,omitempty"`
}

func MintClaim(signingKey, batchID string, workScope joinedrecording.WorkScopeIdentity, expiresAt time.Time) (string, error) {
	scopeSHA, err := workScope.SHA256(batchID)
	if err != nil {
		return "", errors.New("invalid joined claim token scope")
	}
	claims := Claims{Version: 1, Audience: Audience, ExpiresAt: expiresAt.UTC().Unix(), Kind: KindClaim,
		BatchID: batchID, WorkScope: workScope.WorkScope, WorkScopeIdentitySHA256: scopeSHA}
	if !validCommon(signingKey, claims, expiresAt) {
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
	return strings.TrimSpace(signingKey) != "" && joinedrecording.ValidBatchID(claims.BatchID) && expiresAt.Unix() > 0
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
		!joinedrecording.ValidBatchID(claims.BatchID) || claims.ExpiresAt <= now.UTC().Unix() ||
		claims.ExpiresAt > now.UTC().Add(time.Hour).Unix() {
		return Claims{}, errors.New("invalid or expired joined worker token")
	}
	switch claims.Kind {
	case KindClaim:
		if claims.SubjectKind != "" || claims.SubjectID != "" || claims.LeaseToken != "" || claims.Operation != "" ||
			!validClaimScope(claims.WorkScope) || !normalizeClaimScope(&claims) {
			return Claims{}, errors.New("invalid joined claim token")
		}
	case KindOperation:
		if claims.SubjectID == "" || len(claims.SubjectID) > 256 || uuid.Validate(claims.LeaseToken) != nil ||
			(claims.SubjectKind != SubjectHour && claims.SubjectKind != SubjectLedger && claims.SubjectKind != SubjectBatchIndex) ||
			(claims.Operation != OperationPreflight && claims.Operation != OperationPublish) || claims.WorkScope != "" ||
			claims.WorkScopeIdentitySHA256 != "" || len(claims.CanaryHourIDs) != 0 || claims.CanaryHourIDsSHA256 != "" {
			return Claims{}, errors.New("invalid joined operation token")
		}
	default:
		return Claims{}, errors.New("invalid joined token kind")
	}
	return claims, nil
}

func normalizeClaimScope(claims *Claims) bool {
	if validLowerHex64(claims.WorkScopeIdentitySHA256) {
		return len(claims.CanaryHourIDs) == 0 && claims.CanaryHourIDsSHA256 == ""
	}
	legacy := joinedrecording.WorkScopeIdentity{WorkScope: claims.WorkScope, CanaryHourIDs: claims.CanaryHourIDs,
		CanaryHourIDsSHA256: claims.CanaryHourIDsSHA256}
	sha, err := legacy.SHA256(claims.BatchID)
	if err != nil {
		return false
	}
	claims.WorkScopeIdentitySHA256 = sha
	claims.CanaryHourIDs = nil
	claims.CanaryHourIDsSHA256 = ""
	return true
}

func validClaimScope(scope string) bool {
	return scope == joinedrecording.WorkScopeCanary || scope == joinedrecording.WorkScopeSingleCanary ||
		scope == joinedrecording.WorkScopeAllowlist50 || scope == joinedrecording.WorkScopeFrozenBatch
}

func validLowerHex64(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
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
