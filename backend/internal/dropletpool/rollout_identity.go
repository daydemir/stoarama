package dropletpool

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

const (
	commonPolicyDomain  = "stoarama-cloud-recorder-common-policy-v1"
	runtimeConfigDomain = "stoarama-cloud-recorder-runtime-config-v1"
	recorderFirewallTag = "fleet:recorder-pool-v1"
)

type CommonPolicyIdentity struct {
	BuildSHA            string
	Capacity            int
	HeartbeatSec        int
	PollSec             int
	BackendAPIURL       string
	FirewallID          string
	FirewallRulesSHA256 string
	FirewallTag         string
}

type RuntimeConfigIdentity struct {
	CommonPolicySHA256 string
	WorkerID           string
	RecordingIDs       []int64
}

func (p CommonPolicyIdentity) Digest() (string, error) {
	p.BuildSHA = strings.ToLower(strings.TrimSpace(p.BuildSHA))
	p.FirewallRulesSHA256 = strings.ToLower(strings.TrimSpace(p.FirewallRulesSHA256))
	p.FirewallID = strings.TrimSpace(p.FirewallID)
	p.FirewallTag = strings.TrimSpace(p.FirewallTag)
	if !isLowerHex(p.BuildSHA, 40) || !isLowerHex(p.FirewallRulesSHA256, 64) {
		return "", fmt.Errorf("invalid common policy hash")
	}
	if p.Capacity <= 0 || p.HeartbeatSec <= 0 || p.PollSec <= 0 || p.FirewallID == "" || p.FirewallTag != recorderFirewallTag {
		return "", fmt.Errorf("invalid common policy fields")
	}
	u, err := url.Parse(strings.TrimSpace(p.BackendAPIURL))
	if err != nil || !strings.EqualFold(u.Scheme, "https") || u.User != nil || u.Hostname() == "" || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("backend API URL must be an HTTPS origin")
	}
	u.Scheme = "https"
	u.Host = strings.ToLower(u.Host)
	u.Path = strings.TrimRight(u.EscapedPath(), "/")
	if u.Path != "" {
		return "", fmt.Errorf("backend API URL must not contain a path")
	}
	return typedDigest(commonPolicyDomain,
		p.BuildSHA, strconv.Itoa(p.Capacity), strconv.Itoa(p.HeartbeatSec), strconv.Itoa(p.PollSec),
		u.String(), p.FirewallID, p.FirewallRulesSHA256, p.FirewallTag), nil
}

func (p RuntimeConfigIdentity) Digest() (string, error) {
	p.CommonPolicySHA256 = strings.ToLower(strings.TrimSpace(p.CommonPolicySHA256))
	p.WorkerID = strings.TrimSpace(p.WorkerID)
	if !isLowerHex(p.CommonPolicySHA256, 64) || !validRecorderWorkerID(p.WorkerID) || len(p.RecordingIDs) > 1 {
		return "", fmt.Errorf("invalid runtime config identity")
	}
	ids := append([]int64(nil), p.RecordingIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	fields := []string{p.CommonPolicySHA256, p.WorkerID, strconv.Itoa(len(ids))}
	for _, id := range ids {
		if id <= 0 {
			return "", fmt.Errorf("recording id must be positive")
		}
		fields = append(fields, strconv.FormatInt(id, 10))
	}
	return typedDigest(runtimeConfigDomain, fields...), nil
}

func (p RuntimeConfigIdentity) Allowlist() (string, error) {
	if len(p.RecordingIDs) == 0 {
		return "", nil
	}
	if len(p.RecordingIDs) != 1 || !validRecorderWorkerID(p.WorkerID) || p.RecordingIDs[0] <= 0 {
		return "", fmt.Errorf("invalid exact recorder allowlist")
	}
	return p.WorkerID + "/" + strconv.FormatInt(p.RecordingIDs[0], 10), nil
}

func typedDigest(domain string, fields ...string) string {
	h := sha256.New()
	writeTypedField(h, domain)
	for _, field := range fields {
		writeTypedField(h, field)
	}
	return hex.EncodeToString(h.Sum(nil))
}

type byteWriter interface{ Write([]byte) (int, error) }

func writeTypedField(w byteWriter, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = w.Write(length[:])
	_, _ = w.Write([]byte(value))
}

func isLowerHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

func validRecorderWorkerID(value string) bool {
	if !strings.HasPrefix(value, NamePrefix) || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		return false
	}
	return true
}
