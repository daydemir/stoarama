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

var expectedKernelFirewallRules = []string{
	"v4|-A OUTPUT -j STOARAMA_EGRESS",
	"v4|-A STOARAMA_EGRESS -d 10.0.0.0/8 -j REJECT",
	"v4|-A STOARAMA_EGRESS -d 100.64.0.0/10 -j REJECT",
	"v4|-A STOARAMA_EGRESS -d 127.0.0.0/8 -j REJECT",
	"v4|-A STOARAMA_EGRESS -d 169.254.0.0/16 -j REJECT",
	"v4|-A STOARAMA_EGRESS -d 172.16.0.0/12 -j REJECT",
	"v4|-A STOARAMA_EGRESS -d 192.168.0.0/16 -j REJECT",
	"v4|-A STOARAMA_EGRESS -d 127.0.0.0/8 -p tcp -m tcp --dport 53 -j RETURN",
	"v4|-A STOARAMA_EGRESS -d 127.0.0.0/8 -p udp -m udp --dport 53 -j RETURN",
	"v4|-A STOARAMA_EGRESS -j RETURN",
	"v4|-A STOARAMA_EGRESS -m state --state ESTABLISHED,RELATED -j RETURN",
	"v4|-N STOARAMA_EGRESS",
	"v6|-A OUTPUT -j STOARAMA_EGRESS",
	"v6|-A STOARAMA_EGRESS -d ::1/128 -j REJECT",
	"v6|-A STOARAMA_EGRESS -d ::1/128 -p tcp -m tcp --dport 53 -j RETURN",
	"v6|-A STOARAMA_EGRESS -d ::1/128 -p udp -m udp --dport 53 -j RETURN",
	"v6|-A STOARAMA_EGRESS -d fc00::/7 -j REJECT",
	"v6|-A STOARAMA_EGRESS -d fe80::/10 -j REJECT",
	"v6|-A STOARAMA_EGRESS -j RETURN",
	"v6|-A STOARAMA_EGRESS -m state --state ESTABLISHED,RELATED -j RETURN",
	"v6|-N STOARAMA_EGRESS",
}

const (
	commonPolicyDomain           = "stoarama-cloud-recorder-common-policy-v2"
	workerObservationDomain      = "stoarama-cloud-recorder-worker-observation-v1"
	recorderFirewallTag          = "fleet:recorder-pool-v1"
	maxHostFirewallScriptBytes   = 32 << 10
	maxKernelFirewallPolicyBytes = 16 << 10
)

type CommonPolicyIdentity struct {
	BuildSHA                   string
	Capacity                   int
	HeartbeatSec               int
	PollSec                    int
	BackendAPIURL              string
	FirewallID                 string
	FirewallRulesSHA256        string
	FirewallTag                string
	HostFirewallScriptSHA256   string
	KernelFirewallPolicySHA256 string
}

func (p CommonPolicyIdentity) Digest() (string, error) {
	p.BuildSHA = strings.ToLower(strings.TrimSpace(p.BuildSHA))
	p.FirewallRulesSHA256 = strings.ToLower(strings.TrimSpace(p.FirewallRulesSHA256))
	p.FirewallID = strings.TrimSpace(p.FirewallID)
	p.FirewallTag = strings.TrimSpace(p.FirewallTag)
	p.HostFirewallScriptSHA256 = strings.ToLower(strings.TrimSpace(p.HostFirewallScriptSHA256))
	p.KernelFirewallPolicySHA256 = strings.ToLower(strings.TrimSpace(p.KernelFirewallPolicySHA256))
	if !isLowerHex(p.BuildSHA, 40) || !isLowerHex(p.FirewallRulesSHA256, 64) ||
		!isLowerHex(p.HostFirewallScriptSHA256, 64) || !isLowerHex(p.KernelFirewallPolicySHA256, 64) {
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
		u.String(), p.FirewallID, p.FirewallRulesSHA256, p.FirewallTag,
		p.HostFirewallScriptSHA256, p.KernelFirewallPolicySHA256), nil
}

// WorkerRolloutFacts are observations derived by the running worker from its
// executable, effective flags, and host firewall state. The API accepts these
// bounded raw facts and recomputes both hashes; caller-provided identity hashes
// are intentionally absent.
type WorkerRolloutFacts struct {
	ExecutableBuildSHA string
	Capacity           int
	HeartbeatSec       int
	PollSec            int
	BackendAPIURL      string
	HostFirewallScript string
	KernelPolicy       string
}

type WorkerRolloutObservation struct {
	ExecutableBuildSHA         string
	Capacity                   int
	HeartbeatSec               int
	PollSec                    int
	BackendAPIURL              string
	HostFirewallScriptSHA256   string
	KernelFirewallPolicySHA256 string
	ObservationSHA256          string
}

func ValidateWorkerRolloutFacts(f WorkerRolloutFacts) (WorkerRolloutObservation, error) {
	build := strings.ToLower(strings.TrimSpace(f.ExecutableBuildSHA))
	if !isLowerHex(build, 40) || f.Capacity <= 0 || f.Capacity > 64 || f.HeartbeatSec <= 0 || f.HeartbeatSec > 300 || f.PollSec <= 0 || f.PollSec > 300 {
		return WorkerRolloutObservation{}, fmt.Errorf("invalid worker rollout runtime facts")
	}
	origin, err := canonicalHTTPSOrigin(f.BackendAPIURL)
	if err != nil {
		return WorkerRolloutObservation{}, err
	}
	if err := validateBoundedASCII("host firewall script", f.HostFirewallScript, maxHostFirewallScriptBytes); err != nil {
		return WorkerRolloutObservation{}, err
	}
	policy, err := canonicalKernelFirewallPolicy(f.KernelPolicy)
	if err != nil {
		return WorkerRolloutObservation{}, err
	}
	if policy != strings.Join(sortedExpectedKernelFirewallRules(), "\n")+"\n" {
		return WorkerRolloutObservation{}, fmt.Errorf("kernel firewall policy differs from the compiled contract")
	}
	scriptSum := sha256.Sum256([]byte(f.HostFirewallScript))
	policySum := sha256.Sum256([]byte(policy))
	out := WorkerRolloutObservation{
		ExecutableBuildSHA:         build,
		Capacity:                   f.Capacity,
		HeartbeatSec:               f.HeartbeatSec,
		PollSec:                    f.PollSec,
		BackendAPIURL:              origin,
		HostFirewallScriptSHA256:   hex.EncodeToString(scriptSum[:]),
		KernelFirewallPolicySHA256: hex.EncodeToString(policySum[:]),
	}
	out.ObservationSHA256 = typedDigest(workerObservationDomain, out.ExecutableBuildSHA,
		strconv.Itoa(out.Capacity), strconv.Itoa(out.HeartbeatSec), strconv.Itoa(out.PollSec),
		out.BackendAPIURL, out.HostFirewallScriptSHA256, out.KernelFirewallPolicySHA256)
	return out, nil
}

func canonicalHTTPSOrigin(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || !strings.EqualFold(u.Scheme, "https") || u.User != nil || u.Hostname() == "" || u.RawQuery != "" || u.Fragment != "" || strings.TrimRight(u.EscapedPath(), "/") != "" {
		return "", fmt.Errorf("backend API URL must be an HTTPS origin")
	}
	u.Scheme = "https"
	u.Host = strings.ToLower(u.Host)
	u.Path = ""
	return u.String(), nil
}

func validateBoundedASCII(label, value string, limit int) error {
	if value == "" || len(value) > limit || strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%s is empty or exceeds its bound", label)
	}
	for _, r := range value {
		if r != '\n' && r != '\t' && (r < 0x20 || r > 0x7e) {
			return fmt.Errorf("%s is not bounded ASCII", label)
		}
	}
	return nil
}

// canonicalKernelFirewallPolicy accepts the already-derived semantic policy,
// not raw command output. One exact LF-terminated, sorted, duplicate-free list
// keeps kernel counters, table order, and incidental host comments out of the
// authority digest while rejecting unknown grammar.
func canonicalKernelFirewallPolicy(raw string) (string, error) {
	if err := validateBoundedASCII("kernel firewall policy", raw, maxKernelFirewallPolicyBytes); err != nil {
		return "", err
	}
	if !strings.HasSuffix(raw, "\n") || strings.Contains(raw, "\r") {
		return "", fmt.Errorf("kernel firewall policy must be LF framed")
	}
	lines := strings.Split(strings.TrimSuffix(raw, "\n"), "\n")
	previous := ""
	for _, line := range lines {
		if line == "" || len(line) > 512 || !strings.HasPrefix(line, "v") || !strings.Contains(line, "|") || (previous != "" && line <= previous) {
			return "", fmt.Errorf("kernel firewall policy is not canonical")
		}
		previous = line
	}
	return raw, nil
}

// DeriveKernelFirewallPolicy converts bounded `iptables -S` and `ip6tables -S`
// observations into the exact semantic policy. It rejects missing, duplicate,
// or additional Stoarama-chain rules and does not retain counters or unrelated
// host firewall state.
func DeriveKernelFirewallPolicy(v4, v6 []byte) (string, error) {
	if len(v4) == 0 || len(v4) > maxKernelFirewallPolicyBytes || len(v6) == 0 || len(v6) > maxKernelFirewallPolicyBytes {
		return "", fmt.Errorf("kernel firewall command output is empty or oversized")
	}
	var actual []string
	for _, input := range []struct {
		family string
		body   []byte
	}{{"v4", v4}, {"v6", v6}} {
		if strings.ContainsRune(string(input.body), '\x00') || strings.ContainsRune(string(input.body), '\r') {
			return "", fmt.Errorf("kernel firewall output is not canonical ASCII")
		}
		for _, line := range strings.Split(strings.TrimSuffix(string(input.body), "\n"), "\n") {
			if !strings.Contains(line, "STOARAMA_EGRESS") {
				continue
			}
			fields := strings.Fields(line)
			for i := range fields {
				if i > 0 && fields[i-1] == "--state" {
					states := strings.Split(fields[i], ",")
					sort.Strings(states)
					fields[i] = strings.Join(states, ",")
				}
			}
			actual = append(actual, input.family+"|"+strings.Join(fields, " "))
		}
	}
	sort.Strings(actual)
	expected := sortedExpectedKernelFirewallRules()
	if len(actual) != len(expected) {
		return "", fmt.Errorf("kernel firewall policy has %d relevant rules, want %d", len(actual), len(expectedKernelFirewallRules))
	}
	for i := range actual {
		if actual[i] != expected[i] {
			return "", fmt.Errorf("kernel firewall policy differs at rule %d", i)
		}
	}
	return strings.Join(actual, "\n") + "\n", nil
}

func ExpectedKernelFirewallPolicySHA256() string {
	sum := sha256.Sum256([]byte(strings.Join(sortedExpectedKernelFirewallRules(), "\n") + "\n"))
	return hex.EncodeToString(sum[:])
}

func sortedExpectedKernelFirewallRules() []string {
	out := append([]string(nil), expectedKernelFirewallRules...)
	sort.Strings(out)
	return out
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
