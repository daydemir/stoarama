// stoarama-presentation-proof verifies an offline, non-production C2 target
// capability assessment. It cannot build, install, launch, or publish a probe;
// emit a media/corpus proof; or access a database, API, relay, or NAS.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/presentationprobe"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	if len(args) == 0 || args[0] != "verify-capability" {
		return errors.New("usage: stoarama-presentation-proof verify-capability")
	}
	fs := flag.NewFlagSet("verify-capability", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	envelopePath := fs.String("envelope", "", "canonical capability assessment")
	signaturePath := fs.String("signature", "", "detached hex signature")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || *envelopePath == "" || *signaturePath == "" {
		return errors.New("exact envelope and signature paths are required")
	}
	envelope, err := os.ReadFile(*envelopePath)
	if err != nil {
		return err
	}
	signatureText, err := os.ReadFile(*signaturePath)
	if err != nil {
		return err
	}
	signature, err := presentationprobe.DecodeSignatureHex(strings.TrimSpace(string(signatureText)))
	if err != nil {
		return err
	}
	got, err := presentationprobe.VerifyPinnedCapabilityEnvelope(envelope, signature)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "capability_signature_verified class=%s target=%s-%s status=target_unrunnable production_eligible=false\n", got.ProvenanceClass, got.Evidence.TargetOS, got.Evidence.TargetArch)
	return err
}
