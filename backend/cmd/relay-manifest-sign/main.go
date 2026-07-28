package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fail("usage: relay-manifest-sign public|sign|verify")
	}
	switch os.Args[1] {
	case "public":
		fs := flag.NewFlagSet("public", flag.ExitOnError)
		keyFile := fs.String("private-key-file", "", "base64 Ed25519 private key file")
		_ = fs.Parse(os.Args[2:])
		privateKey := loadPrivateKey(*keyFile)
		fmt.Println(base64.StdEncoding.EncodeToString(privateKey.Public().(ed25519.PublicKey)))
	case "sign":
		fs := flag.NewFlagSet("sign", flag.ExitOnError)
		keyFile := fs.String("private-key-file", "", "base64 Ed25519 private key file")
		input := fs.String("input", "", "manifest path")
		output := fs.String("output", "", "signature path")
		_ = fs.Parse(os.Args[2:])
		if *input == "" || *output == "" {
			fail("--input and --output are required")
		}
		manifest := readFile(*input)
		signature := ed25519.Sign(loadPrivateKey(*keyFile), manifest)
		if err := os.WriteFile(*output, []byte(base64.StdEncoding.EncodeToString(signature)+"\n"), 0o600); err != nil {
			fail("write signature: %v", err)
		}
	case "verify":
		fs := flag.NewFlagSet("verify", flag.ExitOnError)
		publicKey := fs.String("public-key", "", "base64 Ed25519 public key")
		input := fs.String("input", "", "manifest path")
		signatureFile := fs.String("signature", "", "signature path")
		_ = fs.Parse(os.Args[2:])
		if *input == "" || *signatureFile == "" {
			fail("--input and --signature are required")
		}
		public := decodeKey("public key", *publicKey, ed25519.PublicKeySize)
		signature := decodeKey("signature", string(readFile(*signatureFile)), ed25519.SignatureSize)
		if !ed25519.Verify(ed25519.PublicKey(public), readFile(*input), signature) {
			fail("manifest signature verification failed")
		}
	default:
		fail("unknown command %q", os.Args[1])
	}
}

func loadPrivateKey(path string) ed25519.PrivateKey {
	if strings.TrimSpace(path) == "" {
		fail("--private-key-file is required")
	}
	return ed25519.PrivateKey(decodeKey("private key", string(readFile(path)), ed25519.PrivateKeySize))
}

func decodeKey(name, encoded string, size int) []byte {
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil || len(decoded) != size {
		fail("%s must be base64-encoded %d bytes", name, size)
	}
	return decoded
}

func readFile(path string) []byte {
	if strings.TrimSpace(path) == "" {
		fail("file path is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		fail("read %s: %v", path, err)
	}
	return data
}

func fail(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
