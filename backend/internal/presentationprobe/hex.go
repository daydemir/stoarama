package presentationprobe

import "encoding/hex"

func hexBytes(value string) ([]byte, error) { return hex.DecodeString(value) }
func hexString(value []byte) string         { return hex.EncodeToString(value) }
