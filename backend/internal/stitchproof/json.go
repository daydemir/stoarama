package stitchproof

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"
)

// Every frozen attestation field is required. Decode checks exact field names,
// including case, before the typed decode; encoding/json alone accepts aliases.
func decode(raw []byte, dst any) error {
	v, err := strictJSON(raw)
	if err != nil {
		return err
	}
	if err := required(v, reflect.TypeOf(dst).Elem()); err != nil {
		return err
	}
	return json.Unmarshal(raw, dst)
}

// DecodeExact rejects duplicate keys, nulls, trailing data and unknown fields in
// versioned local ledger records. Their tagged union enforces required fields.
func DecodeExact(raw []byte, dst any) error {
	v, err := strictJSON(raw)
	if err != nil {
		return err
	}
	if err := exactNames(v, reflect.TypeOf(dst).Elem()); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	return d.Decode(dst)
}

func exactNames(v any, t reflect.Type) error {
	if t.Kind() == reflect.Pointer {
		return exactNames(v, t.Elem())
	}
	if m, ok := v.(map[string]any); ok && t.Kind() == reflect.Struct {
		fields := map[string]reflect.Type{}
		for i := range t.NumField() {
			f := t.Field(i)
			fields[strings.Split(f.Tag.Get("json"), ",")[0]] = f.Type
		}
		for name, item := range m {
			child, ok := fields[name]
			if !ok {
				return fmt.Errorf("unknown exact JSON field %q", name)
			}
			if err := exactNames(item, child); err != nil {
				return err
			}
		}
	}
	return nil
}

func strictJSON(raw []byte) (any, error) {
	if !utf8.Valid(raw) {
		return nil, errors.New("invalid UTF-8 JSON")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	v, err := jsonValue(d, 0)
	if err != nil {
		return nil, err
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, errors.New("trailing JSON data")
	}
	return v, nil
}

func jsonValue(d *json.Decoder, depth int) (any, error) {
	if depth > 64 {
		return nil, errors.New("JSON nesting limit")
	}
	t, err := d.Token()
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, errors.New("null is not evidence")
	}
	switch t {
	case json.Delim('{'):
		m := map[string]any{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return nil, err
			}
			k, ok := key.(string)
			if !ok {
				return nil, errors.New("invalid JSON object key")
			}
			if _, exists := m[k]; exists {
				return nil, fmt.Errorf("duplicate JSON field %q", k)
			}
			v, err := jsonValue(d, depth+1)
			if err != nil {
				return nil, err
			}
			m[k] = v
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return nil, errors.New("invalid JSON object end")
		}
		return m, nil
	case json.Delim('['):
		a := []any{}
		for d.More() {
			v, err := jsonValue(d, depth+1)
			if err != nil {
				return nil, err
			}
			a = append(a, v)
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return nil, errors.New("invalid JSON array end")
		}
		return a, nil
	default:
		if _, delim := t.(json.Delim); delim {
			return nil, errors.New("unexpected JSON delimiter")
		}
		return t, nil
	}
}

func required(v any, t reflect.Type) error {
	switch t.Kind() {
	case reflect.Struct:
		m, ok := v.(map[string]any)
		if !ok || len(m) != t.NumField() {
			return fmt.Errorf("%s requires every exact field", t.Name())
		}
		for i := range t.NumField() {
			f := t.Field(i)
			name := f.Tag.Get("json")
			value, exists := m[name]
			if !exists {
				return fmt.Errorf("missing field %s.%s", t.Name(), name)
			}
			if err := required(value, f.Type); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
		}
	case reflect.Slice:
		a, ok := v.([]any)
		if !ok {
			return errors.New("expected JSON array")
		}
		for _, e := range a {
			if err := required(e, t.Elem()); err != nil {
				return err
			}
		}
	case reflect.String:
		s, ok := v.(string)
		if !ok || len(s) == 0 || len(s) > 2048 {
			return errors.New("required bounded string")
		}
	case reflect.Int64:
		n, ok := v.(json.Number)
		if !ok {
			return errors.New("required integer")
		}
		i, err := n.Int64()
		if err != nil || i <= 0 {
			return errors.New("required positive integer")
		}
	case reflect.Float64:
		n, ok := v.(json.Number)
		if !ok {
			return errors.New("required number")
		}
		f, err := n.Float64()
		if err != nil || f <= 0 {
			return errors.New("required positive finite number")
		}
	case reflect.Bool:
		if _, ok := v.(bool); !ok {
			return errors.New("required boolean")
		}
	default:
		return fmt.Errorf("unsupported frozen field type %s", t)
	}
	return nil
}
