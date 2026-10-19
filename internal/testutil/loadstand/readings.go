package loadstand

import (
	"bytes"
	"context"
	"encoding"
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"time"
)

func object(raw json.RawMessage, required ...string) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := validateJSONValue(decoder, 0); err != nil {
		return nil, ErrInvalidSample
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, ErrInvalidSample
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, ErrInvalidSample
	}
	if err := canonicalFields(fields, required...); err != nil {
		return nil, err
	}
	for _, name := range required {
		value, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, ErrFamilyUnavailable
		}
	}
	return fields, nil
}

// Schema fields use exact names. encoding/json also accepts Unicode case-fold
// aliases for struct fields; reject those before a validated object is decoded.
// Counter-map labels have no declared fields and retain their exact spelling.
func canonicalFields(fields map[string]json.RawMessage, known ...string) error {
	for name := range fields {
		for _, canonical := range known {
			if name != canonical && strings.EqualFold(name, canonical) {
				return ErrInvalidSample
			}
		}
	}
	return nil
}

func validateJSONValue(d *json.Decoder, depth int) error {
	if depth > 32 {
		return ErrInvalidSample
	}
	token, err := d.Token()
	if err != nil {
		return ErrInvalidSample
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return nil
	}
	switch delimiter {
	case '{':
		names := map[string]struct{}{}
		for d.More() {
			token, err := d.Token()
			if err != nil {
				return ErrInvalidSample
			}
			name, ok := token.(string)
			if !ok {
				return ErrInvalidSample
			}
			if _, exists := names[name]; exists {
				return ErrInvalidSample
			}
			names[name] = struct{}{}
			if err := validateJSONValue(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := validateJSONValue(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return ErrInvalidSample
	}
	end, err := d.Token()
	if err != nil {
		return ErrInvalidSample
	}
	if delimiter == '{' && end != json.Delim('}') || delimiter == '[' && end != json.Delim(']') {
		return ErrInvalidSample
	}
	return nil
}
func value[T any](fields map[string]json.RawMessage, name string) (T, error) {
	var out T
	raw, ok := fields[name]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return out, ErrFamilyUnavailable
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, ErrInvalidSample
	}
	return out, nil
}
func (s *ProcessSource) commandObject(ctx context.Context, command string, required ...string) (map[string]json.RawMessage, error) {
	var raw json.RawMessage
	if err := s.client.Call(ctx, command, nil, &raw); err != nil {
		return nil, err
	}
	return object(raw, required...)
}
func (s *ProcessSource) familyObject(ctx context.Context, name string, required ...string) (map[string]json.RawMessage, error) {
	fields, err := s.RouteSummary(ctx)
	if err != nil {
		return nil, err
	}
	raw, ok := fields[name]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, ErrFamilyUnavailable
	}
	return object(raw, required...)
}
func timestamp(fields map[string]json.RawMessage, name string) (time.Time, error) {
	at, err := value[time.Time](fields, name)
	if err != nil {
		return at, err
	}
	if at.IsZero() {
		return time.Time{}, ErrInvalidSample
	}
	return at, nil
}

// decodeDTO uses the destination's actual JSON schema, including optional and
// nested fields. Map keys stay opaque; only declared struct fields are folded.
func decodeDTO(raw json.RawMessage, target any) error {
	row := reflect.TypeOf(target)
	if row == nil || row.Kind() != reflect.Pointer {
		return ErrInvalidSample
	}
	if err := canonicalSchema(raw, row); err != nil {
		return err
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return ErrInvalidSample
	}
	return nil
}
func canonicalSchema(raw json.RawMessage, row reflect.Type) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	for row.Kind() == reflect.Pointer {
		row = row.Elem()
	}
	// Custom scalar codecs (for example time.Time and netip.Addr) own their
	// representation; they do not use encoding/json's struct-field matching.
	pointer := reflect.PointerTo(row)
	if pointer.Implements(reflect.TypeFor[json.Unmarshaler]()) || pointer.Implements(reflect.TypeFor[encoding.TextUnmarshaler]()) {
		return nil
	}
	switch row.Kind() {
	case reflect.Struct:
		fields, err := object(raw)
		if err != nil {
			return err
		}
		known := make(map[string]reflect.Type, row.NumField())
		names := make([]string, 0, row.NumField())
		for i := 0; i < row.NumField(); i++ {
			field := row.Field(i)
			if !field.IsExported() {
				continue
			}
			// Embedded field promotion needs a new explicit schema decision.
			if field.Anonymous {
				return ErrInvalidSample
			}
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			known[name] = field.Type
			names = append(names, name)
		}
		if err := canonicalFields(fields, names...); err != nil {
			return err
		}
		for name, field := range fields {
			if fieldType, ok := known[name]; ok {
				if err := canonicalSchema(field, fieldType); err != nil {
					return err
				}
			}
		}
	case reflect.Map:
		fields, err := object(raw)
		if err != nil {
			return err
		}
		for _, field := range fields {
			if err := canonicalSchema(field, row.Elem()); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		if row.Kind() == reflect.Slice && row.Elem().Kind() == reflect.Uint8 {
			return nil
		}
		var elements []json.RawMessage
		if err := json.Unmarshal(raw, &elements); err != nil {
			return ErrInvalidSample
		}
		for _, element := range elements {
			if err := canonicalSchema(element, row.Elem()); err != nil {
				return err
			}
		}
	}
	return nil
}
func (s *ProcessSource) readDTO(ctx context.Context, command string, target any) error {
	var raw json.RawMessage
	if err := s.client.Call(ctx, command, nil, &raw); err != nil {
		return err
	}
	return decodeDTO(raw, target)
}
