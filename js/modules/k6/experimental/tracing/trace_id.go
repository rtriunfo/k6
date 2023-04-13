package tracing

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"time"
)

const (
	// Being 075 the ASCII code for 'K' :)
	k6Prefix = 0o756

	// To ingest and process the related spans in k6 Cloud.
	k6CloudCode = 12

	// To not ingest and process the related spans, b/c they are part of a non-cloud run.
	k6LocalCode = 33

	// metadataTraceIDKeyName is the key name of the traceID in the output metadata.
	metadataTraceIDKeyName = "trace_id"

	// traceIDEncodedSize is the size of the encoded traceID.
	traceIDEncodedSize = 16
)

// newTraceID generates a new trace ID.
//
// Because the trace ID relies on randomness, and its underlying serialization
// depends on the time, it is mandatory to use this function to generate
// trace IDs, instead of creating them manually.
func newTraceID(prefix int16, code int8, t time.Time, randSource io.Reader) (traceID, error) {
	if prefix != k6Prefix {
		return traceID{}, fmt.Errorf("invalid prefix 0o%o, expected 0o%o", prefix, k6Prefix)
	}

	if (code != k6CloudCode) && (code != k6LocalCode) {
		return traceID{}, fmt.Errorf("invalid code 0o%d, accepted values are 0o%d and 0o%d", code, k6CloudCode, k6LocalCode)
	}

	// Encode The trace ID into a binary buffer.
	buf := make([]byte, traceIDEncodedSize)
	n := binary.PutVarint(buf, int64(prefix))
	n += binary.PutVarint(buf[n:], int64(code))
	n += binary.PutVarint(buf[n:], t.UnixNano())

	// Calculate the number of random bytes needed.
	randomBytesSize := traceIDEncodedSize - n

	// Generate the random bytes.
	randomness := make([]byte, randomBytesSize)
	err := binary.Read(randSource, binary.BigEndian, randomness)
	if err != nil {
		return traceID{}, fmt.Errorf("failed to generate random bytes from os; reason: %w", err)
	}

	// Combine the values and random bytes to form the encoded trace ID buffer.
	buf = append(buf[:n], randomness...)

	return traceID{
		prefix:           prefix,
		code:             code,
		time:             t,
		randomizedSuffix: randomness,
		serialized:       buf,
	}, nil
}

// traceID represents a trace-id as defined by the [W3c specification], and
// used by w3c, b3 and jaeger propagators. See Considerations for trace-id field [generation]
// for more information.
//
// [W3c specification]: https://www.w3.org/TR/trace-context/#trace-id
// [generation]: https://www.w3.org/TR/trace-context/#considerations-for-trace-id-field-generation
type traceID struct {
	// prefix is the first 2 bytes of the trace-id, and is used to identify the
	// vendor of the trace-id.
	prefix int16

	// code is the third byte of the trace-id, and is used to identify the
	// vendor's specific trace-id format.
	code int8

	// time is the time at which the trace-id was generated.
	//
	// The time component is used as a source of randomness, and to ensure
	// uniqueness of the trace-id.
	//
	// When encoded, it should be in a format occupying the last 8 bytes of
	// the trace-id, and should ideally be encoded as nanoseconds.
	time time.Time

	// randomizedSuffix holds the random bytes suffix that are used to ensure uniqueness of the
	// trace-id.
	randomizedSuffix []byte

	// serialized holds the serialized trace-id.
	serialized []byte
}

// Encode encodes the TraceID into a hex string.
//
// The trace id is first encoded as a 16 bytes sequence, as follows:
// 1. Up to 2 bytes are encoded as the Prefix
// 2. The third byte is the Code.
// 3. Up to the following 8 bytes are UnixTimestampNano.
// 4. The remaining bytes are filled with random bytes.
//
// The resulting 16 bytes sequence is then encoded as a hex string.
func (t traceID) Encode() (string, error) {
	if t.serialized == nil {
		return "", fmt.Errorf("serialized traceID is not set")
	}

	if len(t.serialized) != traceIDEncodedSize {
		return "", fmt.Errorf("serialized traceID has incorrect length: %d", len(t.serialized))
	}

	return hex.EncodeToString(t.serialized), nil
}
