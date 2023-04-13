package tracing

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func TestNewTraceID(t *testing.T) {
	t.Parallel()

	// Dummy random source for consistent tests.
	dummyRandom := bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A})

	tests := []struct {
		name      string
		prefix    int16
		code      int8
		t         time.Time
		randSrc   io.Reader
		want      traceID
		wantErr   bool
		errString string
	}{
		{
			name:    "Valid k6CloudCode",
			prefix:  k6Prefix,
			code:    k6CloudCode,
			t:       time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			randSrc: dummyRandom,
			want: traceID{
				prefix:           k6Prefix,
				code:             k6CloudCode,
				time:             time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				randomizedSuffix: []byte{0x01, 0x02, 0x03, 0x04},
			},
			wantErr: false,
		},
		{
			name:      "Invalid prefix",
			prefix:    0o1,
			code:      k6CloudCode,
			t:         time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			randSrc:   dummyRandom,
			wantErr:   true,
			errString: "invalid prefix 0o1, expected 0o756",
		},
		{
			name:      "Invalid code",
			prefix:    k6Prefix,
			code:      99,
			t:         time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			randSrc:   dummyRandom,
			wantErr:   true,
			errString: "invalid code 0o99, accepted values are 0o12 and 0o33",
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := newTraceID(tt.prefix, tt.code, tt.t, tt.randSrc)

			if (err != nil) != tt.wantErr {
				t.Errorf("newTraceID error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr && err.Error() != tt.errString {
				t.Errorf("newTraceID error string = %v, expected %v", err.Error(), tt.errString)
				return
			}

			if !tt.wantErr {
				if got.prefix != tt.want.prefix || got.code != tt.want.code || !got.time.Equal(tt.want.time) {
					t.Errorf("newTraceID got = %v, want %v", got, tt.want)
				}
				if !bytes.Equal(got.randomizedSuffix, tt.want.randomizedSuffix) {
					t.Errorf("newTraceID randomizedSuffix = %v, want %v", got.randomizedSuffix, tt.want.randomizedSuffix)
				}
			}
		})
	}
}

func TestTraceID_Encode(t *testing.T) {
	t.Parallel()

	// Sample serialized traceID for testing
	sampleSerialized := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}

	tests := []struct {
		name    string
		traceID traceID
		want    string
		wantErr bool
	}{
		{
			name: "Valid traceID",
			traceID: traceID{
				serialized: sampleSerialized,
			},
			want:    "0102030405060708090a0b0c0d0e0f10",
			wantErr: false,
		},
		{
			name: "Empty serialized traceID",
			traceID: traceID{
				serialized: nil,
			},
			wantErr: true,
		},
		{
			name: "Incorrect length serialized traceID",
			traceID: traceID{
				serialized: []byte{0x01, 0x02, 0x03},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tt.traceID.Encode()

			if (err != nil) != tt.wantErr {
				t.Errorf("traceID.Encode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && got != tt.want {
				t.Errorf("traceID.Encode() got = %v, want %v", got, tt.want)
			}
		})
	}
}
