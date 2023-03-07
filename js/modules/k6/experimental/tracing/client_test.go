package tracing

import (
	"net/http"
	"testing"

	"github.com/dop251/goja"
	"github.com/oxtoacart/bpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/js/modulestest"
	"go.k6.io/k6/lib"
	"go.k6.io/k6/lib/testutils/httpmultibin"
	"go.k6.io/k6/metrics"
)

// traceParentHeaderName is the normalized trace header name.
// Although the traceparent header is case insensitive, the
// Go http.Header sets it capitalized.
const traceparentHeaderName string = "Traceparent"

// testTraceID is a valid trace ID used in tests.
const testTraceID string = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"

func TestClientInstrumentArguments(t *testing.T) {
	t.Parallel()

	t.Run("no args should fail", func(t *testing.T) {
		t.Parallel()

		testCase := newTestCase(t)

		_, err := testCase.client.instrumentArguments(testCase.traceContextHeader)
		require.Error(t, err)
	})

	t.Run("1 arg should initialize params successfully", func(t *testing.T) {
		t.Parallel()

		testCase := newTestCase(t)
		rt := testCase.testSetup.VU.Runtime()

		gotArgs, gotErr := testCase.client.instrumentArguments(testCase.traceContextHeader, goja.Null())

		assert.NoError(t, gotErr)
		assert.Len(t, gotArgs, 2)
		assert.Equal(t, goja.Null(), gotArgs[0])

		gotParams := gotArgs[1].ToObject(rt)
		assert.NotNil(t, gotParams)
		gotHeaders := gotParams.Get("headers").ToObject(rt)
		assert.NotNil(t, gotHeaders)
		gotTraceParent := gotHeaders.Get(traceparentHeaderName)
		assert.NotNil(t, gotTraceParent)
		assert.Equal(t, testTraceID, gotTraceParent.String())
	})

	t.Run("2 args with null params should initialize it", func(t *testing.T) {
		t.Parallel()

		testCase := newTestCase(t)
		rt := testCase.testSetup.VU.Runtime()

		gotArgs, gotErr := testCase.client.instrumentArguments(testCase.traceContextHeader, goja.Null(), goja.Null())

		assert.NoError(t, gotErr)
		assert.Len(t, gotArgs, 2)
		assert.Equal(t, goja.Null(), gotArgs[0])

		gotParams := gotArgs[1].ToObject(rt)
		assert.NotNil(t, gotParams)
		gotHeaders := gotParams.Get("headers").ToObject(rt)
		assert.NotNil(t, gotHeaders)
		gotTraceParent := gotHeaders.Get(traceparentHeaderName)
		assert.NotNil(t, gotTraceParent)
		assert.Equal(t, testTraceID, gotTraceParent.String())
	})

	t.Run("2 args with undefined params should initialize it", func(t *testing.T) {
		t.Parallel()

		testCase := newTestCase(t)
		rt := testCase.testSetup.VU.Runtime()

		gotArgs, gotErr := testCase.client.instrumentArguments(testCase.traceContextHeader, goja.Null(), goja.Undefined())

		assert.NoError(t, gotErr)
		assert.Len(t, gotArgs, 2)
		assert.Equal(t, goja.Null(), gotArgs[0])

		gotParams := gotArgs[1].ToObject(rt)
		assert.NotNil(t, gotParams)
		gotHeaders := gotParams.Get("headers").ToObject(rt)
		assert.NotNil(t, gotHeaders)
		gotTraceParent := gotHeaders.Get(traceparentHeaderName)
		assert.NotNil(t, gotTraceParent)
		assert.Equal(t, testTraceID, gotTraceParent.String())
	})

	t.Run("2 args with predefined params and headers updates them successfully", func(t *testing.T) {
		t.Parallel()

		testCase := newTestCase(t)
		rt := testCase.testSetup.VU.Runtime()

		wantHeaders := rt.NewObject()
		require.NoError(t, wantHeaders.Set("X-Test-Header", "testvalue"))
		wantParams := rt.NewObject()
		require.NoError(t, wantParams.Set("headers", wantHeaders))

		gotArgs, gotErr := testCase.client.instrumentArguments(testCase.traceContextHeader, goja.Null(), wantParams)

		assert.NoError(t, gotErr)
		assert.Len(t, gotArgs, 2)
		assert.Equal(t, goja.Null(), gotArgs[0])
		assert.Equal(t, wantParams, gotArgs[1])

		gotHeaders := gotArgs[1].ToObject(rt).Get("headers").ToObject(rt)

		gotTraceParent := gotHeaders.Get(traceparentHeaderName)
		assert.NotNil(t, gotTraceParent)
		assert.Equal(t, testTraceID, gotTraceParent.String())

		gotTestHeader := gotHeaders.Get("X-Test-Header")
		assert.NotNil(t, gotTestHeader)
		assert.Equal(t, "testvalue", gotTestHeader.String())
	})

	t.Run("2 args with predefined params and no headers sets and updates them successfully", func(t *testing.T) {
		t.Parallel()

		testCase := newTestCase(t)
		rt := testCase.testSetup.VU.Runtime()
		wantParams := rt.NewObject()

		gotArgs, gotErr := testCase.client.instrumentArguments(testCase.traceContextHeader, goja.Null(), wantParams)

		assert.NoError(t, gotErr)
		assert.Len(t, gotArgs, 2)
		assert.Equal(t, goja.Null(), gotArgs[0])
		assert.Equal(t, wantParams, gotArgs[1])

		gotHeaders := gotArgs[1].ToObject(rt).Get("headers").ToObject(rt)

		gotTraceParent := gotHeaders.Get(traceparentHeaderName)
		assert.NotNil(t, gotTraceParent)
		assert.Equal(t, testTraceID, gotTraceParent.String())
	})
}

func TestClientInstrumentedCall(t *testing.T) {
	t.Parallel()

	testCase := newTestCase(t)
	testCase.testSetup.MoveToVUContext(&lib.State{
		Tags: lib.NewVUStateTags(&metrics.TagSet{}),
	})
	testCase.client.propagator = &W3CPropagator{}

	fn := func(args ...goja.Value) error {
		gotMetadataTraceID, gotTraceIDKey := testCase.client.vu.State().Tags.GetCurrentValues().Metadata["trace_id"]
		assert.True(t, gotTraceIDKey)
		assert.NotEmpty(t, gotMetadataTraceID)
		return nil
	}

	_, hasTraceIDKey := testCase.client.vu.State().Tags.GetCurrentValues().Metadata["trace_id"]
	assert.False(t, hasTraceIDKey)
	_ = testCase.client.instrumentedCall(fn)
}

func TestSimpleCall(t *testing.T) {
	t.Parallel()

	testCase := newTestSetup(t)
	rt := testCase.TestRuntime.VU.Runtime()
	// Calling in the VU context should fail
	_, err := testCase.TestRuntime.VU.Runtime().RunString(`
		instrumentHTTP({propagator: 'w3c'})
        let http = require('k6/http')
	`)
	require.NoError(t, err)

	samples := make(chan metrics.SampleContainer, 1000)
	tb := httpmultibin.NewHTTPMultiBin(t)
	testCase.TestRuntime.MoveToVUContext(&lib.State{
		BuiltinMetrics: metrics.RegisterBuiltinMetrics(testCase.TestRuntime.VU.InitEnvField.Registry),
		Tags:           lib.NewVUStateTags(testCase.TestRuntime.VU.InitEnvField.Registry.RootTagSet()),
		Transport:      tb.HTTPTransport,
		BPool:          bpool.NewBufferPool(1),
		Samples:        samples,
		Options:        lib.Options{SystemTags: &metrics.DefaultSystemTagSet},
	})
	err = rt.Set("f", func(expected bool, expectedTraceID string) {
		traceID, gotTraceID := testCase.TestRuntime.VU.State().Tags.GetCurrentValues().Metadata["trace_id"]
		require.Equal(t, expected, gotTraceID)
		if expectedTraceID != "" {
			require.Equal(t, expectedTraceID, traceID)
		}
	})
	require.NoError(t, err)

	t.Cleanup(testCase.TestRuntime.EventLoop.WaitOnRegistered)
	err = testCase.TestRuntime.EventLoop.Start(func() error {
		_, err = rt.RunString(tb.Replacer.Replace(`
        f(false)
        http.asyncRequest("GET", "HTTPBIN_URL" )
        f(false)
        `))
		return err
	})
	require.NoError(t, err)
	close(samples)

	var sampleRead bool
	for sampleContainer := range samples {
		for _, sample := range sampleContainer.GetSamples() {
			require.NotEmpty(t, sample.Metadata["trace_id"])
			sampleRead = true
		}
	}
	require.True(t, sampleRead)
}

type tracingClientTestCase struct {
	t                  *testing.T
	testSetup          *modulestest.Runtime
	client             Client
	traceContextHeader http.Header
}

func newTestCase(t *testing.T) *tracingClientTestCase {
	testSetup := modulestest.NewRuntime(t)
	client := Client{vu: testSetup.VU}
	traceContextHeader := http.Header{}
	traceContextHeader.Add(traceparentHeaderName, testTraceID)

	return &tracingClientTestCase{
		t:                  t,
		testSetup:          testSetup,
		client:             client,
		traceContextHeader: traceContextHeader,
	}
}
