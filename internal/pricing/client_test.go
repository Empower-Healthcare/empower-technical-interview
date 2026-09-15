package pricing

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/empower-healthcare/parcellab/internal/correlation"
)

func newTestClient(t *testing.T, url string, timeout time.Duration) (*Client, Metrics) {
	t.Helper()
	metrics := NewMetrics(prometheus.NewRegistry())
	return NewClient(url, timeout, metrics), metrics
}

func TestQuoteSuccessForwardsCorrelationID(t *testing.T) {
	var (
		gotHeader string
		gotBody   map[string]any
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get(correlation.HTTPHeader)
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_, _ = w.Write([]byte(`{"amount_cents":700,"currency":"USD"}`))
	}))
	defer srv.Close()

	client, metrics := newTestClient(t, srv.URL, time.Second)
	ctx := correlation.WithID(t.Context(), "req_test_1")
	money, err := client.Quote(ctx, 1500)
	if err != nil {
		t.Fatal(err)
	}
	if money.AmountCents != 700 || money.Currency != "USD" {
		t.Fatalf("unexpected money %+v", money)
	}
	if gotHeader != "req_test_1" {
		t.Fatalf("correlation header not forwarded, got %q", gotHeader)
	}
	if gotBody["weight_grams"] != float64(1500) {
		t.Fatalf("unexpected request body %v", gotBody)
	}
	if n := testutil.ToFloat64(metrics.requests.WithLabelValues(string(outcomeOK))); n != 1 {
		t.Fatalf("ok counter = %v, want 1", n)
	}
}

func TestQuoteTimesOutWithinBound(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-release // hold the response until the test finishes
	}))
	defer srv.Close()
	defer close(release)

	client, metrics := newTestClient(t, srv.URL, 50*time.Millisecond)
	start := time.Now()
	_, err := client.Quote(t.Context(), 1500)
	if !errors.Is(err, ErrTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want ErrTimeout wrapping context.DeadlineExceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("call was not bounded by client timeout, took %s", elapsed)
	}
	if n := testutil.ToFloat64(metrics.requests.WithLabelValues(string(outcomeTimeout))); n != 1 {
		t.Fatalf("timeout counter = %v, want 1", n)
	}
}

// TestQuoteTimesOutReadingBody covers the other half of the deadline: the
// pricing service answered with headers, then stalled. That must still be a
// timeout, not "unavailable".
func TestQuoteTimesOutReadingBody(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"amount_cents":`))
		w.(http.Flusher).Flush()
		<-release
	}))
	defer srv.Close()
	defer close(release)

	client, metrics := newTestClient(t, srv.URL, 50*time.Millisecond)
	_, err := client.Quote(t.Context(), 1500)
	if !errors.Is(err, ErrTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want ErrTimeout wrapping context.DeadlineExceeded, got %v", err)
	}
	if n := testutil.ToFloat64(metrics.requests.WithLabelValues(string(outcomeTimeout))); n != 1 {
		t.Fatalf("timeout counter = %v, want 1", n)
	}
}

// TestQuoteCallerCancellationIsNotATimeout: the caller giving up must surface
// as context.Canceled so the gRPC layer reports Canceled, not DeadlineExceeded
// or Unavailable, and must not count against the pricing service.
func TestQuoteCallerCancellationIsNotATimeout(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(started)
		<-release
	}))
	defer srv.Close()
	defer close(release)

	client, metrics := newTestClient(t, srv.URL, time.Minute)
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		<-started
		cancel()
	}()
	_, err := client.Quote(ctx, 1500)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if errors.Is(err, ErrTimeout) || errors.Is(err, ErrUnavailable) {
		t.Fatalf("cancellation must not be classified as timeout/unavailable, got %v", err)
	}
	if n := testutil.ToFloat64(metrics.requests.WithLabelValues(string(outcomeCanceled))); n != 1 {
		t.Fatalf("canceled counter = %v, want 1", n)
	}
}

func TestQuoteClassifiesResponses(t *testing.T) {
	cases := []struct {
		name        string
		status      int
		body        string
		wantErr     error
		wantOutcome outcome
	}{
		{name: "server error", status: http.StatusInternalServerError, body: "boom", wantErr: ErrUnavailable, wantOutcome: outcomeUnavailable},
		{name: "bad request", status: http.StatusBadRequest, body: `{"detail":"weight_grams required"}`, wantErr: ErrRejected, wantOutcome: outcomeRejected},
		{name: "validation error", status: http.StatusUnprocessableEntity, body: `{"detail":"weight out of range"}`, wantErr: ErrRejected, wantOutcome: outcomeRejected},
		{name: "not found is not a rejection", status: http.StatusNotFound, body: `{"detail":"Not Found"}`, wantErr: ErrUnavailable, wantOutcome: outcomeUnavailable},
		{name: "rate limited is not a rejection", status: http.StatusTooManyRequests, body: ``, wantErr: ErrUnavailable, wantOutcome: outcomeUnavailable},
		{name: "garbage body", status: http.StatusOK, body: `not json`, wantErr: ErrUnavailable, wantOutcome: outcomeUnavailable},
		{name: "missing currency", status: http.StatusOK, body: `{"amount_cents":700}`, wantErr: ErrUnavailable, wantOutcome: outcomeUnavailable},
		{name: "missing amount", status: http.StatusOK, body: `{"currency":"USD"}`, wantErr: ErrUnavailable, wantOutcome: outcomeUnavailable},
		{name: "null amount", status: http.StatusOK, body: `{"amount_cents":null,"currency":"USD"}`, wantErr: ErrUnavailable, wantOutcome: outcomeUnavailable},
		{name: "negative amount", status: http.StatusOK, body: `{"amount_cents":-1,"currency":"USD"}`, wantErr: ErrUnavailable, wantOutcome: outcomeUnavailable},
		{name: "lowercase currency", status: http.StatusOK, body: `{"amount_cents":700,"currency":"usd"}`, wantErr: ErrUnavailable, wantOutcome: outcomeUnavailable},
		{name: "explicit zero is accepted", status: http.StatusOK, body: `{"amount_cents":0,"currency":"USD"}`, wantErr: nil, wantOutcome: outcomeOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(sub *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			client, metrics := newTestClient(sub, srv.URL, time.Second)
			_, err := client.Quote(sub.Context(), 1500)
			if !errors.Is(err, tc.wantErr) {
				sub.Fatalf("Quote error = %v, want %v", err, tc.wantErr)
			}
			if n := testutil.ToFloat64(metrics.requests.WithLabelValues(string(tc.wantOutcome))); n != 1 {
				sub.Fatalf("%s counter = %v, want 1", tc.wantOutcome, n)
			}
		})
	}
}

func TestQuoteConnectionRefusedIsUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()

	client, _ := newTestClient(t, url, time.Second)
	if _, err := client.Quote(t.Context(), 1500); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("want ErrUnavailable, got %v", err)
	}
}
