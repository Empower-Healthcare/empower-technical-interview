// Package pricing is the HTTP/JSON client for the Python pricing service.
//
// It is the only place in the Go code that knows the pricing service's wire
// format, so a contract change (a new request field, for example) is made here
// and in pricing/app/main.py.
package pricing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/empower-healthcare/parcellab/internal/correlation"
	"github.com/empower-healthcare/parcellab/internal/domain"
)

var (
	// ErrUnavailable: the pricing service could not be reached, answered with
	// an unexpected status, or returned a response that violates the contract.
	ErrUnavailable = errors.New("pricing service unavailable")
	// ErrTimeout: the bounded downstream deadline (or the caller's) elapsed.
	// The wrapped chain also contains context.DeadlineExceeded.
	ErrTimeout = errors.New("pricing service timed out")
	// ErrRejected: the pricing service answered 400 or 422, i.e. it judged the
	// request invalid. The Go service validates input first, so this normally
	// means the two services disagree on the contract. Other 4xx statuses (401,
	// 404, 429, ...) say nothing about the parcel and are ErrUnavailable.
	ErrRejected = errors.New("pricing service rejected the request")
)

// outcome is the bounded label set for the request counter.
type outcome string

const (
	outcomeOK          outcome = "ok"
	outcomeRejected    outcome = "rejected"
	outcomeUnavailable outcome = "unavailable"
	outcomeTimeout     outcome = "timeout"
	outcomeCanceled    outcome = "canceled" // the caller gave up; not a pricing-service failure
)

// Metrics are the pricing client's Prometheus series.
type Metrics struct {
	requests *prometheus.CounterVec
	duration prometheus.Histogram
}

// NewMetrics creates and registers the client metrics.
func NewMetrics(reg prometheus.Registerer) Metrics {
	m := Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parcellab_pricing_requests_total",
			Help: "Calls from the shipment service to the pricing service, by outcome (ok, rejected, unavailable, timeout, canceled).",
		}, []string{"outcome"}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "parcellab_pricing_request_duration_seconds",
			Help:    "Latency of pricing service calls.",
			Buckets: prometheus.DefBuckets,
		}),
	}
	reg.MustRegister(m.requests, m.duration)
	return m
}

func (m Metrics) observe(o outcome, elapsed time.Duration) {
	m.requests.WithLabelValues(string(o)).Inc()
	m.duration.Observe(elapsed.Seconds())
}

// quoteRequest is the JSON body of POST /quote. DeliverySpeed is always sent,
// so the pricing service never applies its own default.
type quoteRequest struct {
	WeightGrams   int32  `json:"weight_grams"`
	DeliverySpeed string `json:"delivery_speed"`
}

// Wire values of delivery_speed. Keep in sync with pricing/app/pricing.py.
const (
	wireStandard = "standard"
	wireExpress  = "express"
)

func wireDeliverySpeed(speed domain.DeliverySpeed) (string, error) {
	switch speed {
	case domain.DeliveryStandard:
		return wireStandard, nil
	case domain.DeliveryExpress:
		return wireExpress, nil
	default:
		return "", fmt.Errorf("%w: got %d", domain.ErrInvalidDeliverySpeed, speed)
	}
}

// quoteResponse is the JSON body returned by POST /quote. The fields are
// pointers only here, at the wire boundary, so an absent or null amount is
// distinguishable from a genuine zero.
type quoteResponse struct {
	AmountCents *int64  `json:"amount_cents"`
	Currency    *string `json:"currency"`
}

// Client calls the pricing service with a bounded per-call timeout.
type Client struct {
	baseURL string
	timeout time.Duration
	http    *http.Client
	metrics Metrics
}

// NewClient returns a Client. timeout bounds every call regardless of the
// caller's own deadline; the shorter of the two wins.
func NewClient(baseURL string, timeout time.Duration, metrics Metrics) *Client {
	return &Client{
		baseURL: baseURL,
		timeout: timeout,
		http:    &http.Client{}, // deadlines come from ctx, not from the client
		metrics: metrics,
	}
}

// Quote prices a parcel. The correlation ID from ctx is forwarded as an HTTP header.
// An unsupported speed fails before the call and is not counted in the metrics.
func (c *Client) Quote(ctx context.Context, weightGrams int32, speed domain.DeliverySpeed) (domain.Money, error) {
	wireSpeed, err := wireDeliverySpeed(speed)
	if err != nil {
		return domain.Money{}, err
	}
	start := time.Now()
	money, o, err := c.quote(ctx, quoteRequest{WeightGrams: weightGrams, DeliverySpeed: wireSpeed})
	c.metrics.observe(o, time.Since(start))
	return money, err
}

func (c *Client) quote(ctx context.Context, in quoteRequest) (domain.Money, outcome, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	body, err := json.Marshal(in)
	if err != nil {
		return domain.Money{}, outcomeUnavailable, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/quote", bytes.NewReader(body))
	if err != nil {
		return domain.Money{}, outcomeUnavailable, err
	}
	req.Header.Set("Content-Type", "application/json")
	if id := correlation.FromContext(ctx); id != "" {
		req.Header.Set(correlation.HTTPHeader, id)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		o, err := classifyTransport(ctx, "request", err)
		return domain.Money{}, o, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		// Headers arrived but the body stalled: the same deadline rules apply.
		o, err := classifyTransport(ctx, "read body", err)
		return domain.Money{}, o, err
	}

	return decodeQuote(resp.StatusCode, raw)
}

// classifyTransport turns a failed Do/ReadAll into the caller-facing error.
// The context error is kept in the chain so callers can distinguish the
// caller's own cancellation (context.Canceled, left unwrapped by ErrTimeout)
// from the bounded downstream deadline (ErrTimeout + context.DeadlineExceeded).
func classifyTransport(ctx context.Context, stage string, err error) (outcome, error) {
	switch ctxErr := ctx.Err(); {
	case errors.Is(ctxErr, context.DeadlineExceeded):
		return outcomeTimeout, fmt.Errorf("%w: %s: %w", ErrTimeout, stage, ctxErr)
	case ctxErr != nil:
		return outcomeCanceled, fmt.Errorf("%s: %w", stage, ctxErr)
	default:
		return outcomeUnavailable, fmt.Errorf("%w: %s: %w", ErrUnavailable, stage, err)
	}
}

// decodeQuote maps an HTTP response to a Money value or a classified error.
func decodeQuote(status int, raw []byte) (domain.Money, outcome, error) {
	switch {
	case status == http.StatusBadRequest || status == http.StatusUnprocessableEntity:
		return domain.Money{}, outcomeRejected, fmt.Errorf("%w: HTTP %d: %s", ErrRejected, status, bytes.TrimSpace(raw))
	case status != http.StatusOK:
		return domain.Money{}, outcomeUnavailable, fmt.Errorf("%w: HTTP %d", ErrUnavailable, status)
	}

	var out quoteResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return domain.Money{}, outcomeUnavailable, fmt.Errorf("%w: decode response: %w", ErrUnavailable, err)
	}
	if out.AmountCents == nil || out.Currency == nil {
		return domain.Money{}, outcomeUnavailable, fmt.Errorf("%w: response is missing amount_cents or currency: %s", ErrUnavailable, bytes.TrimSpace(raw))
	}
	money := domain.Money{AmountCents: *out.AmountCents, Currency: *out.Currency}
	if err := domain.ValidateMoney(money); err != nil {
		return domain.Money{}, outcomeUnavailable, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return money, outcomeOK, nil
}
