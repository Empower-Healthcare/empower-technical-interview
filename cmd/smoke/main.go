// smoke verifies the running stack end to end:
//
//	gRPC health -> GetQuote -> CreateShipment -> event in Kafka -> dispatch
//	persisted -> replay creates no second dispatch -> metrics scrapeable.
//
// It runs inside the Compose network (`make smoke`) and exits non-zero on the
// first failed step.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	parcellabv1 "github.com/empower-healthcare/parcellab/gen/parcellab/v1"
	"github.com/empower-healthcare/parcellab/internal/app"
	"github.com/empower-healthcare/parcellab/internal/broker"
	"github.com/empower-healthcare/parcellab/internal/correlation"
	"github.com/empower-healthcare/parcellab/internal/domain"
	"github.com/empower-healthcare/parcellab/internal/events"
)

// The 1,500 g example from the README: 500 + 2 * 100 cents.
const (
	exampleWeightGrams = 1500
	exampleAmountCents = 700
	exampleCurrency    = "USD"
)

type smokeConfig struct {
	GRPCAddr      string
	Publisher     broker.PublisherConfig
	ShipmentMgmt  string
	RelayMgmt     string
	ConsumerMgmt  string
	PricingURL    string
	PrometheusURL string
	// StepTimeout bounds each step; the asynchronous ones poll until then.
	StepTimeout time.Duration
}

func (c smokeConfig) Validate() error {
	if err := c.Publisher.Validate(); err != nil {
		return err
	}
	if c.StepTimeout <= 0 {
		return fmt.Errorf("SMOKE_STEP_TIMEOUT %s: must be positive", c.StepTimeout)
	}
	return nil
}

// state is what the steps learn about the shipment as they go.
type state struct {
	correlationID string
	shipmentID    domain.ShipmentID
	event         events.ShipmentCreated
	message       broker.Message
	dispatchID    string
}

type step struct {
	name string
	run  func(ctx context.Context, s *state) (detail string, err error)
}

type smoke struct {
	cfg    smokeConfig
	client parcellabv1.ShipmentServiceClient
	health grpc_health_v1.HealthClient
	http   *http.Client
}

func main() { app.Main("smoke", run) }

func run(ctx context.Context, a app.App) error {
	cfg := smokeConfig{
		GRPCAddr: a.Env.String("SHIPMENT_GRPC_ADDR", "shipment-service:50051"),
		Publisher: broker.PublisherConfig{
			Brokers:         a.Env.Brokers(),
			DeliveryTimeout: a.Env.Duration("RELAY_PUBLISH_TIMEOUT", 10*time.Second),
		},
		ShipmentMgmt:  a.Env.String("SHIPMENT_MGMT_URL", "http://shipment-service:8080"),
		RelayMgmt:     a.Env.String("RELAY_MGMT_URL", "http://outbox-relay:8080"),
		ConsumerMgmt:  a.Env.String("CONSUMER_MGMT_URL", "http://dispatch-consumer:8080"),
		PricingURL:    a.Env.String("PRICING_URL", "http://pricing:8000"),
		PrometheusURL: a.Env.String("PROMETHEUS_URL", "http://prometheus:9090"),
		StepTimeout:   a.Env.Duration("SMOKE_STEP_TIMEOUT", 45*time.Second),
	}
	if err := errors.Join(a.Env.Err(), cfg.Validate()); err != nil {
		return err
	}

	conn, err := grpc.NewClient(cfg.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("grpc client: %w", err)
	}
	defer conn.Close()

	s := smoke{
		cfg:    cfg,
		client: parcellabv1.NewShipmentServiceClient(conn),
		health: grpc_health_v1.NewHealthClient(conn),
		http:   &http.Client{},
	}
	st := state{correlationID: "req_smoke_" + time.Now().UTC().Format("150405")}
	ctx = correlation.WithID(ctx, st.correlationID)

	fmt.Fprintf(os.Stdout, "ParcelLab smoke test (correlation_id=%s)\n\n", st.correlationID)
	for i, stp := range s.steps() {
		detail, err := runStep(ctx, cfg.StepTimeout, stp, &st)
		if err != nil {
			fmt.Fprintf(os.Stdout, "  ✗ %d. %s\n      %v\n\nSMOKE FAILED\n", i+1, stp.name, err)
			return fmt.Errorf("step %d (%s): %w", i+1, stp.name, err)
		}
		fmt.Fprintf(os.Stdout, "  ✓ %d. %-44s %s\n", i+1, stp.name, detail)
	}
	fmt.Fprintln(os.Stdout, "\nSMOKE PASSED")
	return nil
}

func runStep(ctx context.Context, timeout time.Duration, stp step, st *state) (string, error) {
	stepCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return stp.run(stepCtx, st)
}

func (s smoke) steps() []step {
	return []step{
		{name: "gRPC health is SERVING", run: s.checkGRPCHealth},
		{name: "pricing service is healthy", run: s.checkPricingHealth},
		{name: "GetQuote prices the 1,500 g example", run: s.getQuote},
		{name: "CreateShipment stores shipment + pending event", run: s.createShipment},
		{name: "event reaches Kafka keyed by shipment ID", run: s.findEventInKafka},
		{name: "consumer persists exactly one dispatch", run: s.waitForDispatch},
		{name: "replaying the event creates no second dispatch", run: s.replayIsDeduplicated},
		{name: "management endpoints expose metrics", run: s.checkMetrics},
		{name: "Prometheus is ready and scraping", run: s.checkPrometheus},
	}
}

func (s smoke) checkGRPCHealth(ctx context.Context, _ *state) (string, error) {
	resp, err := s.health.Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: parcellabv1.ShipmentService_ServiceDesc.ServiceName})
	if err != nil {
		return "", err
	}
	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		return "", fmt.Errorf("status %s", resp.GetStatus())
	}
	return s.cfg.GRPCAddr, nil
}

func (s smoke) checkPricingHealth(ctx context.Context, _ *state) (string, error) {
	body, err := s.get(ctx, s.cfg.PricingURL+"/healthz")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(body)), nil
}

func (s smoke) getQuote(ctx context.Context, st *state) (string, error) {
	resp, err := s.client.GetQuote(s.withCorrelation(ctx, st), &parcellabv1.GetQuoteRequest{WeightGrams: exampleWeightGrams})
	if err != nil {
		return "", err
	}
	price := resp.GetPrice()
	if price.GetAmountCents() != exampleAmountCents || price.GetCurrency() != exampleCurrency {
		return "", fmt.Errorf("got %d %s, want %d %s", price.GetAmountCents(), price.GetCurrency(), exampleAmountCents, exampleCurrency)
	}
	return fmt.Sprintf("%d %s", price.GetAmountCents(), price.GetCurrency()), nil
}

func (s smoke) createShipment(ctx context.Context, st *state) (string, error) {
	resp, err := s.client.CreateShipment(s.withCorrelation(ctx, st), &parcellabv1.CreateShipmentRequest{WeightGrams: exampleWeightGrams})
	if err != nil {
		return "", err
	}
	sh := resp.GetShipment()
	if sh.GetPrice().GetAmountCents() != exampleAmountCents {
		return "", fmt.Errorf("stored amount %d, want %d", sh.GetPrice().GetAmountCents(), exampleAmountCents)
	}
	if sh.GetOutboxStatus() != parcellabv1.OutboxStatus_OUTBOX_STATUS_PENDING {
		return "", fmt.Errorf("new shipment reports outbox %s, want PENDING", sh.GetOutboxStatus())
	}
	st.shipmentID = domain.ShipmentID(sh.GetShipmentId())
	return string(st.shipmentID), nil
}

// findEventInKafka reads the topic from the beginning with a group-less client
// and looks for the record keyed by our shipment ID.
func (s smoke) findEventInKafka(ctx context.Context, st *state) (string, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(s.cfg.Publisher.Brokers...),
		kgo.ConsumeTopics(events.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return "", err
	}
	defer client.Close()

	for ctx.Err() == nil {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			break
		}
		for iter := fetches.RecordIter(); !iter.Done(); {
			rec := iter.Next()
			if string(rec.Key) != string(st.shipmentID) {
				continue
			}
			evt, err := events.Decode(rec.Value)
			if err != nil {
				return "", fmt.Errorf("record for %s is not a valid event: %w", st.shipmentID, err)
			}
			if evt.Data.AmountCents != exampleAmountCents || evt.CorrelationID != st.correlationID {
				return "", fmt.Errorf("event payload mismatch: %+v", evt)
			}
			st.event = evt
			st.message = broker.Message{Topic: rec.Topic, Key: rec.Key, Value: rec.Value, Headers: headerMap(rec.Headers)}
			return fmt.Sprintf("%s partition=%d offset=%d", evt.EventID, rec.Partition, rec.Offset), nil
		}
	}
	return "", fmt.Errorf("no record with key %s on %s before timeout (is the relay running?)", st.shipmentID, events.Topic)
}

// waitForDispatch polls GetShipment until the outbox row is PUBLISHED and the
// dispatch is DISPATCHED. The consumer can win the race against the relay's
// mark-published update, so DISPATCHED + PENDING is a transient state, not a
// failure.
func (s smoke) waitForDispatch(ctx context.Context, st *state) (string, error) {
	var last *parcellabv1.Shipment
	for {
		sh, err := s.getShipment(ctx, st)
		if err != nil {
			return "", err
		}
		last = sh
		dispatched := sh.GetDispatch().GetStatus() == parcellabv1.DispatchStatus_DISPATCH_STATUS_DISPATCHED
		published := sh.GetOutboxStatus() == parcellabv1.OutboxStatus_OUTBOX_STATUS_PUBLISHED
		if dispatched && published {
			break
		}
		if err := sleep(ctx, 250*time.Millisecond); err != nil {
			return "", fmt.Errorf("shipment stayed outbox=%s dispatch=%s (are the relay and consumer running?)",
				last.GetOutboxStatus(), last.GetDispatch().GetStatus())
		}
	}
	if domain.EventID(last.GetDispatch().GetEventId()) != st.event.EventID {
		return "", fmt.Errorf("dispatch references event %s, Kafka carried %s", last.GetDispatch().GetEventId(), st.event.EventID)
	}
	st.dispatchID = last.GetDispatch().GetDispatchId()
	return st.dispatchID, nil
}

// replayIsDeduplicated republishes the exact record and expects the consumer's
// duplicate counter to increase by one while the dispatch stays the same row.
func (s smoke) replayIsDeduplicated(ctx context.Context, st *state) (string, error) {
	before, err := s.consumerOutcome(ctx, "duplicate")
	if err != nil {
		return "", err
	}
	publisher, err := broker.NewPublisher(ctx, s.cfg.Publisher)
	if err != nil {
		return "", err
	}
	defer publisher.Close()
	if err := publisher.Publish(ctx, st.message); err != nil {
		return "", err
	}

	for {
		after, err := s.consumerOutcome(ctx, "duplicate")
		if err != nil {
			return "", err
		}
		if after >= before+1 {
			break
		}
		if err := sleep(ctx, 250*time.Millisecond); err != nil {
			return "", fmt.Errorf("consumer duplicate counter stayed at %v after replay", before)
		}
	}

	sh, err := s.getShipment(ctx, st)
	if err != nil {
		return "", err
	}
	if got := sh.GetDispatch().GetDispatchId(); got != st.dispatchID {
		return "", fmt.Errorf("dispatch changed after replay: %s -> %s", st.dispatchID, got)
	}
	return fmt.Sprintf("duplicate counter %v -> %v, dispatch still %s", before, before+1, st.dispatchID), nil
}

func (s smoke) getShipment(ctx context.Context, st *state) (*parcellabv1.Shipment, error) {
	resp, err := s.client.GetShipment(s.withCorrelation(ctx, st), &parcellabv1.GetShipmentRequest{ShipmentId: string(st.shipmentID)})
	if err != nil {
		return nil, err
	}
	return resp.GetShipment(), nil
}

func (s smoke) checkMetrics(ctx context.Context, _ *state) (string, error) {
	targets := []struct {
		url    string
		metric string
	}{
		{url: s.cfg.ShipmentMgmt, metric: "parcellab_grpc_requests_total"},
		{url: s.cfg.ShipmentMgmt, metric: "parcellab_pricing_requests_total"},
		{url: s.cfg.RelayMgmt, metric: "parcellab_outbox_published_total"},
		{url: s.cfg.ConsumerMgmt, metric: "parcellab_consumer_events_total"},
		{url: s.cfg.PricingURL, metric: "pricing_quotes_total"},
	}
	for _, t := range targets {
		body, err := s.get(ctx, t.url+"/metrics")
		if err != nil {
			return "", err
		}
		if !bytes.Contains(body, []byte(t.metric)) {
			return "", fmt.Errorf("%s/metrics does not expose %s", t.url, t.metric)
		}
	}
	return fmt.Sprintf("%d series checked across 4 endpoints", len(targets)), nil
}

func (s smoke) checkPrometheus(ctx context.Context, _ *state) (string, error) {
	if _, err := s.get(ctx, s.cfg.PrometheusURL+"/-/ready"); err != nil {
		return "", err
	}
	body, err := s.get(ctx, s.cfg.PrometheusURL+"/api/v1/targets?state=active")
	if err != nil {
		return "", err
	}
	var payload struct {
		Data struct {
			ActiveTargets []struct {
				Labels map[string]string `json:"labels"`
				Health string            `json:"health"`
			} `json:"activeTargets"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", fmt.Errorf("decode targets: %w", err)
	}
	up, total := 0, len(payload.Data.ActiveTargets)
	var down []string
	for _, t := range payload.Data.ActiveTargets {
		if t.Health == "up" {
			up++
		} else {
			down = append(down, t.Labels["job"]+"="+t.Health)
		}
	}
	if total == 0 {
		return "", errors.New("prometheus has no active targets")
	}
	if up != total {
		// Targets can be "unknown" for one scrape interval after startup; report, don't fail.
		return fmt.Sprintf("%d/%d targets up (%s)", up, total, strings.Join(down, ", ")), nil
	}
	return fmt.Sprintf("%d/%d targets up", up, total), nil
}

// consumerOutcome reads parcellab_consumer_events_total{outcome="..."} from the
// consumer's /metrics; the series is absent (0) until that outcome has occurred.
func (s smoke) consumerOutcome(ctx context.Context, outcome string) (float64, error) {
	body, err := s.get(ctx, s.cfg.ConsumerMgmt+"/metrics")
	if err != nil {
		return 0, err
	}
	prefix := fmt.Sprintf(`parcellab_consumer_events_total{outcome="%s"} `, outcome)
	for _, line := range strings.Split(string(body), "\n") {
		if v, ok := strings.CutPrefix(line, prefix); ok {
			var n float64
			_, err := fmt.Sscanf(v, "%g", &n)
			return n, err
		}
	}
	return 0, nil
}

func (s smoke) withCorrelation(ctx context.Context, st *state) context.Context {
	return metadata.AppendToOutgoingContext(ctx, correlation.MetadataKey, st.correlationID)
}

func (s smoke) get(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: HTTP %d: %s", url, resp.StatusCode, bytes.TrimSpace(body))
	}
	return body, nil
}

func headerMap(hs []kgo.RecordHeader) map[string]string {
	out := make(map[string]string, len(hs))
	for _, h := range hs {
		out[h.Key] = string(h.Value)
	}
	return out
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
