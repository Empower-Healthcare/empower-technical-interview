package observability

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/empower-healthcare/parcellab/internal/correlation"
)

func TestLoggerAddsCorrelationIDFromContext(t *testing.T) {
	cases := []struct {
		name          string
		correlationID string
		wantPresent   bool
	}{
		{name: "id in context", correlationID: "req_abc", wantPresent: true},
		{name: "no id in context", correlationID: "", wantPresent: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(sub *testing.T) {
			var buf bytes.Buffer
			logger := NewLogger("test-service", &buf, slog.LevelInfo)

			ctx := sub.Context()
			if tc.correlationID != "" {
				ctx = correlation.WithID(ctx, tc.correlationID)
			}
			logger.InfoContext(ctx, "hello", "shipment_id", "shp_1")

			var line map[string]any
			if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
				sub.Fatalf("log line is not JSON: %v\n%s", err, buf.String())
			}
			if line["service"] != "test-service" || line["shipment_id"] != "shp_1" {
				sub.Fatalf("expected service and explicit attrs, got %v", line)
			}
			got, present := line[correlation.LogKey]
			if present != tc.wantPresent {
				sub.Fatalf("correlation_id present=%v, want %v (line %v)", present, tc.wantPresent, line)
			}
			if present && got != tc.correlationID {
				sub.Fatalf("correlation_id = %v, want %q", got, tc.correlationID)
			}
		})
	}
}
