package correlation

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestFromContextWithoutIDIsEmpty(t *testing.T) {
	if got := FromContext(t.Context()); got != "" {
		t.Fatalf("FromContext on a bare context = %q, want empty", got)
	}
}

func TestUnaryServerInterceptor(t *testing.T) {
	cases := []struct {
		name       string
		incoming   metadata.MD
		wantExact  string // "" means "any generated req_ id"
		wantPrefix string
	}{
		{name: "propagates caller id", incoming: metadata.Pairs(MetadataKey, "req_from_caller"), wantExact: "req_from_caller"},
		{name: "generates id when absent", incoming: metadata.MD{}, wantPrefix: "req_"},
		{name: "generates id when empty", incoming: metadata.Pairs(MetadataKey, ""), wantPrefix: "req_"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(sub *testing.T) {
			ctx := metadata.NewIncomingContext(sub.Context(), tc.incoming)

			var seen string
			handler := func(ctx context.Context, _ any) (any, error) {
				seen = FromContext(ctx)
				return nil, nil
			}
			if _, err := UnaryServerInterceptor()(ctx, nil, &grpc.UnaryServerInfo{}, handler); err != nil {
				sub.Fatal(err)
			}

			switch {
			case tc.wantExact != "" && seen != tc.wantExact:
				sub.Fatalf("handler saw %q, want %q", seen, tc.wantExact)
			case tc.wantPrefix != "" && (len(seen) <= len(tc.wantPrefix) || seen[:len(tc.wantPrefix)] != tc.wantPrefix):
				sub.Fatalf("handler saw %q, want generated id with prefix %q", seen, tc.wantPrefix)
			}
		})
	}
}
