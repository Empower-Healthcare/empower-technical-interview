package main

import (
	"errors"
	"strings"
	"testing"
)

const template = `# comment
#POSTGRES_PORT=5432
#SHIPMENT_MGMT_PORT=8080
#RELAY_MGMT_PORT=8081
#CONSUMER_MGMT_PORT=8082

# not a port
#LOG_LEVEL=debug
`

func TestParseSpecs(t *testing.T) {
	specs, err := ParseSpecs(strings.NewReader(template + "RELAY_MGMT_PORT=8091\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := []PortSpec{
		{Name: "POSTGRES_PORT", Default: 5432},
		{Name: "SHIPMENT_MGMT_PORT", Default: 8080},
		{Name: "RELAY_MGMT_PORT", Default: 8081},
		{Name: "CONSUMER_MGMT_PORT", Default: 8082},
	}
	if len(specs) != len(want) {
		t.Fatalf("specs = %v, want %v", specs, want)
	}
	for i := range want {
		if specs[i] != want[i] {
			t.Errorf("specs[%d] = %v, want %v (first occurrence wins, file order kept)", i, specs[i], want[i])
		}
	}
}

func TestParseOverridesIgnoresCommentedDefaults(t *testing.T) {
	overrides, err := ParseOverrides(strings.NewReader(template + "RELAY_MGMT_PORT=8091\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(overrides) != 1 || overrides["RELAY_MGMT_PORT"] != 8091 {
		t.Fatalf("overrides = %v, want only RELAY_MGMT_PORT=8091", overrides)
	}
}

func TestChoose(t *testing.T) {
	specs := []PortSpec{
		{Name: "SHIPMENT_MGMT_PORT", Default: 8080},
		{Name: "RELAY_MGMT_PORT", Default: 8081},
		{Name: "CONSUMER_MGMT_PORT", Default: 8082},
	}
	tests := []struct {
		name      string
		overrides map[string]int
		ours      map[int]bool
		taken     map[int]bool
		want      []Choice
	}{
		{
			name:  "all free",
			taken: map[int]bool{},
			want: []Choice{
				{Name: "SHIPMENT_MGMT_PORT", Desired: 8080, Chosen: 8080, Reason: ReasonFree},
				{Name: "RELAY_MGMT_PORT", Desired: 8081, Chosen: 8081, Reason: ReasonFree},
				{Name: "CONSUMER_MGMT_PORT", Desired: 8082, Chosen: 8082, Reason: ReasonFree},
			},
		},
		{
			name:  "swap skips ports other services want",
			taken: map[int]bool{8080: true},
			want: []Choice{
				{Name: "SHIPMENT_MGMT_PORT", Desired: 8080, Chosen: 8083, Reason: ReasonTaken},
				{Name: "RELAY_MGMT_PORT", Desired: 8081, Chosen: 8081, Reason: ReasonFree},
				{Name: "CONSUMER_MGMT_PORT", Desired: 8082, Chosen: 8082, Reason: ReasonFree},
			},
		},
		{
			name:  "two swaps do not collide",
			taken: map[int]bool{8080: true, 8081: true, 8083: true},
			want: []Choice{
				{Name: "SHIPMENT_MGMT_PORT", Desired: 8080, Chosen: 8084, Reason: ReasonTaken},
				{Name: "RELAY_MGMT_PORT", Desired: 8081, Chosen: 8085, Reason: ReasonTaken},
				{Name: "CONSUMER_MGMT_PORT", Desired: 8082, Chosen: 8082, Reason: ReasonFree},
			},
		},
		{
			name:  "a port published by this stack is kept even though it is in use",
			ours:  map[int]bool{8080: true, 8081: true, 8082: true},
			taken: map[int]bool{8080: true, 8081: true, 8082: true},
			want: []Choice{
				{Name: "SHIPMENT_MGMT_PORT", Desired: 8080, Chosen: 8080, Reason: ReasonOurs},
				{Name: "RELAY_MGMT_PORT", Desired: 8081, Chosen: 8081, Reason: ReasonOurs},
				{Name: "CONSUMER_MGMT_PORT", Desired: 8082, Chosen: 8082, Reason: ReasonOurs},
			},
		},
		{
			name:      "an override replaces the default and is itself checked",
			overrides: map[string]int{"RELAY_MGMT_PORT": 8091},
			taken:     map[int]bool{8081: true, 8091: true},
			want: []Choice{
				{Name: "SHIPMENT_MGMT_PORT", Desired: 8080, Chosen: 8080, Reason: ReasonFree},
				{Name: "RELAY_MGMT_PORT", Desired: 8091, Chosen: 8092, Reason: ReasonTaken},
				{Name: "CONSUMER_MGMT_PORT", Desired: 8082, Chosen: 8082, Reason: ReasonFree},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(sub *testing.T) {
			sel := Selection{
				Overrides: tc.overrides,
				Ours:      tc.ours,
				Free:      func(p int) (bool, error) { return !tc.taken[p], nil },
				Limit:     10,
			}
			got, err := Choose(specs, sel)
			if err != nil {
				sub.Fatal(err)
			}
			if len(got) != len(tc.want) {
				sub.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					sub.Errorf("choice[%d] = %+v, want %+v", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestChooseGivesUpWithinLimit(t *testing.T) {
	specs := []PortSpec{{Name: "POSTGRES_PORT", Default: 5432}}
	sel := Selection{Free: func(int) (bool, error) { return false, nil }, Limit: 3}
	if _, err := Choose(specs, sel); err == nil {
		t.Fatal("expected an error when no port within the limit is free")
	}
}

func TestChooseRejectsInvalidDesiredPorts(t *testing.T) {
	specs := []PortSpec{
		{Name: "SHIPMENT_MGMT_PORT", Default: 8080},
		{Name: "RELAY_MGMT_PORT", Default: 8081},
	}
	tests := []struct {
		name      string
		overrides map[string]int
		ours      map[int]bool
		wantInErr string
	}{
		{name: "duplicate when free", overrides: map[string]int{"SHIPMENT_MGMT_PORT": 8081}, wantInErr: "SHIPMENT_MGMT_PORT and RELAY_MGMT_PORT both want port 8081"},
		{name: "duplicate when published by this stack", overrides: map[string]int{"RELAY_MGMT_PORT": 8080}, ours: map[int]bool{8080: true}, wantInErr: "both want port 8080"},
		{name: "zero", overrides: map[string]int{"RELAY_MGMT_PORT": 0}, wantInErr: "RELAY_MGMT_PORT=0"},
		{name: "above 65535", overrides: map[string]int{"RELAY_MGMT_PORT": 65536}, wantInErr: "RELAY_MGMT_PORT=65536"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(sub *testing.T) {
			probed := false
			sel := Selection{
				Overrides: tc.overrides,
				Ours:      tc.ours,
				Free:      func(int) (bool, error) { probed = true; return true, nil },
				Limit:     10,
			}
			_, err := Choose(specs, sel)
			if err == nil || !strings.Contains(err.Error(), tc.wantInErr) {
				sub.Fatalf("err = %v, want it to contain %q", err, tc.wantInErr)
			}
			if probed {
				sub.Error("invalid configuration must be rejected before any port is probed")
			}
		})
	}
}

func TestChooseStopsOnProbeError(t *testing.T) {
	specs := []PortSpec{{Name: "POSTGRES_PORT", Default: 5432}}
	probeErr := errors.New("probe timed out")
	sel := Selection{Free: func(int) (bool, error) { return false, probeErr }, Limit: 3}
	choices, err := Choose(specs, sel)
	if !errors.Is(err, probeErr) {
		t.Fatalf("err = %v, want it to wrap the probe error", err)
	}
	if choices != nil {
		t.Fatalf("choices = %v, want none when a probe fails", choices)
	}
}

func TestRejectEnvironmentPorts(t *testing.T) {
	specs := []PortSpec{{Name: "POSTGRES_PORT", Default: 5432}, {Name: "GRPC_PORT", Default: 50051}}
	env := map[string]string{"GRPC_PORT": "50052", "UNRELATED": "x"}
	lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }
	err := rejectEnvironmentPorts(specs, lookup)
	if err == nil || !strings.Contains(err.Error(), `GRPC_PORT="50052"`) {
		t.Fatalf("err = %v, want a rejection naming GRPC_PORT", err)
	}
	delete(env, "GRPC_PORT")
	if err := rejectEnvironmentPorts(specs, lookup); err != nil {
		t.Fatalf("unrelated variables must not be rejected: %v", err)
	}
}

func TestRenderEnv(t *testing.T) {
	tests := []struct {
		name     string
		existing string
		choices  []Choice
		want     string
	}{
		{
			name:     "no change leaves the file untouched",
			existing: template,
			choices:  []Choice{{Name: "POSTGRES_PORT", Desired: 5432, Chosen: 5432}},
			want:     template,
		},
		{
			name:     "a commented default is replaced in place, other lines kept",
			existing: template,
			choices:  []Choice{{Name: "POSTGRES_PORT", Desired: 5432, Chosen: 5433}},
			want: strings.Replace(template,
				"#POSTGRES_PORT=5432", "POSTGRES_PORT=5433", 1),
		},
		{
			name:     "an active override is rewritten",
			existing: "RELAY_MGMT_PORT=8091\n",
			choices:  []Choice{{Name: "RELAY_MGMT_PORT", Desired: 8091, Chosen: 8092}},
			want:     "RELAY_MGMT_PORT=8092\n",
		},
		{
			name:     "keys without a line are appended in name order",
			existing: "# ports\n",
			choices: []Choice{
				{Name: "PROMETHEUS_PORT", Desired: 9090, Chosen: 9091},
				{Name: "KAFKA_UI_PORT", Desired: 8085, Chosen: 8086},
			},
			want: "# ports\nKAFKA_UI_PORT=8086\nPROMETHEUS_PORT=9091\n",
		},
		{
			name:     "empty file",
			existing: "",
			choices:  []Choice{{Name: "GRPC_PORT", Desired: 50051, Chosen: 50052}},
			want:     "GRPC_PORT=50052\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(sub *testing.T) {
			if got := RenderEnv(tc.existing, tc.choices); got != tc.want {
				sub.Errorf("RenderEnv =\n%s\nwant\n%s", got, tc.want)
			}
		})
	}
}

func TestParsePublished(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want map[int]bool
	}{
		{
			name: "one object per line, loopback only",
			raw: `{"Service":"postgres","Publishers":[{"URL":"127.0.0.1","TargetPort":5432,"PublishedPort":5432,"Protocol":"tcp"}]}
{"Service":"kafka","Publishers":[{"URL":"","TargetPort":9092,"PublishedPort":0,"Protocol":"tcp"},{"URL":"127.0.0.1","TargetPort":19092,"PublishedPort":19092,"Protocol":"tcp"}]}
`,
			want: map[int]bool{5432: true, 19092: true},
		},
		{
			name: "json array",
			raw:  `[{"Publishers":[{"URL":"127.0.0.1","PublishedPort":8080}]}]`,
			want: map[int]bool{8080: true},
		},
		{
			name: "no containers",
			raw:  "",
			want: map[int]bool{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(sub *testing.T) {
			got, err := parsePublished([]byte(tc.raw))
			if err != nil {
				sub.Fatal(err)
			}
			if len(got) != len(tc.want) {
				sub.Fatalf("got %v, want %v", got, tc.want)
			}
			for p := range tc.want {
				if !got[p] {
					sub.Errorf("missing port %d in %v", p, got)
				}
			}
		})
	}
}
