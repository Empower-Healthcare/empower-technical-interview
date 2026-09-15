package config

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestEnvReadsAndDefaults(t *testing.T) {
	env := NewEnvFrom(map[string]string{
		"STR":     "value",
		"EMPTY":   "",
		"INT":     "42",
		"DUR":     "1500ms",
		"BROKERS": "",
	})

	cases := []struct {
		name string
		got  any
		want any
	}{
		{name: "string set", got: env.String("STR", "def"), want: "value"},
		{name: "string empty falls back", got: env.String("EMPTY", "def"), want: "def"},
		{name: "string unset falls back", got: env.String("MISSING", "def"), want: "def"},
		{name: "int set", got: env.Int("INT", 1), want: 42},
		{name: "int unset falls back", got: env.Int("MISSING", 7), want: 7},
		{name: "duration set", got: env.Duration("DUR", time.Second), want: 1500 * time.Millisecond},
		{name: "duration unset falls back", got: env.Duration("MISSING", time.Second), want: time.Second},
		{name: "database url default", got: env.DatabaseURL(), want: DefaultDatabaseURL},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(sub *testing.T) {
			if tc.got != tc.want {
				sub.Fatalf("got %v, want %v", tc.got, tc.want)
			}
		})
	}
	if err := env.Err(); err != nil {
		t.Fatalf("no parse errors expected, got %v", err)
	}
	if got := env.Brokers(); len(got) != 1 || got[0] != "kafka:9092" {
		t.Fatalf("Brokers() = %v, want default", got)
	}
}

func TestEnvAccumulatesParseErrors(t *testing.T) {
	env := NewEnvFrom(map[string]string{"INT": "forty-two", "DUR": "soon", "BROKERS": "a:1,b:2"})

	if got := env.Int("INT", 5); got != 5 {
		t.Fatalf("Int on bad input must return the default, got %d", got)
	}
	if got := env.Duration("DUR", time.Second); got != time.Second {
		t.Fatalf("Duration on bad input must return the default, got %s", got)
	}
	err := env.Err()
	if err == nil {
		t.Fatal("expected parse errors")
	}
	for _, want := range []string{`INT="forty-two"`, `DUR="soon"`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q should mention %s", err, want)
		}
	}

	env = NewEnvFrom(map[string]string{"KAFKA_BROKERS": "a:1,b:2"})
	if got := env.Brokers(); len(got) != 2 || got[0] != "a:1" || got[1] != "b:2" {
		t.Fatalf("Brokers() = %v, want [a:1 b:2]", got)
	}
}

func TestRetry(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)

	t.Run("succeeds after transient failures", func(sub *testing.T) {
		calls := 0
		err := Retry(sub.Context(), logger, "thing", 10*time.Second, func(context.Context) error {
			calls++
			if calls < 3 {
				return errors.New("not yet")
			}
			return nil
		})
		if err != nil || calls != 3 {
			sub.Fatalf("err=%v calls=%d, want nil / 3", err, calls)
		}
	})

	t.Run("gives up at the deadline and keeps the last error", func(sub *testing.T) {
		last := errors.New("still down")
		err := Retry(sub.Context(), logger, "thing", 10*time.Millisecond, func(context.Context) error { return last })
		if !errors.Is(err, last) || !errors.Is(err, context.DeadlineExceeded) {
			sub.Fatalf("err=%v, want to wrap both %v and context.DeadlineExceeded", err, last)
		}
	})

	t.Run("stops when the parent context is cancelled", func(sub *testing.T) {
		ctx, cancel := context.WithCancel(sub.Context())
		cancel()
		last := errors.New("down")
		err := Retry(ctx, logger, "thing", time.Minute, func(context.Context) error { return last })
		if !errors.Is(err, context.Canceled) || !errors.Is(err, last) {
			sub.Fatalf("err=%v, want to wrap both context.Canceled and %v", err, last)
		}
	})
}
