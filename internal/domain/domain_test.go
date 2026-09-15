package domain

import (
	"errors"
	"regexp"
	"testing"
)

func TestValidateWeight(t *testing.T) {
	cases := []struct {
		name        string
		weightGrams int32
		wantErr     error
	}{
		{name: "below minimum", weightGrams: 0, wantErr: ErrInvalidWeight},
		{name: "negative", weightGrams: -1, wantErr: ErrInvalidWeight},
		{name: "minimum", weightGrams: 1, wantErr: nil},
		{name: "typical", weightGrams: 1500, wantErr: nil},
		{name: "maximum", weightGrams: 30_000, wantErr: nil},
		{name: "above maximum", weightGrams: 30_001, wantErr: ErrInvalidWeight},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(sub *testing.T) {
			err := ValidateWeight(tc.weightGrams)
			if !errors.Is(err, tc.wantErr) {
				sub.Fatalf("ValidateWeight(%d) = %v, want %v", tc.weightGrams, err, tc.wantErr)
			}
		})
	}
}

func TestValidateMoney(t *testing.T) {
	cases := []struct {
		name    string
		money   Money
		wantErr error
	}{
		{name: "typical", money: Money{AmountCents: 700, Currency: "USD"}, wantErr: nil},
		{name: "zero amount is valid", money: Money{AmountCents: 0, Currency: "USD"}, wantErr: nil},
		{name: "negative amount", money: Money{AmountCents: -1, Currency: "USD"}, wantErr: ErrInvalidMoney},
		{name: "empty currency", money: Money{AmountCents: 700, Currency: ""}, wantErr: ErrInvalidMoney},
		{name: "lowercase currency", money: Money{AmountCents: 700, Currency: "usd"}, wantErr: ErrInvalidMoney},
		{name: "too long currency", money: Money{AmountCents: 700, Currency: "USDD"}, wantErr: ErrInvalidMoney},
		{name: "non-letter currency", money: Money{AmountCents: 700, Currency: "US1"}, wantErr: ErrInvalidMoney},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(sub *testing.T) {
			err := ValidateMoney(tc.money)
			if !errors.Is(err, tc.wantErr) {
				sub.Fatalf("ValidateMoney(%+v) = %v, want %v", tc.money, err, tc.wantErr)
			}
		})
	}
}

func TestIdentifiersArePrefixedAndUnique(t *testing.T) {
	cases := []struct {
		name   string
		gen    func() string
		prefix string
	}{
		{name: "shipment", gen: func() string { return string(NewShipmentID()) }, prefix: "shp"},
		{name: "event", gen: func() string { return string(NewEventID()) }, prefix: "evt"},
		{name: "dispatch", gen: func() string { return string(NewDispatchID()) }, prefix: "dsp"},
		{name: "correlation", gen: NewCorrelationID, prefix: "req"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(sub *testing.T) {
			format := regexp.MustCompile(`^` + tc.prefix + `_[0-9a-f]{16}$`)
			first, second := tc.gen(), tc.gen()
			if !format.MatchString(first) {
				sub.Fatalf("id %q does not match %s", first, format)
			}
			if first == second {
				sub.Fatalf("consecutive ids must differ, got %q twice", first)
			}
		})
	}
}
