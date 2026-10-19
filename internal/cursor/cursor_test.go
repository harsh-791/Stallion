package cursor

import (
	"testing"
	"time"
)

func TestRoundTrip(t *testing.T) {
	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	in := Cursor{
		OccurredAt: time.Date(2026, 9, 14, 8, 31, 7, 412000000, time.UTC),
		EventID:    "0b6c1f0e-6d2a-4c3e-9a51-2f7d8e4b1c90",
		AsOf:       42,
		TenantID:   "t_0001",
		Action:     "invoice.delete",
		From:       &from,
		Limit:      50,
	}
	token, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decode(token)
	if err != nil {
		t.Fatal(err)
	}
	if !out.OccurredAt.Equal(in.OccurredAt) || out.EventID != in.EventID || out.AsOf != in.AsOf || out.TenantID != in.TenantID || out.Limit != 50 {
		t.Fatalf("got %+v", out)
	}
	if !out.Matches(in.TenantID, in.Action, in.From, nil, in.Limit) {
		t.Fatal("cursor did not match the request that created it")
	}
	if out.Matches("t_0002", in.Action, in.From, nil, in.Limit) {
		t.Fatal("cursor matched a different tenant")
	}
}

func TestRejectsTamperedToken(t *testing.T) {
	token, err := Encode(Cursor{
		OccurredAt: time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC),
		EventID:    "00000000-0000-4000-8000-000000000001",
		AsOf:       1,
		TenantID:   "t_0001",
		Limit:      50,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(token)
	raw[len(raw)-1] ^= 1
	if _, err := Decode(string(raw)); err == nil {
		t.Fatal("expected a tampered cursor to fail")
	}
}

func TestRejectsGarbage(t *testing.T) {
	if _, err := Decode("not-a-cursor"); err == nil {
		t.Fatal("expected an error")
	}
}
