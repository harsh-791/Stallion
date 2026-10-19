package audit

import (
	"strings"
	"testing"
	"time"
)

func TestValidateAcceptsExampleShape(t *testing.T) {
	email := "priya.n@northwind.example"
	ev := Event{
		EventID:      "0B6C1F0E-6D2A-4C3E-9A51-2F7D8E4B1C90",
		TenantID:     "t_0003",
		ActorID:      "usr_7f2k9q",
		ActorType:    "user",
		ActorEmail:   &email,
		Action:       "invoice.delete",
		ResourceType: "invoice",
		ResourceID:   "inv_55120",
		Result:       "success",
		IP:           "203.0.113.24",
		Metadata:     []byte(`{"invoice_number":"INV-2026-0912","amount":1840.5}`),
		OccurredAt:   time.Date(2026, 9, 14, 8, 31, 7, 412000000, time.UTC),
	}
	if err := ev.Validate(); err != nil {
		t.Fatal(err)
	}
	if ev.EventID != strings.ToLower("0B6C1F0E-6D2A-4C3E-9A51-2F7D8E4B1C90") {
		t.Fatalf("event_id was not canonicalized: %s", ev.EventID)
	}
}

func TestValidateRejects(t *testing.T) {
	base := validEvent()
	email := "svc@example.com"
	cases := []struct {
		name   string
		mutate func(*Event)
	}{
		{"bad uuid", func(e *Event) { e.EventID = "not-a-uuid" }},
		{"bad action", func(e *Event) { e.Action = "invoice.archive" }},
		{"email on service", func(e *Event) {
			e.ActorType = "service"
			e.ActorID = "svc_billing"
			e.ActorEmail = &email
		}},
		{"email without at", func(e *Event) {
			bad := "not-an-email"
			e.ActorEmail = &bad
		}},
		{"email with spaces", func(e *Event) {
			bad := "ada @example.com"
			e.ActorEmail = &bad
		}},
		{"ipv6", func(e *Event) { e.IP = "2001:db8::1" }},
		{"metadata array", func(e *Event) { e.Metadata = []byte(`[]`) }},
		{"missing time", func(e *Event) { e.OccurredAt = time.Time{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := base
			tc.mutate(&ev)
			if err := ev.Validate(); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestDedupeKeepsFirst(t *testing.T) {
	first := validEvent()
	second := validEvent()
	second.Action = "invoice.update"
	out := Dedupe([]Event{first, second, first})
	if len(out) != 1 || out[0].Action != "invoice.delete" {
		t.Fatalf("got %+v", out)
	}
}

func TestAllListedActions(t *testing.T) {
	if len(actions) != 22 {
		t.Fatalf("action count = %d", len(actions))
	}
	for action := range actions {
		if !KnownAction(action) {
			t.Fatalf("missing %s", action)
		}
	}
}

func validEvent() Event {
	email := "ada@example.com"
	return Event{
		EventID:      "00000000-0000-4000-8000-000000000001",
		TenantID:     "t_0001",
		ActorID:      "usr_ada",
		ActorType:    "user",
		ActorEmail:   &email,
		Action:       "invoice.delete",
		ResourceType: "invoice",
		ResourceID:   "inv_1",
		Result:       "success",
		IP:           "203.0.113.10",
		Metadata:     []byte(`{"reason":"duplicate"}`),
		OccurredAt:   time.Date(2026, 9, 14, 8, 0, 0, 0, time.UTC),
	}
}
