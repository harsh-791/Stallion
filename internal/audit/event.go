package audit

import (
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"
)

const (
	MaxBatch     = 1000
	MaxFieldLen  = 512
	MaxMetaBytes = 16 * 1024
)

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var idPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

var actions = map[string]struct{}{
	"user.login":          {},
	"user.logout":         {},
	"user.password_reset": {},
	"user.mfa_enabled":    {},
	"user.invite":         {},
	"user.role_change":    {},
	"user.delete":         {},
	"api_key.create":      {},
	"api_key.revoke":      {},
	"invoice.create":      {},
	"invoice.update":      {},
	"invoice.delete":      {},
	"invoice.export":      {},
	"payment.refund":      {},
	"project.create":      {},
	"project.update":      {},
	"project.delete":      {},
	"document.view":       {},
	"document.download":   {},
	"document.share":      {},
	"settings.update":     {},
	"webhook.create":      {},
}

var actorTypes = map[string]struct{}{
	"user":    {},
	"service": {},
	"api_key": {},
}

var resourceTypes = map[string]struct{}{
	"user":     {},
	"api_key":  {},
	"invoice":  {},
	"payment":  {},
	"project":  {},
	"document": {},
	"settings": {},
	"webhook":  {},
}

var results = map[string]struct{}{
	"success": {},
	"failure": {},
}

// Event is one audit record. JSON field names match the brief.
type Event struct {
	EventID      string          `json:"event_id"`
	TenantID     string          `json:"tenant_id"`
	ActorID      string          `json:"actor_id"`
	ActorType    string          `json:"actor_type"`
	ActorEmail   *string         `json:"actor_email"`
	Action       string          `json:"action"`
	ResourceType string          `json:"resource_type"`
	ResourceID   string          `json:"resource_id"`
	Result       string          `json:"result"`
	IP           string          `json:"ip"`
	Metadata     json.RawMessage `json:"metadata"`
	OccurredAt   time.Time       `json:"occurred_at"`
}

func (e Event) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		EventID      string          `json:"event_id"`
		TenantID     string          `json:"tenant_id"`
		ActorID      string          `json:"actor_id"`
		ActorType    string          `json:"actor_type"`
		ActorEmail   *string         `json:"actor_email"`
		Action       string          `json:"action"`
		ResourceType string          `json:"resource_type"`
		ResourceID   string          `json:"resource_id"`
		Result       string          `json:"result"`
		IP           string          `json:"ip"`
		Metadata     json.RawMessage `json:"metadata"`
		OccurredAt   string          `json:"occurred_at"`
	}{
		EventID:      e.EventID,
		TenantID:     e.TenantID,
		ActorID:      e.ActorID,
		ActorType:    e.ActorType,
		ActorEmail:   e.ActorEmail,
		Action:       e.Action,
		ResourceType: e.ResourceType,
		ResourceID:   e.ResourceID,
		Result:       e.Result,
		IP:           e.IP,
		Metadata:     e.Metadata,
		OccurredAt:   e.OccurredAt.UTC().Format(time.RFC3339Nano),
	})
}

type FieldError struct {
	Field string
	Msg   string
}

func (e *FieldError) Error() string {
	return e.Field + ": " + e.Msg
}

func fieldErr(field, msg string) error {
	return &FieldError{Field: field, Msg: msg}
}

// Validate checks the brief's schema and canonicalizes event_id to lowercase.
func (e *Event) Validate() error {
	if e == nil {
		return fieldErr("event", "is required")
	}
	e.EventID = strings.ToLower(strings.TrimSpace(e.EventID))
	if !uuidPattern.MatchString(e.EventID) {
		return fieldErr("event_id", "must be a UUID")
	}
	if err := requireID("tenant_id", e.TenantID); err != nil {
		return err
	}
	if err := requireID("actor_id", e.ActorID); err != nil {
		return err
	}
	if _, ok := actorTypes[e.ActorType]; !ok {
		return fieldErr("actor_type", "must be user, service, or api_key")
	}
	if e.ActorEmail != nil {
		if e.ActorType != "user" {
			return fieldErr("actor_email", "is set only when actor_type is user")
		}
		email := strings.TrimSpace(*e.ActorEmail)
		at := strings.IndexByte(email, '@')
		if email == "" || len(email) > MaxFieldLen || at < 1 || at == len(email)-1 || strings.ContainsAny(email, " \t\r\n") {
			return fieldErr("actor_email", "must be a non-empty email string")
		}
		*e.ActorEmail = email
	}
	if _, ok := actions[e.Action]; !ok {
		return fieldErr("action", "is not a known action")
	}
	if _, ok := resourceTypes[e.ResourceType]; !ok {
		return fieldErr("resource_type", "is not a known resource type")
	}
	if err := requireID("resource_id", e.ResourceID); err != nil {
		return err
	}
	if _, ok := results[e.Result]; !ok {
		return fieldErr("result", "must be success or failure")
	}
	if ip := net.ParseIP(e.IP); ip == nil || ip.To4() == nil {
		return fieldErr("ip", "must be an IPv4 address")
	}
	if len(e.Metadata) == 0 || len(e.Metadata) > MaxMetaBytes || !json.Valid(e.Metadata) {
		return fieldErr("metadata", "must be a JSON object")
	}
	var probe any
	if err := json.Unmarshal(e.Metadata, &probe); err != nil {
		return fieldErr("metadata", "must be a JSON object")
	}
	if _, ok := probe.(map[string]any); !ok {
		return fieldErr("metadata", "must be a JSON object")
	}
	if e.OccurredAt.IsZero() {
		return fieldErr("occurred_at", "must be an RFC3339 timestamp")
	}
	e.OccurredAt = e.OccurredAt.UTC()
	return nil
}

func KnownAction(action string) bool {
	_, ok := actions[action]
	return ok
}

func requireID(field, value string) error {
	if value == "" || len(value) > MaxFieldLen || !idPattern.MatchString(value) {
		return fieldErr(field, "must be 1-512 characters of letters, digits, '_', '.', or '-'")
	}
	return nil
}

// Dedupe keeps the first copy of each event_id. Postgres rejects an INSERT
// that mentions the same conflict key twice.
func Dedupe(events []Event) []Event {
	if len(events) < 2 {
		return events
	}
	seen := make(map[string]struct{}, len(events))
	out := make([]Event, 0, len(events))
	for _, e := range events {
		if _, ok := seen[e.EventID]; ok {
			continue
		}
		seen[e.EventID] = struct{}{}
		out = append(out, e)
	}
	return out
}

// ValidateTenantID checks a tenant id taken from a URL path.
func ValidateTenantID(id string) error {
	if err := requireID("tenant_id", id); err != nil {
		return fmt.Errorf("tenant_id: must be 1-512 characters of letters, digits, '_', '.', or '-'")
	}
	return nil
}
