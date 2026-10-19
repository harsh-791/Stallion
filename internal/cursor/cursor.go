package cursor

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"time"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// defaultSecret is only for this assignment, where the API and the bench
// tool share a process default. Set CURSOR_SECRET in any shared deployment.
var secret = []byte("audit-cursor-dev")

func SetSecret(s string) {
	if s != "" {
		secret = []byte(s)
	}
}

// Cursor is the position of one list walk. It is bound to the tenant,
// filters, and page size that produced it.
type Cursor struct {
	OccurredAt time.Time
	EventID    string
	AsOf       int64
	TenantID   string
	Action     string
	From       *time.Time
	To         *time.Time
	Limit      int
}

type payload struct {
	O int64  `json:"o"`
	E string `json:"e"`
	A int64  `json:"a"`
	T string `json:"t"`
	C string `json:"c,omitempty"`
	F *int64 `json:"f,omitempty"`
	U *int64 `json:"u,omitempty"`
	L int    `json:"l"`
	M string `json:"m"`
}

func Encode(c Cursor) (string, error) {
	if c.EventID == "" || c.AsOf < 0 || c.TenantID == "" || c.Limit < 1 {
		return "", fmt.Errorf("incomplete cursor")
	}
	p := payload{
		O: c.OccurredAt.UTC().UnixMicro(),
		E: c.EventID,
		A: c.AsOf,
		T: c.TenantID,
		C: c.Action,
		L: c.Limit,
	}
	if c.From != nil {
		v := c.From.UTC().UnixMicro()
		p.F = &v
	}
	if c.To != nil {
		v := c.To.UTC().UnixMicro()
		p.U = &v
	}
	mac, err := sign(p)
	if err != nil {
		return "", err
	}
	p.M = mac
	raw, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func Decode(s string) (Cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return Cursor{}, fmt.Errorf("cursor is not valid")
	}
	var p payload
	if err := json.Unmarshal(raw, &p); err != nil {
		return Cursor{}, fmt.Errorf("cursor is not valid")
	}
	if !uuidPattern.MatchString(p.E) || p.A < 0 || p.T == "" || p.L < 1 || p.M == "" {
		return Cursor{}, fmt.Errorf("cursor is not valid")
	}
	mac, err := sign(p)
	if err != nil {
		return Cursor{}, fmt.Errorf("cursor is not valid")
	}
	got, err := hex.DecodeString(p.M)
	if err != nil {
		return Cursor{}, fmt.Errorf("cursor is not valid")
	}
	want, err := hex.DecodeString(mac)
	if err != nil || !hmac.Equal(got, want) {
		return Cursor{}, fmt.Errorf("cursor is not valid")
	}
	c := Cursor{
		OccurredAt: time.UnixMicro(p.O).UTC(),
		EventID:    p.E,
		AsOf:       p.A,
		TenantID:   p.T,
		Action:     p.C,
		Limit:      p.L,
	}
	if p.F != nil {
		t := time.UnixMicro(*p.F).UTC()
		c.From = &t
	}
	if p.U != nil {
		t := time.UnixMicro(*p.U).UTC()
		c.To = &t
	}
	return c, nil
}

// Matches reports whether the cursor was issued for this exact list request.
func (c Cursor) Matches(tenantID, action string, from, to *time.Time, limit int) bool {
	return c.TenantID == tenantID && c.Action == action && c.Limit == limit && sameTime(c.From, from) && sameTime(c.To, to)
}

func sameTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.UTC().UnixMicro() == b.UTC().UnixMicro()
}

func sign(p payload) (string, error) {
	p.M = ""
	raw, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(raw)
	return hex.EncodeToString(mac.Sum(nil)), nil
}
