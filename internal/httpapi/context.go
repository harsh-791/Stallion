package httpapi

import (
	"context"
	"net/http"
	"time"
)

func contextWithDeadline(r *http.Request, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), d)
}
