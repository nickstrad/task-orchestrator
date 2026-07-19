// Package httpapi holds the wire formats shared by the worker's HTTP server
// and the manager's HTTP client. It belongs to neither package, so it lives
// on its own and both import it.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// ErrorResponse is the JSON body every failing endpoint returns.
type ErrorResponse struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
}

// Defaults for HTTPWithRetry, overridable per call with WithRetryLimit and
// WithRetryDelay.
const (
	RetryLimit = 3
	RetryDelay = 5 * time.Second
)

// WriteJSON sets the content type, writes the status, and encodes body.
func WriteJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

// WriteError sets the HTTP status and writes an ErrorResponse whose Code is
// that same status, so the two can never disagree.
func WriteError(w http.ResponseWriter, status int, msg string) {
	WriteJSON(w, status, ErrorResponse{Message: msg, Code: status})
}

// retryConfig is the resolved settings for a single HTTPWithRetry call.
type retryConfig struct {
	limit  int
	delay  time.Duration
	logger *slog.Logger
}

// RetryOption overrides one of HTTPWithRetry's defaults.
type RetryOption func(*retryConfig)

// WithRetryLimit caps how many times fn is called.
func WithRetryLimit(n int) RetryOption {
	return func(c *retryConfig) { c.limit = n }
}

// WithRetryDelay sets how long to wait between attempts.
func WithRetryDelay(d time.Duration) RetryOption {
	return func(c *retryConfig) { c.delay = d }
}

// WithLogger opts into per-attempt logging under the caller's identity.
// Without it, HTTPWithRetry is silent.
func WithLogger(l *slog.Logger) RetryOption {
	return func(c *retryConfig) { c.logger = l }
}

// Get issues a GET bound to ctx. It is the usual fn to hand HTTPWithRetry.
func Get(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

// HTTPWithRetry calls fn until it succeeds or the retry limit is reached,
// logging each failed attempt at Warn. It does not stop propagating the error —
// the caller decides whether to give up and logs that at Error.
//
// Cancelling ctx aborts both the in-flight request and the wait between
// attempts; the returned error then satisfies errors.Is(err, context.Canceled).
func HTTPWithRetry(ctx context.Context, fn func(context.Context, string) (*http.Response, error), url string, opts ...RetryOption) (*http.Response, error) {
	cfg := retryConfig{
		limit:  RetryLimit,
		delay:  RetryDelay,
		logger: slog.New(slog.DiscardHandler),
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	var res *http.Response
	var err error
	for i := range cfg.limit {
		res, err = fn(ctx, url)

		if err == nil {
			break
		}

		// A cancelled ctx is not a retryable failure — give up without
		// logging it as one.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, errors.Join(err, ctxErr)
		}

		cfg.logger.Warn("http request failed", "url", url, "attempt", i+1, "err", err)

		if i < cfg.limit-1 {
			select {
			case <-ctx.Done():
				return nil, errors.Join(err, ctx.Err())
			case <-time.After(cfg.delay):
			}
		}
	}

	return res, err
}
