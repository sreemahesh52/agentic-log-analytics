package middleware

import (
	"log/slog"
	"net/http"
	"time"

	"golang.org/x/time/rate"
)

const (
	statusRejectedRateLimit = "rejected_ratelimit"
)

// statusRecorder wraps http.ResponseWriter so we can capture the status code
// after the handler writes it. The default ResponseWriter does not expose
// the status code after it has been sent — we need it for logging.
type statusRecorder struct {
	http.ResponseWriter        // embed the real writer — all its methods are promoted
	status             int     // we store the code here when WriteHeader is called
}

// WriteHeader intercepts the status code before forwarding it to the real writer.
// "Intercept and delegate" is the standard Go pattern for wrapping interfaces.
func (r *statusRecorder) WriteHeader(status int) {
	r.status = status                    // save it so RequestLoggingMiddleware can read it
	r.ResponseWriter.WriteHeader(status) // forward to the real ResponseWriter
}

// RateLimiterMiddleware returns an http.Handler wrapper that enforces a token-bucket limit.
// Token bucket explained:
//   - A bucket holds up to `rps` tokens.
//   - The bucket refills at `rps` tokens per second.
//   - Each request consumes 1 token. If the bucket is empty, the request is rejected (429).
//   - Short bursts are allowed (up to `rps` tokens consumed instantly).
// We chose token bucket over leaky bucket because log ingest is naturally bursty
// (e.g., a deployment floods hundreds of logs in <1s). Token bucket absorbs the burst;
// leaky bucket would drop the same valid traffic.
// The function signature "func(http.Handler) http.Handler" is the standard Go
// middleware pattern: it takes the next handler and returns a new handler wrapping it.
func RateLimiterMiddleware(rps int, m rateLimitMetrics) func(http.Handler) http.Handler {
	// rate.NewLimiter(limit, burst): limit = tokens/sec refill rate, burst = bucket size.
	// Setting burst == rps means the bucket holds at most 1 second's worth of tokens.
	limiter := rate.NewLimiter(rate.Limit(rps), rps)

	// Return a function that wraps any http.Handler.
	return func(next http.Handler) http.Handler {
		// http.HandlerFunc adapts a plain function to the http.Handler interface.
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// limiter.Allow removes one token. Returns false if bucket is empty.
			if !limiter.Allow() {
				if m != nil {
					m.IncRateLimitRejection("unknown")
				}
				w.Header().Set("Content-Type", "application/json")
				// 429 Too Many Requests — standard HTTP code for rate limiting.
				w.WriteHeader(http.StatusTooManyRequests)
				w.Write([]byte(`{"error":{"code":"RATE_LIMIT_EXCEEDED","message":"too many requests, slow down","request_id":""}}`))
				return // do not call next — request is rejected here
			}
			// Token consumed successfully — pass the request to the next handler in the chain.
			next.ServeHTTP(w, r)
		})
	}
}

// rateLimitMetrics is a narrow interface (Interface Segregation principle).
// Middleware only needs to increment one counter — it should not import the full
// metrics package. This keeps middleware decoupled from the metrics implementation.
type rateLimitMetrics interface {
	IncRateLimitRejection(tenant string)
}

// RequestLoggingMiddleware logs one structured line per HTTP request after it completes.
// It wraps the ResponseWriter with statusRecorder to capture the status code,
// then logs method, path, status, and duration after the inner handler returns.
func RequestLoggingMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// Wrap w so we can read back the status code the handler wrote.
			// Initial status is 200 — if the handler never calls WriteHeader, that's the default.
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

			// Call the next handler (rate limiter → mux → route handler).
			// This blocks until the handler has written its full response.
			next.ServeHTTP(rec, r)

			// Log after the handler completes — we now have the status and duration.
			logger.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"duration_ms", time.Since(start).Milliseconds(),
				// X-Trace-ID is set by the handler before writing the response body.
				"trace_id", w.Header().Get("X-Trace-ID"),
			)
		})
	}
}
