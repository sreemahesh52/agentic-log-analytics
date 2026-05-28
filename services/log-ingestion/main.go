package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/agentic-log-analytics/log-ingestion/config"
	"github.com/agentic-log-analytics/log-ingestion/handler"
	"github.com/agentic-log-analytics/log-ingestion/kafka"
	"github.com/agentic-log-analytics/log-ingestion/metrics"
	"github.com/agentic-log-analytics/log-ingestion/middleware"
)

const (
	// gracefulShutdownTimeout is how long we wait for in-flight HTTP requests
	// to finish after receiving SIGTERM before force-closing. Kubernetes default
	// grace period is 30s, so 10s gives us room to drain without hitting the kill.
	gracefulShutdownTimeout = 10 * time.Second

	serviceName = "log-ingestion"
)

func main() {
	// flag.Bool registers a CLI flag "-health-check". When the Docker HEALTHCHECK
	// runs "/app/server -health-check", we skip normal startup and just ping /health.
	healthCheck := flag.Bool("health-check", false, "perform health check and exit")
	// flag.Parse actually reads os.Args and populates the registered flags above.
	flag.Parse()

	// Dereference the pointer (*healthCheck) to read the bool value.
	if *healthCheck {
		// runHealthCheck returns 0 (success) or 1 (failure).
		// os.Exit bypasses deferred functions — fine here since we're just probing.
		os.Exit(runHealthCheck())
	}

	// Build a temporary INFO-level logger before config is loaded.
	// structured logging must be the very first action.
	logger := buildLogger("INFO")

	// LoadFromEnv reads all env vars and applies defaults for missing ones.
	cfg := config.LoadFromEnv()

	// Rebuild the logger now that we know the configured log level (DEBUG/INFO/WARN/ERROR).
	logger = buildLogger(cfg.LogLevel)

	// Validate checks required fields. If anything is missing or wrong,
	// we log the error and exit immediately — "fail fast" at startup.
	if err := cfg.Validate(); err != nil {
		logger.Error("invalid configuration", "error", err, "service", serviceName)
		os.Exit(1)
	}

	// NewKafkaProducer dials the broker to verify it's reachable before returning.
	// If Kafka is down at startup, we exit rather than silently failing later.
	producer, err := kafka.NewKafkaProducer(cfg.KafkaBrokers)
	if err != nil {
		logger.Error("failed to connect to kafka", "error", err, "service", serviceName)
		os.Exit(1)
	}

	// metrics.New registers all Prometheus counters/histograms with the default registry.
	m := metrics.New()

	// handler.New injects the producer, topic, metrics, and logger into the handler.
	// The handler never creates these itself — Dependency Inversion principle.
	h := handler.New(producer, cfg.KafkaTopic, m, logger)

	// --- Route registration ---
	// http.NewServeMux creates a fresh request router (multiplexer).
	// Go 1.22+ supports "METHOD /path" patterns directly in HandleFunc.
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/logs", h.IngestLog)  // main ingest endpoint
	mux.HandleFunc("GET /health", h.Health)            // liveness/readiness probe
	mux.Handle("GET /metrics", promhttp.Handler())     // Prometheus scrape endpoint

	// --- Middleware chain ---
	// metricsAdapter wraps *metrics.Metrics to satisfy the narrow rateLimitMetrics
	// interface that middleware expects (Interface Segregation — middleware should
	// not depend on the full Metrics struct).
	metricsAdapter := &metricsRateLimitAdapter{m: m}

	// Middleware is applied inside-out: RateLimiter runs first (innermost),
	// then RequestLogging wraps around it (outermost).
	// A request flows: RequestLogging → RateLimiter → mux → handler
	chain := middleware.RequestLoggingMiddleware(logger)(
		middleware.RateLimiterMiddleware(cfg.RateLimitPerSecond, metricsAdapter)(mux),
	)

	// --- HTTP server config ---
	// ReadTimeout: max time to read the full request (headers + body).
	// WriteTimeout: max time to write the full response.
	// Without these, a slow client can hold a connection open indefinitely.
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      chain,
		ReadTimeout:  time.Duration(cfg.ReadTimeoutSeconds) * time.Second,
		WriteTimeout: time.Duration(cfg.WriteTimeoutSeconds) * time.Second,
	}

	// --- Start server in a goroutine ---
	// "go func { ... }" launches a goroutine — a lightweight thread managed by
	// the Go runtime. We do this because srv.ListenAndServe blocks forever, and
	// we need main to continue to the signal-listening code below.
	// Without the goroutine, the program would hang here and never reach <-quit.
	go func() {
		logger.Info("server starting", "port", cfg.Port, "service", serviceName)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// http.ErrServerClosed is the normal error when Shutdown is called.
			// Any other error (e.g., port already in use) is a real failure.
			logger.Error("server failed", "error", err, "service", serviceName)
			os.Exit(1)
		}
	}()

	// --- Block until OS shutdown signal ---
	// make(chan os.Signal, 1) creates a buffered channel that holds 1 signal.
	// Buffered prevents the signal being dropped if we're not ready to receive yet.
	quit := make(chan os.Signal, 1)

	// signal.Notify tells the OS to send SIGTERM (Kubernetes stop) and
	// SIGINT (Ctrl+C) into our quit channel instead of the default handler.
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	// <-quit blocks here — main pauses until a signal arrives in the channel.
	// This is the idiomatic Go pattern for "wait until told to stop".
	<-quit

	logger.Info("shutdown signal received, draining connections", "service", serviceName)

	// --- Graceful shutdown ---
	// context.WithTimeout creates a context that automatically cancels after
	// gracefulShutdownTimeout (10s). We pass this to srv.Shutdown so it
	// stops waiting for in-flight requests if they take longer than 10s.
	ctx, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
	// defer cancel ensures the context's internal timer is freed even if
	// Shutdown returns early. Without this, Go's race detector warns about a leak.
	defer cancel()

	// srv.Shutdown(ctx) stops accepting new connections and waits for active
	// requests to complete — up to the deadline in ctx.
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("graceful shutdown failed", "error", err, "service", serviceName)
	}

	// Flush any buffered Kafka messages and release the connection.
	// This must happen after HTTP shutdown so no new Publish calls arrive.
	if err := producer.Close(); err != nil {
		logger.Error("failed to close kafka producer", "error", err, "service", serviceName)
	}

	logger.Info("server stopped cleanly", "service", serviceName)
}

// buildLogger creates a JSON-format structured logger at the given level.
// slog is Go's standard structured logging library (added in Go 1.21).
// JSONHandler writes each log entry as a single JSON line — machine-parseable.
func buildLogger(level string) *slog.Logger {
	var lvl slog.Level
	// Map string env var value to slog's typed Level constants.
	switch level {
	case "DEBUG":
		lvl = slog.LevelDebug
	case "WARN":
		lvl = slog.LevelWarn
	case "ERROR":
		lvl = slog.LevelError
	default:
		// Unknown or empty level defaults to INFO.
		lvl = slog.LevelInfo
	}
	// slog.New wraps a Handler. JSONHandler writes to os.Stdout.
	// HandlerOptions.Level filters out log entries below the configured level.
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

// runHealthCheck is called when the binary runs with -health-check flag.
// It hits the /health endpoint and exits 0 (healthy) or 1 (unhealthy).
// Docker uses the exit code to decide if the container is healthy.
func runHealthCheck() int {
	resp, err := http.Get("http://localhost:8082/health")
	if err != nil {
		fmt.Fprintf(os.Stderr, "health check failed: %v\n", err)
		return 1
	}
	// resp.Body must always be closed to release the TCP connection back to the pool.
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "health check returned status %d\n", resp.StatusCode)
		return 1
	}
	return 0
}

// metricsRateLimitAdapter makes *metrics.Metrics satisfy the narrow rateLimitMetrics
// interface that middleware expects. Middleware only needs one method — it should not
// import the entire metrics package just to call Inc on one counter.
type metricsRateLimitAdapter struct {
	m *metrics.Metrics
}

func (a *metricsRateLimitAdapter) IncRateLimitRejection(tenant string) {
	// WithLabelValues selects (or creates) the counter for this label combination,
	// then Inc adds 1 to it. Labels let Prometheus split one metric by dimensions.
	a.m.RequestsTotal.WithLabelValues("rejected_ratelimit", tenant).Inc()
}
