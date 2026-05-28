package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus instrumentation for this service.
// Counters only go up. Histograms record distributions (latency buckets).
// CounterVec / HistogramVec are "labelled" variants — each unique label
// combination (e.g., status="accepted", tenant="acme") is a separate time series.
type Metrics struct {
	RequestsTotal      *prometheus.CounterVec   // counts every HTTP request by outcome
	KafkaPublishErrors *prometheus.CounterVec   // counts Kafka publish failures
	RequestDuration    *prometheus.HistogramVec // measures request latency in seconds
}

// New creates Metrics registered with the default Prometheus registry.
// Use this in main — the default registry is what the /metrics endpoint exposes.
func New() *Metrics {
	return NewWithRegisterer(prometheus.DefaultRegisterer)
}

// NewWithRegisterer creates Metrics using a caller-supplied registry.
// Tests pass prometheus.NewRegistry here so each test gets an isolated registry
// and parallel tests don't panic with "duplicate metrics collector registration".
// This is the standard Go pattern for testable Prometheus instrumentation.
func NewWithRegisterer(registerer prometheus.Registerer) *Metrics {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}

	// promauto.With(registerer) returns a factory that registers metrics with
	// our chosen registry instead of the global default.
	factory := promauto.With(registerer)

	return &Metrics{
		// CounterVec with two label dimensions: "status" and "tenant".
		// Example query: log_ingestion_requests_total{status="accepted"}
		RequestsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "log_ingestion_requests_total",
				Help: "Total number of log ingestion requests by status and tenant.",
			},
			[]string{"status", "tenant"}, // label names — values supplied at Inc() time
		),

		// Tracks Kafka publish failures separately so we can alert on them independently
		// from general request errors (e.g., validation failures are not Kafka failures).
		KafkaPublishErrors: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "log_ingestion_kafka_publish_errors_total",
				Help: "Total number of Kafka publish failures by tenant.",
			},
			[]string{"tenant"},
		),

		// Histogram records how long requests take. prometheus.DefBuckets are:
		// .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10 seconds.
		// Grafana can compute p50/p95/p99 from histogram buckets.
		RequestDuration: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "log_ingestion_request_duration_seconds",
				Help:    "HTTP request latency distribution for the log ingestion service.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"method", "path"},
		),
	}
}
