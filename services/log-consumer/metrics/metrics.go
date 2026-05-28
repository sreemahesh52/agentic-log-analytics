package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus counters for the log-consumer service.
// promauto registers them with the default registry at construction time.
// Injected into BatchWriter rather than accessed as globals — Dependency Inversion.
type Metrics struct {
	// BatchesTotal counts completed flush operations (successful or DLQ-routed).
	// A batch is one call to flush that had at least one record.
	BatchesTotal prometheus.Counter

	// LogsInsertedTotal counts individual log records successfully written to PostgreSQL.
	// Divided by BatchesTotal gives average batch size.
	LogsInsertedTotal prometheus.Counter

	// DLQTotal counts batches routed to logs.dlq after retry exhaustion.
	// A non-zero value means PostgreSQL was unavailable long enough to exhaust retries.
	DLQTotal prometheus.Counter
}

// New registers and returns all Prometheus metrics for this service.
// promauto.NewCounter panics on duplicate registration — safe here since
// New is called exactly once at startup.
func New() *Metrics {
	return &Metrics{
		BatchesTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "log_consumer_batches_total",
			Help: "Total number of log batches flushed to PostgreSQL (successful or DLQ).",
		}),
		LogsInsertedTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "log_consumer_logs_inserted_total",
			Help: "Total number of log records successfully inserted into PostgreSQL.",
		}),
		DLQTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "log_consumer_dlq_total",
			Help: "Total number of batches sent to logs.dlq after all retries exhausted.",
		}),
	}
}
