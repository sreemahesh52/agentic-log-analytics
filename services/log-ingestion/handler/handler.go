package handler

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/agentic-log-analytics/log-ingestion/kafka"
	"github.com/agentic-log-analytics/log-ingestion/metrics"
)

// Status label constants used in Prometheus metrics.
// Named constants prevent typos — "accepted" vs "aceepted" etc.
const (
	statusAccepted           = "accepted"
	statusRejectedValidation = "rejected_validation"
	statusError              = "error"
)

// validLevels is a set (map with bool values) for O(1) membership checks.
// map[string]bool{key: true} is the idiomatic Go way to represent a set.
var validLevels = map[string]bool{
	"DEBUG": true,
	"INFO":  true,
	"WARN":  true,
	"ERROR": true,
	"FATAL": true,
}

// LogEntry is the JSON body the caller sends in POST /api/v1/logs.
// Struct tags (json:"field_name") control how Go maps JSON keys to struct fields.
// omitempty means the field is omitted from JSON output if it is empty/zero.
type LogEntry struct {
	Service  string                 `json:"service"`
	Level    string                 `json:"level"`
	Message  string                 `json:"message"`
	TraceID  string                 `json:"trace_id,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"` // arbitrary key-value pairs
}

// kafkaMessage is what we actually write to the Kafka topic.
// It is richer than LogEntry: we add Timestamp here, not in the HTTP layer.
// Unexported (lowercase) — only this package uses it.
type kafkaMessage struct {
	Service   string                 `json:"service"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	TraceID   string                 `json:"trace_id"`
	Metadata  map[string]interface{} `json:"metadata"`
	Timestamp string                 `json:"timestamp"`
}

// acceptedResponse is the 202 success body returned to the HTTP caller.
type acceptedResponse struct {
	TraceID   string `json:"trace_id"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// errorDetail is the inner object inside every error response.
type errorDetail struct {
	Code      string `json:"code"`       // UPPER_SNAKE machine-readable code
	Message   string `json:"message"`    // human-readable description
	RequestID string `json:"request_id"` // UUID for correlating logs to this request
}

// errorResponse wraps errorDetail — matches the error shape exactly.
// {"error": {"code": "...", "message": "...", "request_id": "..."}}
type errorResponse struct {
	Error errorDetail `json:"error"`
}

// healthResponse is the body for GET /health.
type healthResponse struct {
	Status string `json:"status"` // "ok" or "degraded"
	Kafka  string `json:"kafka"`  // "connected" or "unreachable"
}

// Handler holds everything the HTTP handlers need to do their job.
// All dependencies are injected via New — never created inside the handler itself.
type Handler struct {
	producer kafka.KafkaProducer // interface, not concrete type → testable with mocks
	topic    string
	metrics  *metrics.Metrics
	logger   *slog.Logger
}

// New creates a Handler with all dependencies injected from outside.
// This is the Dependency Inversion principle: handler depends on abstractions,
// not on concrete kafka or metrics implementations.
func New(producer kafka.KafkaProducer, topic string, m *metrics.Metrics, logger *slog.Logger) *Handler {
	return &Handler{
		producer: producer,
		topic:    topic,
		metrics:  m,
		logger:   logger,
	}
}

// IngestLog handles POST /api/v1/logs.
// Go HTTP handlers always receive (http.ResponseWriter, *http.Request).
// ResponseWriter is used to write the response; Request contains everything about the incoming call.
func (h *Handler) IngestLog(w http.ResponseWriter, r *http.Request) {
	// Record when the request started so we can measure total latency at the end.
	start := time.Now()

	// Generate a unique ID for this request so we can correlate logs if something goes wrong.
	requestID := uuid.New().String()

	// defer runs this function AFTER IngestLog returns, no matter what path we exit on.
	// This guarantees the duration metric is always recorded — even on early returns.
	defer func() {
		h.metrics.RequestDuration.WithLabelValues("POST", "/api/v1/logs").
			Observe(time.Since(start).Seconds())
	}()

	// --- Step 1: Parse JSON body ---
	// json.NewDecoder(r.Body) reads from the request body stream.
	// Decode(&entry) fills in the LogEntry struct fields from the JSON keys.
	var entry LogEntry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		h.writeError(w, http.StatusBadRequest, "VALIDATION_ERROR",
			"request body must be valid JSON", requestID)
		h.metrics.RequestsTotal.WithLabelValues(statusRejectedValidation, extractTenantID(entry.Metadata)).Inc()
		return // stop processing — write nothing else to w after an error response
	}

	tenantID := extractTenantID(entry.Metadata)

	// --- Step 2: Validate fields ---
	if err := h.validateEntry(&entry); err != nil {
		// err.Error converts the error to a string for the human-readable message.
		h.writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error(), requestID)
		h.metrics.RequestsTotal.WithLabelValues(statusRejectedValidation, tenantID).Inc()
		return
	}

	// --- Step 3: Auto-generate TraceID if caller did not provide one ---
	if entry.TraceID == "" {
		entry.TraceID = uuid.New().String() // UUID v4: random, globally unique
	}
	// Set the trace ID as a response header so the caller can use it for correlation.
	w.Header().Set("X-Trace-ID", entry.TraceID)

	// --- Step 4: Build the Kafka message payload ---
	// json.Marshal converts the struct to a []byte of JSON.
	payload, err := h.buildKafkaPayload(&entry)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR",
			"failed to build message payload", requestID)
		h.metrics.RequestsTotal.WithLabelValues(statusError, tenantID).Inc()
		return
	}

	// --- Step 5: Publish to Kafka ---
	// r.Context carries the request's cancellation signal.
	// If the client disconnects mid-request, the context is cancelled and
	// WriteMessages will abort rather than waiting for Kafka indefinitely.
	if err := h.producer.Publish(r.Context(), h.topic, entry.TraceID, payload); err != nil {
		h.logger.Error("kafka publish failed",
			"error", err,
			"trace_id", entry.TraceID,
			"service", entry.Service,
		)
		h.metrics.KafkaPublishErrors.WithLabelValues(tenantID).Inc()
		h.metrics.RequestsTotal.WithLabelValues(statusError, tenantID).Inc()
		// 503 = downstream dependency (Kafka) unavailable. Caller should retry.
		h.writeError(w, http.StatusServiceUnavailable, "KAFKA_UNAVAILABLE",
			"message broker unavailable, please retry", requestID)
		return
	}

	// --- Step 6: Return 202 Accepted ---
	// 202 means "we received it and queued it". The log is NOT yet in PostgreSQL —
	// that happens in the Log Consumer service downstream.
	h.metrics.RequestsTotal.WithLabelValues(statusAccepted, tenantID).Inc()
	h.logger.Info("log accepted",
		"trace_id", entry.TraceID,
		"service", entry.Service,
		"level", entry.Level,
	)
	h.writeJSON(w, http.StatusAccepted, acceptedResponse{
		TraceID:   entry.TraceID,
		Status:    statusAccepted,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	})
}

// Health handles GET /health.
// Returns 200 if Kafka is reachable, 503 if not.
// Used by Docker HEALTHCHECK and Kubernetes liveness probes.
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	if err := h.producer.HealthCheck(); err != nil {
		h.logger.Warn("kafka health check failed", "error", err)
		// 503 Service Unavailable — this instance is degraded, redirect traffic elsewhere.
		h.writeJSON(w, http.StatusServiceUnavailable, healthResponse{
			Status: "degraded",
			Kafka:  "unreachable",
		})
		return
	}
	h.writeJSON(w, http.StatusOK, healthResponse{
		Status: "ok",
		Kafka:  "connected",
	})
}

// extractTenantID reads tenant_id from a log entry's metadata map.
// Returns "unknown" when the field is absent or not a non-empty string.
func extractTenantID(metadata map[string]interface{}) string {
	if metadata == nil {
		return "unknown"
	}
	if tid, ok := metadata["tenant_id"].(string); ok && tid != "" {
		return tid
	}
	return "unknown"
}

// validateEntry checks all required fields and valid enum values.
// Returns a descriptive error — the message goes directly to the API caller.
func (h *Handler) validateEntry(entry *LogEntry) error {
	if entry.Service == "" {
		return fmt.Errorf("service is required")
	}
	if entry.Level == "" {
		return fmt.Errorf("level is required")
	}
	// Map lookup: validLevels[key] returns (value, ok). Here we only need the bool.
	// If the key is absent, Go returns the zero value (false) — no panic.
	if !validLevels[entry.Level] {
		return fmt.Errorf("level must be one of DEBUG, INFO, WARN, ERROR, FATAL — got %q", entry.Level)
	}
	if entry.Message == "" {
		return fmt.Errorf("message is required")
	}
	return nil
}

// buildKafkaPayload constructs the enriched JSON payload that goes into Kafka.
// We add Timestamp here (not in the HTTP response) because Kafka consumers
// need a reliable UTC timestamp on every message.
func (h *Handler) buildKafkaPayload(entry *LogEntry) ([]byte, error) {
	metadata := entry.Metadata
	// Normalise nil map to empty map so JSON encodes as {} not null.
	if metadata == nil {
		metadata = make(map[string]interface{})
	}
	msg := kafkaMessage{
		Service:   entry.Service,
		Level:     entry.Level,
		Message:   entry.Message,
		TraceID:   entry.TraceID,
		Metadata:  metadata,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
	// json.Marshal returns ([]byte, error). []byte is a raw byte slice of the JSON string.
	return json.Marshal(msg)
}

// writeJSON sets the Content-Type header, writes the HTTP status code,
// then encodes the body struct as JSON into the response.
// Must be called only once per request — headers cannot be changed after WriteHeader.
func (h *Handler) writeJSON(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status) // sends the status line + headers to the client
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// At this point the status is already sent — we can't change it.
		// Log the error but don't try to write another response.
		h.logger.Error("failed to write JSON response", "error", err)
	}
}

// writeError is a convenience wrapper that builds the error shape
// and delegates to writeJSON. All error responses go through this single path
// so the shape is always consistent across every endpoint.
func (h *Handler) writeError(w http.ResponseWriter, status int, code, message, requestID string) {
	h.writeJSON(w, status, errorResponse{
		Error: errorDetail{
			Code:      code,
			Message:   message,
			RequestID: requestID,
		},
	})
}
