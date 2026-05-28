package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/agentic-log-analytics/log-ingestion/handler"
	"github.com/agentic-log-analytics/log-ingestion/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// mockProducer is a test double that satisfies the KafkaProducer interface
// without any real Kafka broker. Tests control publish behavior via fields.
type mockProducer struct {
	publishErr     error
	healthCheckErr error
	published      [][]byte
}

func (m *mockProducer) Publish(_ context.Context, _, _ string, value []byte) error {
	if m.publishErr != nil {
		return m.publishErr
	}
	m.published = append(m.published, value)
	return nil
}

func (m *mockProducer) HealthCheck() error {
	return m.healthCheckErr
}

func (m *mockProducer) Close() error {
	return nil
}

func newTestHandler(producer *mockProducer) *handler.Handler {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	m := metrics.NewWithRegisterer(prometheus.NewRegistry())
	return handler.New(producer, "logs.raw", m, logger)
}

func TestIngestLog_HappyPath_Returns202WithTraceID(t *testing.T) {
	producer := &mockProducer{}
	h := newTestHandler(producer)

	body := `{"service":"payment-service","level":"ERROR","message":"connection refused"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/logs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.IngestLog(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}

	if resp["status"] != "accepted" {
		t.Errorf("expected status=accepted, got %v", resp["status"])
	}
	if resp["trace_id"] == "" {
		t.Error("expected non-empty trace_id in response")
	}

	if len(producer.published) != 1 {
		t.Errorf("expected 1 message published to Kafka, got %d", len(producer.published))
	}
}

func TestIngestLog_InvalidLevel_Returns400WithValidationError(t *testing.T) {
	producer := &mockProducer{}
	h := newTestHandler(producer)

	body := `{"service":"test","level":"INVALID","message":"test"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/logs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.IngestLog(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}

	errBlock, ok := resp["error"].(map[string]interface{})
	if !ok {
		t.Fatal("expected error object in response")
	}
	if errBlock["code"] != "VALIDATION_ERROR" {
		t.Errorf("expected code=VALIDATION_ERROR, got %v", errBlock["code"])
	}

	if len(producer.published) != 0 {
		t.Error("expected no Kafka messages published on validation error")
	}
}

func TestIngestLog_MissingService_Returns400(t *testing.T) {
	producer := &mockProducer{}
	h := newTestHandler(producer)

	body := `{"level":"ERROR","message":"test"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/logs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.IngestLog(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing service, got %d", w.Code)
	}
}

func TestIngestLog_KafkaFailure_Returns503(t *testing.T) {
	producer := &mockProducer{publishErr: fmt.Errorf("broker unavailable")}
	h := newTestHandler(producer)

	body := `{"service":"payment-service","level":"ERROR","message":"test"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/logs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.IngestLog(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on kafka failure, got %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}

	errBlock, ok := resp["error"].(map[string]interface{})
	if !ok {
		t.Fatal("expected error object in response")
	}
	if errBlock["code"] != "KAFKA_UNAVAILABLE" {
		t.Errorf("expected code=KAFKA_UNAVAILABLE, got %v", errBlock["code"])
	}
}

func TestIngestLog_InvalidJSON_Returns400(t *testing.T) {
	producer := &mockProducer{}
	h := newTestHandler(producer)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/logs", bytes.NewBufferString(`not-json`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.IngestLog(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid JSON, got %d", w.Code)
	}
}

func TestHealth_KafkaHealthy_Returns200(t *testing.T) {
	producer := &mockProducer{}
	h := newTestHandler(producer)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	h.Health(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	if resp["status"] != "ok" {
		t.Errorf("expected status=ok, got %v", resp["status"])
	}
	if resp["kafka"] != "connected" {
		t.Errorf("expected kafka=connected, got %v", resp["kafka"])
	}
}

func TestHealth_KafkaUnhealthy_Returns503(t *testing.T) {
	producer := &mockProducer{healthCheckErr: fmt.Errorf("dial tcp: connection refused")}
	h := newTestHandler(producer)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	h.Health(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	if resp["status"] != "degraded" {
		t.Errorf("expected status=degraded, got %v", resp["status"])
	}
}

func TestIngestLog_AutoGeneratesTraceID_WhenNotProvided(t *testing.T) {
	producer := &mockProducer{}
	h := newTestHandler(producer)

	body := `{"service":"auth-service","level":"INFO","message":"user login"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/logs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.IngestLog(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}

	traceID, ok := resp["trace_id"].(string)
	if !ok || traceID == "" {
		t.Error("expected auto-generated trace_id in response")
	}
}
