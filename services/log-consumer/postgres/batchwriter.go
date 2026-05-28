package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/agentic-log-analytics/log-consumer/kafka"
	"github.com/agentic-log-analytics/log-consumer/metrics"
)

// Retry backoff constants for exponential backoff with jitter.
// Delay sequence (before jitter): 1s → 2s → 4s.
const (
	baseRetryDelay = 1 * time.Second
	maxRetryDelay  = 8 * time.Second
	// jitterFraction adds ±25% randomness to each delay, preventing thundering-herd
	// when multiple BatchWriter instances retry simultaneously after a DB outage.
	jitterFraction = 0.25
)

// dlqMessage is the payload published to logs.dlq when a batch fails all retries.
// every DLQ message must include enough
// context to debug the failure AND replay the original records without any
// external data source.
type dlqMessage struct {
	OriginalTopic  string      `json:"original_topic"`
	FailureReason  string      `json:"failure_reason"`
	RetryCount     int         `json:"retry_count"`
	// UTC RFC3339Nano — all timestamps UTC.
	FirstAttemptAt string      `json:"first_attempt_at"`
	LastAttemptAt  string      `json:"last_attempt_at"`
	// Full batch payload for replay. Stored as LogRecord not raw bytes
	// so a replay script can re-inject directly without re-parsing.
	OriginalPayload []LogRecord `json:"original_payload"`
}

// BatchWriter accumulates LogRecord values and flushes them to PostgreSQL
// when either the batch reaches batchSize (size trigger) OR the flush
// timer fires (time trigger).
// Why dual-trigger flushing:
//   Size trigger: during bursts (100 logs/sec), batches fill quickly.
//   Without the size trigger, all 100 records accumulate until the 5-second
//   timer fires, increasing end-to-end latency unnecessarily.
//   Time trigger: during quiet periods (1 log/min), the batch never reaches
//   100 records. Without the time trigger, those logs sit in memory indefinitely
//   and are never persisted until the next size-triggered flush.
// Dependencies are injected via NewBatchWriter (Dependency Inversion):
//   - LogRepository for PostgreSQL writes (contains all SQL)
//   - KafkaPublisher for logs.enriched publishes and DLQ writes
//   - Metrics for Prometheus counters
// BatchWriter contains zero SQL and zero Kafka protocol code.
type BatchWriter struct {
	repo          LogRepository
	publisher     kafka.KafkaPublisher
	outputTopic   string // logs.enriched
	dlqTopic      string // logs.dlq
	inputTopic    string // logs.raw.clean — written into DLQ messages
	batchSize     int
	flushInterval time.Duration
	maxRetries    int
	// pending holds records that have been received but not yet flushed.
	// Protected by mu — both the consumer loop (Add) and timer goroutine (flush)
	// access this slice concurrently.
	pending       []LogRecord
	mu            sync.Mutex
	logger        *slog.Logger
	m             *metrics.Metrics
	// stopCh signals the timer goroutine to stop on graceful shutdown.
	stopCh        chan struct{}
	// doneCh is closed when the timer goroutine has exited — main waits on it.
	doneCh        chan struct{}
}

// NewBatchWriter constructs a BatchWriter with all dependencies injected.
// Start the flush timer goroutine by calling StartFlushTimer after construction.
func NewBatchWriter(
	repo LogRepository,
	publisher kafka.KafkaPublisher,
	outputTopic, dlqTopic, inputTopic string,
	batchSize int,
	flushInterval time.Duration,
	maxRetries int,
	logger *slog.Logger,
	m *metrics.Metrics,
) *BatchWriter {
	return &BatchWriter{
		repo:          repo,
		publisher:     publisher,
		outputTopic:   outputTopic,
		dlqTopic:      dlqTopic,
		inputTopic:    inputTopic,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		maxRetries:    maxRetries,
		// Pre-allocate with batchSize capacity to avoid slice growth on initial appends.
		pending:       make([]LogRecord, 0, batchSize),
		logger:        logger,
		m:             m,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
}

// Add appends a LogRecord to the pending batch.
// When the batch reaches batchSize, flush is called synchronously.
// Synchronous flush provides natural backpressure: if the DB is slow,
// the consumer loop slows down rather than accumulating unbounded memory.
func (b *BatchWriter) Add(record LogRecord) {
	b.mu.Lock()
	b.pending = append(b.pending, record)
	size := len(b.pending)
	b.mu.Unlock()

	// Size trigger: flush immediately when capacity is reached.
	if size >= b.batchSize {
		// context.Background because Add has no context from the caller —
		// this flush is an internal operation driven by batch size, not a request.
		b.flush(context.Background())
	}
}

// StartFlushTimer launches the timer goroutine that flushes the batch every
// flushInterval even if the batch is not full.
// This goroutine runs until Stop is called.
// go func launches a goroutine — a lightweight thread managed by the Go runtime.
// We use a goroutine here because time.NewTicker blocks until a tick arrives, and
// we need the consumer loop in main to run concurrently with the flush timer.
func (b *BatchWriter) StartFlushTimer() {
	go func() {
		// close(b.doneCh) runs when this goroutine exits, unblocking Stop.
		defer close(b.doneCh)

		// time.NewTicker fires at every flushInterval duration.
		ticker := time.NewTicker(b.flushInterval)
		// defer ticker.Stop releases the ticker's internal goroutine and channel.
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Time trigger: flush whatever has accumulated since the last tick.
				b.flush(context.Background())

			case <-b.stopCh:
				// Shutdown signal received. Flush remaining records before exiting
				// so no records are lost during graceful shutdown.
				b.logger.Info("flush_timer_stopping_final_flush")
				b.flush(context.Background())
				return
			}
		}
	}()
}

// Stop signals the timer goroutine to finish and blocks until it has.
// Call this during graceful shutdown after the consumer loop has exited,
// so no more Add calls arrive while we're flushing the final batch.
func (b *BatchWriter) Stop() {
	// close(b.stopCh) unblocks the <-b.stopCh case in the timer goroutine.
	close(b.stopCh)
	// <-b.doneCh blocks until the goroutine closes doneCh (after final flush).
	<-b.doneCh
}

// flush atomically takes the current pending batch, resets it, then processes
// the batch outside the lock. This design means:
//   - Add is never blocked by a slow DB operation
//   - The lock is held only for a slice swap (nanoseconds), not for DB I/O
//   - A concurrent flush from both the timer and a size trigger is safe:
//     whichever runs first takes the records, the other sees an empty slice and returns
func (b *BatchWriter) flush(ctx context.Context) {
	// --- Take the batch under the lock ---
	b.mu.Lock()
	if len(b.pending) == 0 {
		b.mu.Unlock()
		return // Nothing to flush — skip entirely
	}
	// Swap pending with a fresh slice. The old slice (now `batch`) is owned
	// exclusively by this flush invocation. New Add calls write to the new slice.
	batch := b.pending
	b.pending = make([]LogRecord, 0, b.batchSize)
	b.mu.Unlock()

	// --- Deduplication by TraceID ---
	// Records with the same TraceID represent duplicate deliveries from Kafka
	// (at-least-once semantics). We keep only the first occurrence within each batch.
	// Records with empty TraceID are never deduplicated (each is unique).
	batch = deduplicateByTraceID(batch)

	b.logger.Info("batch_flush_started", "size", len(batch))

	// --- Retry loop with exponential backoff ---
	b.flushWithRetry(ctx, batch)
}

// flushWithRetry attempts BulkInsert up to maxRetries times.
// On success: publishes each record to logs.enriched and records metrics.
// On exhaustion: sends the batch to the DLQ and records the DLQ metric.
func (b *BatchWriter) flushWithRetry(ctx context.Context, batch []LogRecord) {
	// Record the time of the first attempt for the DLQ message.
	firstAttemptAt := time.Now().UTC()
	var lastErr error

	for attempt := 0; attempt < b.maxRetries; attempt++ {
		if err := b.repo.BulkInsert(ctx, batch); err != nil {
			lastErr = err
			b.logger.Warn(
				"bulk_insert_failed",
				"attempt", attempt+1,
				"max_retries", b.maxRetries,
				"error", err,
			)

			// Don't sleep after the last attempt — we're about to go to DLQ.
			if attempt < b.maxRetries-1 {
				delay := computeBackoffDelay(attempt)
				b.logger.Info("bulk_insert_retrying", "delay_ms", delay.Milliseconds())
				time.Sleep(delay)
			}
			continue
		}

		// --- Insert succeeded ---
		// Publish each record to logs.enriched so downstream consumers
		// (anomaly detector, etc.) can react to this data.
		// Best-effort: publish failures are logged but do not trigger a retry
		// (the record is already safely in PostgreSQL).
		b.publishToEnriched(ctx, batch)

		// Update Prometheus counters.
		b.m.BatchesTotal.Inc()
		b.m.LogsInsertedTotal.Add(float64(len(batch)))

		b.logger.Info("batch_flush_complete", "inserted", len(batch))
		return
	}

	// --- All retries exhausted ---
	// never silently discard messages.
	// publish to DLQ with full context.
	b.logger.Error(
		"bulk_insert_all_retries_exhausted",
		"retries", b.maxRetries,
		"batch_size", len(batch),
		"error", lastErr,
	)
	b.sendToDLQ(ctx, batch, lastErr, firstAttemptAt)
	b.m.BatchesTotal.Inc()
	b.m.DLQTotal.Inc()
}

// publishToEnriched publishes each record's JSON to the logs.enriched topic.
// The anomaly detection agent consumes this topic to run statistical and semantic checks.
// Failures are logged at WARN but do not block the pipeline — the record is
// already in PostgreSQL, so the critical persistence is preserved.
func (b *BatchWriter) publishToEnriched(ctx context.Context, batch []LogRecord) {
	for _, record := range batch {
		payload, err := json.Marshal(record)
		if err != nil {
			b.logger.Warn("enriched_marshal_failed", "service", record.Service, "error", err)
			continue
		}
		// Use TraceID as the partition key so all logs from the same request
		// are ordered within a partition. Empty TraceID uses an empty key (round-robin).
		if err := b.publisher.Publish(ctx, b.outputTopic, record.TraceID, payload); err != nil {
			b.logger.Warn(
				"enriched_publish_failed",
				"topic", b.outputTopic,
				"service", record.Service,
				"error", err,
			)
		}
	}
}

// sendToDLQ publishes one DLQ message for the entire failed batch.
// mandates: original_topic, failure_reason, retry_count,
// first_seen_at (UTC), last_attempt_at (UTC), original_payload.
// every DLQ write is logged at ERROR.
func (b *BatchWriter) sendToDLQ(ctx context.Context, batch []LogRecord, lastErr error, firstAttemptAt time.Time) {
	msg := dlqMessage{
		OriginalTopic: b.inputTopic,
		FailureReason: fmt.Sprintf("%v", lastErr),
		RetryCount:    b.maxRetries,
		// .UTC.Format(time.RFC3339Nano) produces "2024-01-15T10:23:45.123456789Z".
		FirstAttemptAt: firstAttemptAt.UTC().Format(time.RFC3339Nano),
		LastAttemptAt:  time.Now().UTC().Format(time.RFC3339Nano),
		OriginalPayload: batch,
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		b.logger.Error("dlq_marshal_failed", "error", err)
		return
	}

	// Use empty key for DLQ messages — DLQ partitioning is not order-sensitive.
	if err := b.publisher.Publish(ctx, b.dlqTopic, "", payload); err != nil {
		// DLQ publish failed: log at ERROR but cannot do anything else.
		// This is the "last resort" — if even the DLQ is unreachable, we log and move on.
		b.logger.Error(
			"dlq_publish_failed",
			"topic", b.dlqTopic,
			"batch_size", len(batch),
			"error", err,
		)
	} else {
		b.logger.Error(
			"batch_sent_to_dlq",
			"topic", b.dlqTopic,
			"batch_size", len(batch),
			"failure_reason", msg.FailureReason,
		)
	}
}

// deduplicateByTraceID returns a new slice with at most one record per TraceID.
// Records with empty TraceIDs are always kept (no deduplication key available).
// Order is preserved: the first occurrence of each TraceID is kept.
// Why deduplicate in the batch, not just at DB level?
//   The logs table has no UNIQUE constraint on trace_id because:
//   1. trace_id is nullable — NULL != NULL in SQL, so UNIQUE allows duplicates.
//   2. Adding UNIQUE would require a partial index (WHERE trace_id IS NOT NULL)
//      AND would slow down COPY inserts with uniqueness checks on every row.
//   Application-level dedup is faster and avoids the index overhead.
func deduplicateByTraceID(batch []LogRecord) []LogRecord {
	seen := make(map[string]struct{}, len(batch))
	result := make([]LogRecord, 0, len(batch))

	for _, r := range batch {
		if r.TraceID == "" {
			// No deduplication key — always include
			result = append(result, r)
			continue
		}
		if _, exists := seen[r.TraceID]; !exists {
			seen[r.TraceID] = struct{}{}
			result = append(result, r)
		}
	}
	return result
}

// computeBackoffDelay returns an exponentially increasing duration with jitter.
// attempt=0 → ~1s, attempt=1 → ~2s, attempt=2 → ~4s (before jitter).
// Jitter of ±25% prevents thundering-herd when multiple instances retry together.
func computeBackoffDelay(attempt int) time.Duration {
	// 1 << attempt: 1, 2, 4, 8... (left-shift doubles the value each step)
	delay := baseRetryDelay * time.Duration(1<<attempt)
	if delay > maxRetryDelay {
		delay = maxRetryDelay
	}
	// rand.Float64 returns [0.0, 1.0). Transform to [-1.0, 1.0) for symmetric jitter.
	jitter := time.Duration(float64(delay) * jitterFraction * (rand.Float64()*2 - 1))
	return delay + jitter
}
