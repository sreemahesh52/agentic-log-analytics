package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LogRecord is the central data type flowing from the Kafka consumer
// through the BatchWriter into PostgreSQL. It mirrors the cleaned log
// message shape produced by the security middleware.
type LogRecord struct {
	TenantID           string                 // UUID string from metadata.tenant_id
	Service            string                 // producing microservice name
	Level              string                 // DEBUG | INFO | WARN | ERROR | FATAL
	Message            string                 // sanitised log message text
	TraceID            string                 // UUID string; empty string → NULL in DB
	Timestamp          time.Time              // parsed from ISO 8601 string
	Metadata           map[string]interface{} // arbitrary key-value pairs from producer
	InjectionAttempted bool                   // true if injection detector fired
}

// DatabaseWriteError is a typed error returned when a PostgreSQL write fails.
// Using a typed error (not bare error) lets callers distinguish DB failures
// from other error categories with errors.As — enabling targeted retry logic.
type DatabaseWriteError struct {
	Op  string // operation that failed, e.g. "BulkInsert"
	Err error  // underlying error from pgx
}

func (e *DatabaseWriteError) Error() string {
	return fmt.Sprintf("database %s failed: %v", e.Op, e.Err)
}

// Unwrap allows errors.Is and errors.As to traverse the error chain.
func (e *DatabaseWriteError) Unwrap() error {
	return e.Err
}

// LogRepository is the interface for all PostgreSQL operations on the logs table.
// All SQL lives in the implementation — none in the BatchWriter or main.go.
// This is the Repository Pattern: callers depend on intent (BulkInsert), not on SQL.
// Dependency Inversion: BatchWriter depends on this interface, not LogRepositoryImpl,
// so tests can inject a fake repository without a real database.
type LogRepository interface {
	// BulkInsert writes all records in the slice to PostgreSQL using the COPY protocol.
	// Duplicate trace_ids within the slice are deduplicated before writing.
	// Returns DatabaseWriteError on failure.
	BulkInsert(ctx context.Context, logs []LogRecord) error
}

// LogRepositoryImpl is the concrete PostgreSQL implementation of LogRepository.
// It is unexported by convention — callers receive the LogRepository interface.
type LogRepositoryImpl struct {
	// pool is injected via NewLogRepository — never opened inside this struct.
	// connection pooling via pgxpool, never open per-request.
	pool *pgxpool.Pool
}

// NewLogRepository is the factory function for LogRepositoryImpl.
// The pool must already be connected; this function does not open connections.
func NewLogRepository(pool *pgxpool.Pool) LogRepository {
	return &LogRepositoryImpl{pool: pool}
}

// BulkInsert writes a batch of log records to PostgreSQL using the COPY protocol.
// Why COPY over INSERT:
//   The PostgreSQL COPY protocol transfers rows as a stream of text or binary
//   rows directly into the storage layer, bypassing the SQL parser and planner
//   for each row. For batches of 100 rows, COPY is typically 5–10× faster than
//   individual INSERT statements.
// Why CopyFrom over individual INSERTs:
//   pgx.CopyFrom sends the entire slice in one round-trip. Individual INSERTs
//   require one round-trip each — for a batch of 100 this is 100 round-trips vs 1.
// The entire batch is wrapped in an explicit transaction so a partial failure
// (e.g. a CHECK constraint violation on row 50) rolls back all 100 rows.
// The caller's retry logic then resends the full batch.
func (r *LogRepositoryImpl) BulkInsert(ctx context.Context, logs []LogRecord) error {
	if len(logs) == 0 {
		return nil
	}

	// --- Acquire a connection from the pool ---
	// Acquire borrows one connection for the duration of this function.
	// Release returns it automatically via defer.
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return &DatabaseWriteError{Op: "Acquire", Err: err}
	}
	// defer conn.Release returns this connection to the pool even if we return
	// early due to an error — prevents connection leaks.
	defer conn.Release()

	// --- Begin explicit transaction ---
	// any operation modifying data uses an explicit transaction.
	// If CopyFrom fails mid-batch, the transaction rolls back all rows —
	// the next retry starts from a clean slate.
	tx, err := conn.Begin(ctx)
	if err != nil {
		return &DatabaseWriteError{Op: "Begin", Err: err}
	}
	// defer tx.Rollback is a no-op after a successful Commit, but guarantees
	// the transaction is rolled back if we return before reaching Commit.
	defer tx.Rollback(ctx) //nolint:errcheck — rollback error is ignorable here

	// --- Build rows for CopyFrom ---
	// Convert each LogRecord into a row of interface{} values that pgx can encode.
	rows, err := buildCopyRows(logs)
	if err != nil {
		return &DatabaseWriteError{Op: "BuildRows", Err: err}
	}

	// --- Execute COPY ---
	// pgx.Identifier{"logs"} is the table name as a safely-quoted identifier.
	// pgx.CopyFromRows wraps our [][]any as a pgx.CopyFromSource.
	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"logs"},
		[]string{
			"tenant_id",
			"timestamp",
			"service",
			"level",
			"message",
			"trace_id",
			"metadata",
			"injection_attempted",
		},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return &DatabaseWriteError{Op: "CopyFrom", Err: err}
	}

	// --- Commit ---
	if err := tx.Commit(ctx); err != nil {
		return &DatabaseWriteError{Op: "Commit", Err: err}
	}

	return nil
}

// buildCopyRows converts a slice of LogRecord into the [][]any format required
// by pgx.CopyFromRows. Type conversion happens here — never in BulkInsert itself.
// Returns an error if any critical type conversion fails (e.g. invalid UUID).
func buildCopyRows(logs []LogRecord) ([][]any, error) {
	rows := make([][]any, 0, len(logs))

	for i, r := range logs {
		// --- tenant_id (UUID, NOT NULL) ---
		// pgtype.UUID provides a nullable UUID type that pgx v5 encodes correctly
		// for both UUID columns and COPY protocol rows.
		var tenantUUID pgtype.UUID
		if err := tenantUUID.Scan(r.TenantID); err != nil {
			return nil, fmt.Errorf("row %d: invalid tenant_id %q: %w", i, r.TenantID, err)
		}

		// --- trace_id (UUID, nullable) ---
		// pgtype.UUID{Valid: false} encodes as SQL NULL.
		// A missing or unparseable TraceID becomes NULL rather than an error.
		var traceUUID pgtype.UUID
		if r.TraceID != "" {
			// Scan parses the UUID string; ignore parse errors (store NULL instead).
			_ = traceUUID.Scan(r.TraceID)
		}

		// --- metadata (JSONB) ---
		// json.Marshal converts map[string]interface{} to a JSON byte slice.
		// pgx v5 encodes []byte as raw bytes for JSONB — PostgreSQL parses it as JSON.
		metadataJSON, err := json.Marshal(r.Metadata)
		if err != nil {
			// A nil or un-marshallable map defaults to empty object — not an error.
			metadataJSON = []byte(`{}`)
		}

		// --- timestamp (TIMESTAMPTZ) ---
		// pgx v5 encodes time.Time → timestamptz correctly regardless of timezone.

		rows = append(rows, []any{
			tenantUUID,             // tenant_id   UUID NOT NULL
			r.Timestamp,           // timestamp   TIMESTAMPTZ NOT NULL
			r.Service,             // service     TEXT NOT NULL
			r.Level,               // level       TEXT NOT NULL
			r.Message,             // message     TEXT NOT NULL
			traceUUID,             // trace_id    UUID nullable
			metadataJSON,          // metadata    JSONB NOT NULL
			r.InjectionAttempted,  // injection_attempted BOOLEAN NOT NULL
		})
	}

	return rows, nil
}
