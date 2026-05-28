package kafka

import (
	"context"
	"fmt"
	"net"
	"time"

	// Alias the import as "kafkago" to avoid naming conflict with this package ("kafka").
	kafkago "github.com/segmentio/kafka-go"
)

const (
	dialTimeoutSeconds  = 5  // max seconds to wait when TCP-dialing a broker for health checks
	writeTimeoutSeconds = 10 // max seconds to wait for a Kafka broker to acknowledge a write
)

// KafkaProducer is the interface all callers depend on.
// By depending on an interface (not *kafkago.Writer directly), we can:
//   - Swap in a mock in tests without a real Kafka broker
//   - Add retry/circuit-breaker wrappers without touching callers
// This is the Dependency Inversion principle from SOLID.
type KafkaProducer interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
	HealthCheck() error
	Close() error
}

// kafkaProducer is the private concrete implementation of KafkaProducer.
// Lowercase = unexported: callers must use the KafkaProducer interface, not this type.
type kafkaProducer struct {
	writer  *kafkago.Writer // the actual kafka-go writer that sends bytes to the broker
	brokers []string        // kept for health checks (re-dial on each check)
}

// NewKafkaProducer is a factory function — the only way to create a kafkaProducer.
// It verifies connectivity first so the service fails fast at startup if Kafka is down,
// rather than silently failing on the first Publish call.
func NewKafkaProducer(brokers []string) (KafkaProducer, error) {
	// verifyConnectivity dials each broker over TCP.
	// %w wraps the original error so callers can unwrap it with errors.Is/As.
	if err := verifyConnectivity(brokers); err != nil {
		return nil, fmt.Errorf("verifying kafka connectivity: %w", err)
	}

	// kafkago.Writer is the kafka-go producer. Key settings explained:
	writer := &kafkago.Writer{
		// Addr specifies the broker(s). TCP accepts variadic "host:port" strings.
		Addr: kafkago.TCP(brokers...),

		// LeastBytes sends each message to the partition with the fewest bytes in flight.
		// This minimises latency without requiring a fixed key-to-partition mapping.
		Balancer: &kafkago.LeastBytes{},

		// WriteTimeout: if the broker doesn't ack within this window, Publish returns an error.
		WriteTimeout: writeTimeoutSeconds * time.Second,

		// RequireOne means: wait for the partition leader to confirm the write.
		// Faster than RequireAll (all replicas) but message survives a leader restart.
		RequiredAcks: kafkago.RequireOne,

		// Disable auto topic creation — topics are pre-created by kafka-init.
		// If we typo a topic name, we get an error immediately rather than silently
		// creating a wrong topic and losing messages.
		AllowAutoTopicCreation: false,
	}

	return &kafkaProducer{
		writer:  writer,
		brokers: brokers,
	}, nil
}

// Publish sends one message to the specified Kafka topic.
// ctx carries a deadline/cancellation: if the caller's HTTP request is cancelled
// (client disconnects), WriteMessages aborts rather than blocking indefinitely.
// key is used for partition routing — same key always goes to the same partition,
// preserving ordering for that key. We use trace_id as the key.
func (p *kafkaProducer) Publish(ctx context.Context, topic, key string, value []byte) error {
	msg := kafkago.Message{
		Topic: topic,
		Key:   []byte(key),   // []byte() converts string to byte slice — Kafka works with bytes
		Value: value,
		Time:  time.Now().UTC(), // UTC timestamp on the Kafka message envelope
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		// Wrap the error with context so the caller knows which topic failed.
		return fmt.Errorf("publishing to topic %s: %w", topic, err)
	}
	return nil
}

// HealthCheck re-dials the broker list to confirm at least one is reachable.
// This is called by GET /health — it's intentionally lightweight (TCP dial only,
// no consumer group registration or metadata fetch).
func (p *kafkaProducer) HealthCheck() error {
	return verifyConnectivity(p.brokers)
}

// Close flushes any buffered messages and closes the underlying TCP connections.
// Must be called during graceful shutdown — after HTTP server drains, before process exits.
func (p *kafkaProducer) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("closing kafka writer: %w", err)
	}
	return nil
}

// verifyConnectivity attempts a raw TCP dial to each broker.
// Returns nil as soon as any one broker responds — we only need one reachable broker.
// Returns an error only if ALL brokers fail.
func verifyConnectivity(brokers []string) error {
	timeout := dialTimeoutSeconds * time.Second
	var lastErr error

	for _, broker := range brokers {
		// net.DialTimeout opens a TCP connection to "host:port" within the timeout.
		conn, err := net.DialTimeout("tcp", broker, timeout)
		if err != nil {
			lastErr = err
			continue // try next broker
		}
		// We only needed to confirm the port is open — close immediately.
		conn.Close()
		return nil // at least one broker is reachable
	}

	// If we get here, every broker failed.
	if lastErr != nil {
		return fmt.Errorf("no kafka broker reachable (tried %d brokers): %w", len(brokers), lastErr)
	}
	return fmt.Errorf("no brokers provided")
}
