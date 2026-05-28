package kafka

import (
	"context"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

const (
	publishWriteTimeoutSeconds = 10 // max seconds to wait for broker to ack a publish
)

// KafkaPublisher is the interface for publishing messages to Kafka topics.
// Using an interface here (not *kafkago.Writer directly) means:
//   - Tests can inject a no-op mock without a real broker
//   - The BatchWriter never imports kafka-go — only this interface
// This is Dependency Inversion: the postgres package depends on the abstraction
// (this interface), not on the concrete kafka-go library.
type KafkaPublisher interface {
	// Publish sends one message to the named topic with the given key and value.
	// The key controls partition routing: same key → same partition → ordered delivery.
	Publish(ctx context.Context, topic, key string, value []byte) error

	// HealthCheck verifies at least one broker is reachable via TCP.
	HealthCheck() error

	// Close flushes pending messages and releases TCP connections.
	Close() error
}

// kafkaPublisher is the private concrete implementation of KafkaPublisher.
type kafkaPublisher struct {
	writer  *kafkago.Writer
	brokers []string // retained for HealthCheck
}

// NewKafkaPublisher creates a kafka-go Writer and verifies broker connectivity.
// Fails fast at startup if no broker is reachable.
func NewKafkaPublisher(brokers []string) (KafkaPublisher, error) {
	if err := verifyBrokerConnectivity(brokers); err != nil {
		return nil, fmt.Errorf("verifying kafka publisher connectivity: %w", err)
	}

	writer := &kafkago.Writer{
		// TCP accepts variadic "host:port" strings and creates a multi-broker address.
		Addr:     kafkago.TCP(brokers...),
		Balancer: &kafkago.LeastBytes{},
		// WriteTimeout: if the broker doesn't ack within this window, Publish returns error.
		WriteTimeout: publishWriteTimeoutSeconds * time.Second,
		// RequireOne: wait for the partition leader to confirm the write.
		// Balances durability (survives leader restart) with latency (no all-replica wait).
		RequiredAcks: kafkago.RequireOne,
		// AllowAutoTopicCreation=false: all topics are pre-created by kafka-init.
		// A typo in a topic name returns an error immediately instead of silently
		// creating a misnamed topic and losing messages.
		AllowAutoTopicCreation: false,
	}

	return &kafkaPublisher{writer: writer, brokers: brokers}, nil
}

// Publish sends a single message to the specified topic.
// key is used for partition routing — we use trace_id so all logs for the same
// request are ordered within a partition. Downstream consumers that order by
// trace_id see a consistent sequence.
func (p *kafkaPublisher) Publish(ctx context.Context, topic, key string, value []byte) error {
	msg := kafkago.Message{
		Topic: topic,
		// []byte(key) converts the string to a byte slice — Kafka works with bytes.
		Key:   []byte(key),
		Value: value,
		// time.Now.UTC satisfies UTC timestamp on the message envelope.
		Time: time.Now().UTC(),
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		// Wrap with context so callers know which topic failed.
		return fmt.Errorf("publishing to kafka topic %s: %w", topic, err)
	}
	return nil
}

// HealthCheck re-dials brokers to confirm at least one is reachable.
func (p *kafkaPublisher) HealthCheck() error {
	return verifyBrokerConnectivity(p.brokers)
}

// Close flushes buffered messages and releases the underlying TCP connections.
// Called during graceful shutdown after the batch writer stops.
func (p *kafkaPublisher) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("closing kafka publisher: %w", err)
	}
	return nil
}
