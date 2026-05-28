package kafka

import (
	"context"
	"fmt"
	"net"
	"time"

	// Alias to avoid conflict with this package name.
	kafkago "github.com/segmentio/kafka-go"
)

const (
	dialTimeoutSeconds = 5  // max seconds for TCP broker connectivity check
	minBytesPerFetch   = 1  // return as soon as at least 1 byte is available
	maxBytesPerFetch   = 10 << 20 // 10 MiB max per fetch — caps memory per read
)

// Message represents one Kafka message received by the consumer.
// The inner field is typed as any to keep the kafka-go dependency
// out of the public API surface — callers never import kafka-go directly.
// This is Dependency Inversion: the interface is defined in terms of Message,
// not in terms of kafka-go's concrete types.
type Message struct {
	Value []byte // raw message payload bytes
	// inner holds the kafka-go Message needed for CommitMessage.
	// Unexported — only the kafkaConsumer implementation accesses it.
	inner any
}

// KafkaConsumer is the interface all callers depend on for message consumption.
// By depending on this interface, the consumer loop in main.go can be tested
// with a mock that returns pre-built Message values — no real broker needed.
// This is the Dependency Inversion principle from SOLID.
type KafkaConsumer interface {
	// FetchMessage blocks until the next message is available or ctx is cancelled.
	// Returns the message bytes and an opaque inner token for CommitMessage.
	FetchMessage(ctx context.Context) (Message, error)

	// CommitMessage acknowledges the given message to the broker.
	// Kafka will not redeliver it on the next consumer group restart.
	// Must only be called with a Message returned by this same consumer instance.
	CommitMessage(ctx context.Context, msg Message) error

	// HealthCheck verifies at least one broker is reachable via TCP.
	HealthCheck() error

	// Close shuts down the reader and releases all resources.
	Close() error
}

// kafkaConsumer is the private concrete implementation of KafkaConsumer.
// Lowercase = unexported: callers use the interface, not this struct.
type kafkaConsumer struct {
	reader  *kafkago.Reader
	brokers []string // retained for HealthCheck re-dials
}

// NewKafkaConsumer creates a Reader subscribed to the given topic and consumer group.
// Returns an error if no broker is reachable at construction time (fail fast).
// groupID controls offset tracking: all consumers with the same group ID share
// the partition assignments. Messages are delivered to exactly one member of the group.
// This lets you scale the consumer by running multiple instances without duplicate processing.
func NewKafkaConsumer(brokers []string, topic, groupID string) (KafkaConsumer, error) {
	// Verify at least one broker is reachable before declaring success.
	if err := verifyBrokerConnectivity(brokers); err != nil {
		return nil, fmt.Errorf("verifying kafka broker connectivity: %w", err)
	}

	// kafkago.NewReader creates a consumer that tracks offsets via the broker.
	// CommitMessages calls are required to advance the committed offset.
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
		// MinBytes: return as soon as any data arrives — low latency.
		MinBytes: minBytesPerFetch,
		// MaxBytes: cap memory usage per fetch call.
		MaxBytes: maxBytesPerFetch,
		// CommitInterval=0 means offsets are committed only when CommitMessages is called.
		// This is manual commit mode — critical for at-least-once delivery semantics.
		// With auto-commit, an offset could be committed before the record is safely
		// inserted into PostgreSQL, causing data loss on crash.
		CommitInterval: 0,
	})

	return &kafkaConsumer{reader: reader, brokers: brokers}, nil
}

// FetchMessage blocks until the next unprocessed message is available.
// ctx cancellation causes an immediate return with the ctx error.
// The returned Message.inner is the kafka-go Message needed for CommitMessage.
func (c *kafkaConsumer) FetchMessage(ctx context.Context) (Message, error) {
	// FetchMessage waits for the next message from the broker.
	// It does NOT commit the offset — that only happens via CommitMessages.
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return Message{}, fmt.Errorf("fetching kafka message: %w", err)
	}
	return Message{
		Value: msg.Value,
		// Store the full kafka-go Message so CommitMessage can pass it back.
		// The type assertion in CommitMessage recovers it safely.
		inner: msg,
	}, nil
}

// CommitMessage commits the offset for the given message to the Kafka broker.
// Once committed, the consumer group will not redeliver this message.
// This should be called only AFTER the message has been durably processed
// (inserted into PostgreSQL and published to logs.enriched), not before.
func (c *kafkaConsumer) CommitMessage(ctx context.Context, msg Message) error {
	// Recover the kafka-go Message from the opaque inner field.
	// This panics if msg was not created by this consumer — a programming error.
	kafkaMsg, ok := msg.inner.(kafkago.Message)
	if !ok {
		return fmt.Errorf("commit called with invalid message token (type: %T)", msg.inner)
	}
	// CommitMessages marks this offset as processed. The broker advances
	// the consumer group's committed offset to this point.
	if err := c.reader.CommitMessages(ctx, kafkaMsg); err != nil {
		return fmt.Errorf("committing kafka offset: %w", err)
	}
	return nil
}

// HealthCheck attempts a TCP dial to each broker and returns nil if any is reachable.
func (c *kafkaConsumer) HealthCheck() error {
	return verifyBrokerConnectivity(c.brokers)
}

// Close stops the reader and releases the underlying TCP connections.
// Must be called during graceful shutdown after the consumer loop exits.
func (c *kafkaConsumer) Close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("closing kafka reader: %w", err)
	}
	return nil
}

// verifyBrokerConnectivity performs a raw TCP dial to each broker.
// Returns nil as soon as any broker responds — we only need one for operation.
// Returns an error only if ALL brokers fail the dial.
func verifyBrokerConnectivity(brokers []string) error {
	timeout := dialTimeoutSeconds * time.Second
	var lastErr error

	for _, broker := range brokers {
		// net.DialTimeout opens a TCP connection. We don't need the connection —
		// just confirmation the port is open. Close immediately after success.
		conn, err := net.DialTimeout("tcp", broker, timeout)
		if err != nil {
			lastErr = err
			continue
		}
		// defer is not used here because we want to close immediately, not at
		// function return, to free the connection as fast as possible.
		conn.Close()
		return nil
	}

	if lastErr != nil {
		return fmt.Errorf("no kafka broker reachable (tried %d): %w", len(brokers), lastErr)
	}
	return fmt.Errorf("no brokers provided")
}
