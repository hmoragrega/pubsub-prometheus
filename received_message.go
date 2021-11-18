package pubsub_prometheus

import (
	"context"
	"time"

	"github.com/hmoragrega/pubsub"
	"github.com/prometheus/client_golang/prometheus"
)

var _ pubsub.ReceivedMessage = (*receivedMessage)(nil)

type receivedMessage struct {
	pubsub.ReceivedMessage
	consumerName string
	ackCounter   *prometheus.CounterVec
}

func wrap(msg pubsub.ReceivedMessage, consumerName string, ackCounter *prometheus.CounterVec) *receivedMessage {
	return &receivedMessage{
		ReceivedMessage: msg,
		consumerName:    consumerName,
		ackCounter:      ackCounter,
	}
}

func (m *receivedMessage) Ack(ctx context.Context) (err error) {
	defer func() {
		m.count("ack", err)
	}()

	return m.ReceivedMessage.Ack(ctx)
}

func (m *receivedMessage) NAck(ctx context.Context) (err error) {
	defer func() {
		m.count("nack", err)
	}()

	return m.ReceivedMessage.NAck(ctx)
}

func (m *receivedMessage) ReSchedule(ctx context.Context, delay time.Duration) (err error) {
	defer func() {
		m.count("re-schedule", err)
	}()

	return m.ReceivedMessage.ReSchedule(ctx, delay)
}

func (m *receivedMessage) count(operation string, err error) {
	m.ackCounter.
		With(metricLabels(map[string]string{operationKey: operation}, m.consumerName, m.ReceivedMessage, err)).
		Add(1)
}
