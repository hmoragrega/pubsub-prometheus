package pubsub_prometheus

import (
	"context"
	"fmt"
	"time"

	"github.com/hmoragrega/pubsub"
	"github.com/prometheus/client_golang/prometheus"
)

// InstrumentPublisher is a helper to instrument a publisher with the default monitor.
func InstrumentPublisher(next pubsub.Publisher) (pubsub.Publisher, error) {
	return InstrumentPublisherWithMonitor(&defaultMonitor, next)
}

// InstrumentPublisherWithMonitor is a helper to instrument a publisher with the default monitor.
func InstrumentPublisherWithMonitor(monitor *Monitor, next pubsub.Publisher) (pubsub.Publisher, error) {
	return monitor.InstrumentPublisher(next)
}

// MustInstrumentPublisher is a helper to instrument a publisher with the default monitor.
func MustInstrumentPublisher(next pubsub.Publisher) pubsub.Publisher {
	return MustInstrumentPublisherWithMonitor(&defaultMonitor, next)
}

// MustInstrumentPublisherWithMonitor is a helper to instrument a publisher with the default monitor.
func MustInstrumentPublisherWithMonitor(monitor *Monitor, next pubsub.Publisher) pubsub.Publisher {
	pub, err := InstrumentPublisherWithMonitor(monitor, next)
	if err != nil {
		panic(err)
	}
	return pub
}

// InstrumentPublisher is a publisher middleware that will send metrics on publishing operations.
func (m *Monitor) InstrumentPublisher(next pubsub.Publisher) (pubsub.Publisher, error) {
	if err := m.Register(); err != nil {
		return nil, err
	}
	return pubsub.PublisherFunc(func(ctx context.Context, topic string, envelopes ...*pubsub.Message) (err error) {
		start := time.Now()
		defer func() {
			m.publish.With(map[string]string{
				topicKey: topic,
				errorKey: fmt.Sprintf("%v", err != nil),
			}).Observe(time.Since(start).Seconds())

			if err == nil {
				m.published.With(map[string]string{
					topicKey: topic,
				}).Add(float64(len(envelopes)))
			}
		}()

		return next.Publish(ctx, topic, envelopes...)
	}), nil
}

// MustInstrumentPublisher is a publisher middleware that will send metrics on publishing operations.
func (m *Monitor) MustInstrumentPublisher(next pubsub.Publisher) pubsub.Publisher {
	return MustInstrumentPublisherWithMonitor(m, next)
}

func (m *Monitor) buildPublish(opts prometheus.HistogramOpts) *prometheus.HistogramVec {
	if opts.Name == "" {
		opts.Name = "pubsub_message_publishing"
	}
	if opts.Help == "" {
		opts.Help = "Publishing calls executed"
	}
	if opts.Namespace == "" {
		opts.Namespace = m.Namespace
	}
	if opts.Subsystem == "" {
		opts.Subsystem = m.Subsystem
	}

	opts.ConstLabels = mergeLabels(m.ConstLabels, opts.ConstLabels)

	h := prometheus.NewHistogramVec(opts, []string{topicKey, errorKey})
	m.publish = h
	return h
}

func (m *Monitor) buildPublished(opts prometheus.CounterOpts) *prometheus.CounterVec {
	if opts.Name == "" {
		opts.Name = "pubsub_message_published"
	}
	if opts.Help == "" {
		opts.Help = "Number of message published"
	}
	if opts.Namespace == "" {
		opts.Namespace = m.Namespace
	}
	if opts.Subsystem == "" {
		opts.Subsystem = m.Subsystem
	}

	opts.ConstLabels = mergeLabels(m.ConstLabels, opts.ConstLabels)

	h := prometheus.NewCounterVec(opts, []string{topicKey})
	m.published = h
	return h
}
