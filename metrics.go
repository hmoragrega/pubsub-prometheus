package pubsub_prometheus

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/hmoragrega/pubsub"
	"github.com/prometheus/client_golang/prometheus"
)

type labelKey = string

const (
	consumerKey   labelKey = "consumer"
	msgNameKey    labelKey = "msg_name"
	msgVersionKey labelKey = "msg_version"
	errorKey      labelKey = "error"

	checkpointKey labelKey = "checkpoint"
	operationKey  labelKey = "operation"
)

var (
	commonLabels = []string{consumerKey, msgNameKey, msgVersionKey, errorKey}
)

// Monitor can be used to instrument a pubsub.Router.
//
// The zero value monitor will add the metrics with their default names
// in the prometheus default registerer.
type Monitor struct {
	// Registerer optional registerer for the metrics.
	Registerer prometheus.Registerer

	// ProcessedOpts optional options for the processed message histogram.
	ProcessedOpts prometheus.HistogramOpts

	// CheckpointOpts optional options for the router checkpoints counter.
	CheckpointOpts prometheus.CounterOpts

	// AckOpts optional options for the message acknowledgements counter.
	AckOpts prometheus.CounterOpts

	processed  *prometheus.HistogramVec
	checkpoint *prometheus.CounterVec
	ack        *prometheus.CounterVec

	once        sync.Once
	registerErr error
}

// Register will register the metrics in the prometheus registerer.
//
// Note that Register will be automatically called when instrumenting a router.
// Use it for fine-grained control on registration errors.
//
// Register can only be called once, and successive calls will return the
// first execution error.
func (m *Monitor) Register() error {
	m.once.Do(func() {
		reg := prometheus.DefaultRegisterer
		if m.Registerer != nil {
			reg = m.Registerer
		}

		collectors := []prometheus.Collector{
			m.buildProcessed(m.ProcessedOpts),
			m.buildCheckpoint(m.CheckpointOpts),
			m.buildAck(m.AckOpts),
		}

		result := make(chan error, 4)
		go func() {
			defer close(result)
			for _, c := range collectors {
				result <- reg.Register(c)
			}
		}()

		for err := range result {
			if err != nil {
				m.registerErr = multierror.Append(m.registerErr, err)
				return
			}
		}
	})

	return m.registerErr
}

// MustInstrumentWithMonitor helper to instrument a router and returns the same instance,
// use it for one line router initializations.
//
// It will panic on metric registration error.
func MustInstrumentWithMonitor(monitor *Monitor, router *pubsub.Router) *pubsub.Router {
	return monitor.MustInstrument(router)
}

// MustInstrument helper to instrument a router and returns the same instance,
// use it for one line router initializations.
//
// It will panic on metric registration error.
func MustInstrument(router *pubsub.Router) *pubsub.Router {
	return MustInstrumentWithMonitor(&Monitor{}, router)
}

// MustInstrument helper to instrument a router and returns the same instance.
//
// It will panic on metric registration error.
func (m *Monitor) MustInstrument(router *pubsub.Router) *pubsub.Router {
	if err := m.Instrument(router); err != nil {
		panic(err)
	}
	return router
}

// InstrumentWithMonitor helper to instrument a router returning any errors that may happen.
func InstrumentWithMonitor(monitor *Monitor, router *pubsub.Router) error {
	return monitor.Instrument(router)
}

// Instrument helper to instrument a router returning any errors that may happen.
func Instrument(router *pubsub.Router) error {
	return InstrumentWithMonitor(&Monitor{}, router)
}

// Instrument a router returning any errors that may happen.
func (m *Monitor) Instrument(router *pubsub.Router) error {
	if err := m.Register(); err != nil {
		return err
	}

	// checkpoint counters
	router.OnReceive = pubsub.WrapCheckpoint(router.OnReceive, m.onCheckpoint("receive"))
	router.OnUnmarshal = pubsub.WrapCheckpoint(router.OnUnmarshal, m.onCheckpoint("unmarshal"))
	router.OnHandler = pubsub.WrapCheckpoint(router.OnHandler, m.onCheckpoint("handler"))
	router.OnAck = pubsub.WrapCheckpoint(router.OnAck, m.onCheckpoint("ack"))

	// processed messages
	router.OnProcess = pubsub.WrapOnProcess(router.OnProcess, m.onProcess)

	// acknowledgements
	router.MessageModifier = pubsub.WrapMessageModifier(router.MessageModifier, messageWrapper(m.ack))

	return nil
}

func messageWrapper(ack *prometheus.CounterVec) pubsub.MessageModifier {
	return func(ctx context.Context, consumerName string, message pubsub.ReceivedMessage) pubsub.ReceivedMessage {
		return wrap(message, consumerName, ack)
	}
}

// OnProcess will sent a histogram metric
func (m *Monitor) onProcess(_ context.Context, consumerName string, elapsed time.Duration, msg pubsub.ReceivedMessage, err error) {
	m.processed.
		With(metricLabels(nil, consumerName, msg, err)).
		Observe(elapsed.Seconds())
}

func (m *Monitor) onCheckpoint(checkpoint string) pubsub.Checkpoint {
	return func(ctx context.Context, consumerName string, msg pubsub.ReceivedMessage, err error) error {
		m.checkpoint.
			With(metricLabels(map[string]string{checkpointKey: checkpoint}, consumerName, msg, err)).
			Add(1)

		return nil
	}
}

func (m *Monitor) buildProcessed(opts prometheus.HistogramOpts) *prometheus.HistogramVec {
	if opts.Name == "" {
		opts.Name = "pubsub_message_processed"
	}
	if opts.Help == "" {
		opts.Help = "Number of processed messages"
	}

	h := prometheus.NewHistogramVec(opts, metricKeys())
	m.processed = h
	return h
}

func (m *Monitor) buildCheckpoint(opts prometheus.CounterOpts) *prometheus.CounterVec {
	if opts.Name == "" {
		opts.Name = "pubsub_message_checkpoint"
	}
	if opts.Help == "" {
		opts.Help = "Number of message checkpoints executed"
	}

	h := prometheus.NewCounterVec(opts, metricKeys(checkpointKey))
	m.checkpoint = h
	return h
}

func (m *Monitor) buildAck(opts prometheus.CounterOpts) *prometheus.CounterVec {
	if opts.Name == "" {
		opts.Name = "pubsub_message_acknowledgements"
	}
	if opts.Help == "" {
		opts.Help = "Number of message acknowledgements executed"
	}

	h := prometheus.NewCounterVec(opts, metricKeys(operationKey))
	m.ack = h
	return h
}

func metricKeys(keys ...string) []string {
	return append(keys, commonLabels...)
}

func metricLabels(custom map[string]string, consumerName string, msg pubsub.ReceivedMessage, err error) map[string]string {
	labels := map[string]string{
		consumerKey:   consumerName,
		msgNameKey:    msg.Name(),
		msgVersionKey: msg.Version(),
		errorKey:      fmt.Sprintf("%v", err != nil),
	}
	for k, v := range custom {
		labels[k] = v
	}
	return labels
}
