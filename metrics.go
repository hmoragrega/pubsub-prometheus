package pubsub_prometheus

import (
	"context"
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
	topicKey      labelKey = "topic"
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

	// PublishOpts optional options for the publishing attempts histogram.
	PublishOpts prometheus.HistogramOpts

	// PublishedOpts optional options for the published messages counter.
	PublishedOpts prometheus.CounterOpts

	// ConsumedOpts optional options for the subscriber next message counter.
	ConsumedOpts prometheus.CounterOpts

	// Namespace will be used on all metrics unless overwritten by the
	// specific metric config.
	Namespace string

	// Subsystem will be used on all metrics unless overwritten by the
	// specific metric config.
	Subsystem string

	// ConstLabels can be used to add constant label in the all the metrics.
	ConstLabels prometheus.Labels

	processed  *prometheus.HistogramVec
	checkpoint *prometheus.CounterVec
	ack        *prometheus.CounterVec
	publish    *prometheus.HistogramVec
	published  *prometheus.CounterVec
	consumed   *prometheus.CounterVec

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
			m.buildPublish(m.PublishOpts),
			m.buildPublished(m.PublishedOpts),
			m.buildConsumed(m.ConsumedOpts),
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

// MustInstrumentRouterWithMonitor helper to instrument a router and returns the same instance,
// use it for one line router initializations.
//
// It will panic on metric registration error.
func MustInstrumentRouterWithMonitor(monitor *Monitor, router *pubsub.Router) *pubsub.Router {
	return monitor.MustInstrumentRouter(router)
}

// MustInstrumentRouter helper to instrument a router and returns the same instance,
// use it for one line router initializations.
//
// It will panic on metric registration error.
func MustInstrumentRouter(router *pubsub.Router) *pubsub.Router {
	return MustInstrumentRouterWithMonitor(&Monitor{}, router)
}

// MustInstrumentRouter helper to instrument a router and returns the same instance.
//
// It will panic on metric registration error.
func (m *Monitor) MustInstrumentRouter(router *pubsub.Router) *pubsub.Router {
	if err := m.InstrumentRouter(router); err != nil {
		panic(err)
	}
	return router
}

// InstrumentRouterWithMonitor helper to instrument a router returning any errors that may happen.
func InstrumentRouterWithMonitor(monitor *Monitor, router *pubsub.Router) error {
	return monitor.InstrumentRouter(router)
}

// InstrumentRouter helper to instrument a router returning any errors that may happen.
func InstrumentRouter(router *pubsub.Router) error {
	return InstrumentRouterWithMonitor(&Monitor{}, router)
}

// InstrumentRouter a router returning any errors that may happen.
func (m *Monitor) InstrumentRouter(router *pubsub.Router) error {
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

	// acknowledgements and consume operations
	router.OnNext = pubsub.WrapNext(router.OnNext, m.onNext)

	return nil
}

func (m *Monitor) onNext(_ context.Context, consumerName string, next pubsub.Next) pubsub.Next {
	if msg := next.Message; msg != nil {
		next.Message = wrap(msg, consumerName, m.ack)
	}

	m.consumed.With(
		metricLabels(nil, consumerName, next.Message, next.Err)).
		Add(1)

	return next
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
	if opts.Namespace == "" {
		opts.Namespace = m.Namespace
	}
	if opts.Subsystem == "" {
		opts.Subsystem = m.Subsystem
	}

	opts.ConstLabels = mergeLabels(m.ConstLabels, opts.ConstLabels)

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
	if opts.Namespace == "" {
		opts.Namespace = m.Namespace
	}
	if opts.Subsystem == "" {
		opts.Subsystem = m.Subsystem
	}

	opts.ConstLabels = mergeLabels(m.ConstLabels, opts.ConstLabels)

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
	if opts.Namespace == "" {
		opts.Namespace = m.Namespace
	}
	if opts.Subsystem == "" {
		opts.Subsystem = m.Subsystem
	}

	opts.ConstLabels = mergeLabels(m.ConstLabels, opts.ConstLabels)

	h := prometheus.NewCounterVec(opts, metricKeys(operationKey))
	m.ack = h
	return h
}

func (m *Monitor) buildConsumed(opts prometheus.CounterOpts) *prometheus.CounterVec {
	if opts.Name == "" {
		opts.Name = "pubsub_message_consumed"
	}
	if opts.Help == "" {
		opts.Help = "Counter consumed next message"
	}
	if opts.Namespace == "" {
		opts.Namespace = m.Namespace
	}
	if opts.Subsystem == "" {
		opts.Subsystem = m.Subsystem
	}

	opts.ConstLabels = mergeLabels(m.ConstLabels, opts.ConstLabels)

	h := prometheus.NewCounterVec(opts, metricKeys())
	m.consumed = h
	return h
}

func metricKeys(keys ...string) []string {
	return append(keys, commonLabels...)
}

func metricLabels(custom map[string]string, consumerName string, msg pubsub.ReceivedMessage, err error) map[string]string {
	var version string
	var msgName string
	if msg != nil {
		version = msg.Version()
		msgName = msg.Name()
	}

	labels := map[string]string{
		consumerKey:   consumerName,
		msgNameKey:    msgName,
		msgVersionKey: version,
		errorKey:      errorLabel(err),
	}
	for k, v := range custom {
		labels[k] = v
	}
	return labels
}

func mergeLabels(a, b map[string]string) (merged map[string]string) {
	merged = make(map[string]string, len(a)+len(b))
	for k, v := range a {
		merged[k] = v
	}
	for k, v := range b {
		merged[k] = v
	}
	return merged
}

func errorLabel(err error) string {
	if err != nil {
		return "true"
	}
	return "false"
}
