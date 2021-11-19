package pubsub_prometheus

import (
	"context"
	"errors"
	"github.com/hmoragrega/pubsub/marshaller"
	"testing"
	"time"

	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/pubsubtest/channels"
	"github.com/prometheus/client_golang/prometheus"
	promclient "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestDefaultInstrument(t *testing.T) {
	t.Cleanup(resetRegisterer)

	require.NoError(t, InstrumentRouter(&pubsub.Router{}))
	require.NotNil(t, InstrumentPublisher(pubsub.NoOpPublisher()))
}

func TestInstrumentMultipleRouters(t *testing.T) {
	t.Cleanup(resetRegisterer)

	t.Run("zero value router can be instrumented", func(t *testing.T) {
		var m Monitor
		require.NotPanics(t, func() {
			MustInstrumentRouterWithMonitor(&m, &pubsub.Router{})
		})

		t.Run("the same monitor can instrument more routers", func(t *testing.T) {
			require.NotPanics(t, func() {
				MustInstrumentRouterWithMonitor(&m, &pubsub.Router{})
			})
		})
	})
}

func TestInstrumentMultipleMonitor(t *testing.T) {
	t.Cleanup(resetRegisterer)

	var r pubsub.Router
	MustInstrumentRouter(&r)

	t.Run("panics if the registerer is not different", func(t *testing.T) {
		require.Panics(t, func() {
			MustInstrumentRouterWithMonitor(&Monitor{}, &r)
		})
	})

	t.Run("can instrument on a different registerer", func(t *testing.T) {
		m := Monitor{Registerer: prometheus.NewRegistry()}
		require.NotPanics(t, func() {
			MustInstrumentRouterWithMonitor(&m, &r)
		})
	})

	t.Run("can instrument on if metrics FQN change", func(t *testing.T) {
		m := Monitor{
			ProcessedOpts:  prometheus.HistogramOpts{Namespace: "foo"},
			CheckpointOpts: prometheus.CounterOpts{Namespace: "foo"},
			AckOpts:        prometheus.CounterOpts{Namespace: "foo"},
			PublishOpts:    prometheus.HistogramOpts{Namespace: "foo"},
			PublishedOpts:  prometheus.CounterOpts{Namespace: "foo"},
			ConsumedOpts:   prometheus.CounterOpts{Namespace: "foo"},
		}
		require.NotPanics(t, func() {
			MustInstrumentRouterWithMonitor(&m, &r)
		})
	})
}

func TestProcessedMessages(t *testing.T) {
	t.Cleanup(resetRegisterer)

	reg := prometheus.NewRegistry()
	m := Monitor{
		Registerer: reg,
		ProcessedOpts: prometheus.HistogramOpts{
			Buckets:   []float64{1},
			Namespace: "custom_ns",
		},
		PublishOpts: prometheus.HistogramOpts{
			Buckets: []float64{1},
		},
		Namespace: "ns",
	}

	r := pubsub.Router{
		AckDecider: func(ctx context.Context, topic string, msg pubsub.ReceivedMessage, err error) pubsub.Acknowledgement {
			switch msg.ID() {
			case "1":
				return pubsub.Ack
			case "2":
				return pubsub.NAck
			case "3":
				return pubsub.ReSchedule
			}
			return pubsub.NoOp
		},
	}

	require.NoError(t, InstrumentRouterWithMonitor(&m, &r))

	consumerA := "consumer-a"
	consumerB := "consumer-b"

	var envPub channels.Publisher
	pub := pubsub.WrapPublisher(
		pubsub.NewPublisher(&envPub, &marshaller.ByteMarshaller{}),
		func(publisher pubsub.Publisher) pubsub.Publisher {
			return pubsub.PublisherFunc(func(ctx context.Context, topic string, envelopes ...*pubsub.Message) error {
				time.Sleep(20 * time.Millisecond)
				return publisher.Publish(ctx, topic, envelopes...)
			})
		},
		m.InstrumentPublisher,
	)

	require.NoError(t, r.Register(consumerA, envPub.Subscriber(consumerA), slowHandler))
	require.NoError(t, r.Register(consumerB, envPub.Subscriber(consumerB), slowHandler))

	ctx := context.Background()
	require.NoError(t, pub.Publish(ctx, consumerA, []*pubsub.Message{{
		ID:   "1",
		Name: "event-a",
		Data: "foo",
	}, {
		ID:   "2",
		Name: "event-a",
		Data: "bar",
	}}...))

	require.NoError(t, pub.Publish(ctx, consumerB, &pubsub.Message{
		ID:   "3",
		Name: "event-b",
		Data: "oof",
	}))

	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()

	require.NoError(t, r.Run(ctx))

	mfs, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, mfs, 6)

	resetSum := 0.0
	requireMetricFamily(t, mfs[0], "custom_ns_pubsub_message_processed", promclient.MetricType_HISTOGRAM)
	require.Len(t, mfs[0].Metric, 2)

	// verify timings and set to zero, so we can test against a string
	require.InDelta(t, 0.05, *mfs[0].Metric[0].Histogram.SampleSum, 0.005)
	require.InDelta(t, 0.025, *mfs[0].Metric[1].Histogram.SampleSum, 0.005)
	mfs[0].Metric[0].Histogram.SampleSum = &resetSum
	mfs[0].Metric[1].Histogram.SampleSum = &resetSum
	requireMetrics(t, mfs[0].Metric, expectedProcessedMetrics)

	requireMetricFamily(t, mfs[1], "ns_pubsub_message_acknowledgements", promclient.MetricType_COUNTER)
	require.Len(t, mfs[1].Metric, 3)
	requireMetrics(t, mfs[1].Metric, expectedAckMetrics)

	requireMetricFamily(t, mfs[2], "ns_pubsub_message_checkpoint", promclient.MetricType_COUNTER)
	require.Len(t, mfs[2].Metric, 8)
	requireMetrics(t, mfs[2].Metric, expectedCheckpointMetrics)

	requireMetricFamily(t, mfs[3], "ns_pubsub_message_consumed", promclient.MetricType_COUNTER)
	require.Len(t, mfs[3].Metric, 2)
	requireMetrics(t, mfs[3].Metric, expectedConsumedMetrics)

	requireMetricFamily(t, mfs[4], "ns_pubsub_message_published", promclient.MetricType_COUNTER)
	require.Len(t, mfs[4].Metric, 2)
	requireMetrics(t, mfs[4].Metric, expectedPublishedMetrics)

	requireMetricFamily(t, mfs[5], "ns_pubsub_message_publishing", promclient.MetricType_HISTOGRAM)
	require.Len(t, mfs[5].Metric, 2)
	require.InDelta(t, 0.02, *mfs[5].Metric[0].Histogram.SampleSum, 0.005)
	require.InDelta(t, 0.02, *mfs[5].Metric[1].Histogram.SampleSum, 0.005)
	mfs[5].Metric[0].Histogram.SampleSum = &resetSum
	mfs[5].Metric[1].Histogram.SampleSum = &resetSum
	requireMetrics(t, mfs[5].Metric, expectedPublishingMetrics)
}

func requireMetrics(t *testing.T, metrics []*promclient.Metric, want []string) {
	for i, m := range metrics {
		require.Equal(t, want[i], m.String())
	}
}

var expectedAckMetrics = []string{
	`label:<name:"consumer" value:"consumer-a" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-a" > label:<name:"msg_version" value:"byte:s" > label:<name:"operation" value:"ack" > counter:<value:1 > `,
	`label:<name:"consumer" value:"consumer-a" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-a" > label:<name:"msg_version" value:"byte:s" > label:<name:"operation" value:"nack" > counter:<value:1 > `,
	`label:<name:"consumer" value:"consumer-b" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-b" > label:<name:"msg_version" value:"byte:s" > label:<name:"operation" value:"re-schedule" > counter:<value:1 > `,
}

var expectedCheckpointMetrics = []string{
	`label:<name:"checkpoint" value:"ack" > label:<name:"consumer" value:"consumer-a" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-a" > label:<name:"msg_version" value:"byte:s" > counter:<value:2 > `,
	`label:<name:"checkpoint" value:"ack" > label:<name:"consumer" value:"consumer-b" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-b" > label:<name:"msg_version" value:"byte:s" > counter:<value:1 > `,
	`label:<name:"checkpoint" value:"handler" > label:<name:"consumer" value:"consumer-a" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-a" > label:<name:"msg_version" value:"byte:s" > counter:<value:2 > `,
	`label:<name:"checkpoint" value:"handler" > label:<name:"consumer" value:"consumer-b" > label:<name:"error" value:"true" > label:<name:"msg_name" value:"event-b" > label:<name:"msg_version" value:"byte:s" > counter:<value:1 > `,
	`label:<name:"checkpoint" value:"receive" > label:<name:"consumer" value:"consumer-a" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-a" > label:<name:"msg_version" value:"byte:s" > counter:<value:2 > `,
	`label:<name:"checkpoint" value:"receive" > label:<name:"consumer" value:"consumer-b" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-b" > label:<name:"msg_version" value:"byte:s" > counter:<value:1 > `,
	`label:<name:"checkpoint" value:"unmarshal" > label:<name:"consumer" value:"consumer-a" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-a" > label:<name:"msg_version" value:"byte:s" > counter:<value:2 > `,
	`label:<name:"checkpoint" value:"unmarshal" > label:<name:"consumer" value:"consumer-b" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-b" > label:<name:"msg_version" value:"byte:s" > counter:<value:1 > `,
}

var expectedProcessedMetrics = []string{
	`label:<name:"consumer" value:"consumer-a" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-a" > label:<name:"msg_version" value:"byte:s" > histogram:<sample_count:2 sample_sum:0 bucket:<cumulative_count:2 upper_bound:1 > > `,
	`label:<name:"consumer" value:"consumer-b" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-b" > label:<name:"msg_version" value:"byte:s" > histogram:<sample_count:1 sample_sum:0 bucket:<cumulative_count:1 upper_bound:1 > > `,
}

var expectedConsumedMetrics = []string{
	`label:<name:"consumer" value:"consumer-a" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-a" > label:<name:"msg_version" value:"byte:s" > counter:<value:2 > `,
	`label:<name:"consumer" value:"consumer-b" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-b" > label:<name:"msg_version" value:"byte:s" > counter:<value:1 > `,
}

var expectedPublishingMetrics = []string{
	`label:<name:"error" value:"false" > label:<name:"topic" value:"consumer-a" > histogram:<sample_count:1 sample_sum:0 bucket:<cumulative_count:1 upper_bound:1 > > `,
	`label:<name:"error" value:"false" > label:<name:"topic" value:"consumer-b" > histogram:<sample_count:1 sample_sum:0 bucket:<cumulative_count:1 upper_bound:1 > > `,
}

var expectedPublishedMetrics = []string{
	`label:<name:"topic" value:"consumer-a" > counter:<value:2 > `,
	`label:<name:"topic" value:"consumer-b" > counter:<value:1 > `,
}

func requireMetricFamily(t *testing.T, mf *promclient.MetricFamily, name string, mType promclient.MetricType) {
	require.Equal(t, name, *mf.Name)
	require.Equal(t, mType, *mf.Type)
}

func resetRegisterer() {
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
}

var slowHandler = pubsub.HandlerFunc(func(_ context.Context, msg *pubsub.Message) error {
	time.Sleep(25 * time.Millisecond)
	if msg.ID == "3" {
		return errors.New("oops")
	}
	return nil
})
