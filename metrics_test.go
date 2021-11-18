package pubsub_prometheus

import (
	"context"
	"errors"
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

	require.NoError(t, Instrument(&pubsub.Router{}))
}

func TestInstrumentMultipleRouters(t *testing.T) {
	t.Cleanup(resetRegisterer)

	t.Run("zero value router can be instrumented", func(t *testing.T) {
		var m Monitor
		require.NotPanics(t, func() {
			MustInstrumentWithMonitor(&m, &pubsub.Router{})
		})

		t.Run("the same monitor can instrument more routers", func(t *testing.T) {
			require.NotPanics(t, func() {
				MustInstrumentWithMonitor(&m, &pubsub.Router{})
			})
		})
	})
}

func TestInstrumentMultipleMonitor(t *testing.T) {
	t.Cleanup(resetRegisterer)

	var r pubsub.Router
	MustInstrument(&r)

	t.Run("panics if the registerer is not different", func(t *testing.T) {
		require.Panics(t, func() {
			MustInstrumentWithMonitor(&Monitor{}, &r)
		})
	})

	t.Run("can instrument on a different registerer", func(t *testing.T) {
		m := Monitor{Registerer: prometheus.NewRegistry()}
		require.NotPanics(t, func() {
			MustInstrumentWithMonitor(&m, &r)
		})
	})

	t.Run("can instrument on if metrics name change", func(t *testing.T) {
		m := Monitor{
			ProcessedOpts:  prometheus.HistogramOpts{Name: "router_processed_messages"},
			CheckpointOpts: prometheus.CounterOpts{Name: "router_checkpoint_counter"},
			AckOpts:        prometheus.CounterOpts{Name: "router_ack_counter"},
		}
		require.NotPanics(t, func() {
			MustInstrumentWithMonitor(&m, &r)
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

	require.NoError(t, InstrumentWithMonitor(&m, &r))

	consumerA := "consumer-a"
	consumerB := "consumer-b"

	var pub channels.Publisher

	require.NoError(t, r.Register(consumerA, pub.Subscriber(consumerA), slowHandler))
	require.NoError(t, r.Register(consumerB, pub.Subscriber(consumerB), slowHandler))

	ctx := context.Background()
	require.NoError(t, pub.Publish(ctx, consumerA, []*pubsub.Envelope{{
		ID:      "1",
		Name:    "event-a",
		Version: "v1",
	}, {
		ID:      "2",
		Name:    "event-a",
		Version: "v1",
	}}...))

	require.NoError(t, pub.Publish(ctx, consumerB, &pubsub.Envelope{
		ID:      "3",
		Name:    "event-b",
		Version: "v2",
	}))

	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()

	require.NoError(t, r.Run(ctx))

	mfs, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, mfs, 3)

	resetSum := 0.0
	requireMetricFamily(t, mfs[0], "custom_ns_pubsub_message_processed", promclient.MetricType_HISTOGRAM)
	require.Len(t, mfs[0].Metric, 2)
	require.InDelta(t, 0.05, *mfs[0].Metric[0].Histogram.SampleSum, 0.005)
	require.InDelta(t, 0.025, *mfs[0].Metric[1].Histogram.SampleSum, 0.005)

	// reset sum timings so we can test against a string
	mfs[0].Metric[0].Histogram.SampleSum = &resetSum
	mfs[0].Metric[1].Histogram.SampleSum = &resetSum

	requireMetric(t, mfs[0].Metric[0], expectedProcessedMetrics[0])
	requireMetric(t, mfs[0].Metric[1], expectedProcessedMetrics[1])

	requireMetricFamily(t, mfs[1], "ns_pubsub_message_acknowledgements", promclient.MetricType_COUNTER)
	require.Len(t, mfs[1].Metric, 3)
	requireMetric(t, mfs[1].Metric[0], expectedAckMetrics[0])
	requireMetric(t, mfs[1].Metric[1], expectedAckMetrics[1])
	requireMetric(t, mfs[1].Metric[2], expectedAckMetrics[2])

	requireMetricFamily(t, mfs[2], "ns_pubsub_message_checkpoint", promclient.MetricType_COUNTER)
	require.Len(t, mfs[2].Metric, 8)
	requireMetric(t, mfs[2].Metric[0], expectedCheckpointMetrics[0])
	requireMetric(t, mfs[2].Metric[1], expectedCheckpointMetrics[1])
	requireMetric(t, mfs[2].Metric[2], expectedCheckpointMetrics[2])
	requireMetric(t, mfs[2].Metric[3], expectedCheckpointMetrics[3])
	requireMetric(t, mfs[2].Metric[4], expectedCheckpointMetrics[4])
	requireMetric(t, mfs[2].Metric[5], expectedCheckpointMetrics[5])
	requireMetric(t, mfs[2].Metric[6], expectedCheckpointMetrics[6])
	requireMetric(t, mfs[2].Metric[7], expectedCheckpointMetrics[7])
}

var expectedAckMetrics = []string{
	`label:<name:"consumer" value:"consumer-a" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-a" > label:<name:"msg_version" value:"v1" > label:<name:"operation" value:"ack" > counter:<value:1 > `,
	`label:<name:"consumer" value:"consumer-a" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-a" > label:<name:"msg_version" value:"v1" > label:<name:"operation" value:"nack" > counter:<value:1 > `,
	`label:<name:"consumer" value:"consumer-b" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-b" > label:<name:"msg_version" value:"v2" > label:<name:"operation" value:"re-schedule" > counter:<value:1 > `,
}

var expectedCheckpointMetrics = []string{
	`label:<name:"checkpoint" value:"ack" > label:<name:"consumer" value:"consumer-a" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-a" > label:<name:"msg_version" value:"v1" > counter:<value:2 > `,
	`label:<name:"checkpoint" value:"ack" > label:<name:"consumer" value:"consumer-b" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-b" > label:<name:"msg_version" value:"v2" > counter:<value:1 > `,
	`label:<name:"checkpoint" value:"handler" > label:<name:"consumer" value:"consumer-a" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-a" > label:<name:"msg_version" value:"v1" > counter:<value:2 > `,
	`label:<name:"checkpoint" value:"handler" > label:<name:"consumer" value:"consumer-b" > label:<name:"error" value:"true" > label:<name:"msg_name" value:"event-b" > label:<name:"msg_version" value:"v2" > counter:<value:1 > `,
	`label:<name:"checkpoint" value:"receive" > label:<name:"consumer" value:"consumer-a" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-a" > label:<name:"msg_version" value:"v1" > counter:<value:2 > `,
	`label:<name:"checkpoint" value:"receive" > label:<name:"consumer" value:"consumer-b" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-b" > label:<name:"msg_version" value:"v2" > counter:<value:1 > `,
	`label:<name:"checkpoint" value:"unmarshal" > label:<name:"consumer" value:"consumer-a" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-a" > label:<name:"msg_version" value:"v1" > counter:<value:2 > `,
	`label:<name:"checkpoint" value:"unmarshal" > label:<name:"consumer" value:"consumer-b" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-b" > label:<name:"msg_version" value:"v2" > counter:<value:1 > `,
}

var expectedProcessedMetrics = []string{
	`label:<name:"consumer" value:"consumer-a" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-a" > label:<name:"msg_version" value:"v1" > histogram:<sample_count:2 sample_sum:0 bucket:<cumulative_count:2 upper_bound:1 > > `,
	`label:<name:"consumer" value:"consumer-b" > label:<name:"error" value:"false" > label:<name:"msg_name" value:"event-b" > label:<name:"msg_version" value:"v2" > histogram:<sample_count:1 sample_sum:0 bucket:<cumulative_count:1 upper_bound:1 > > `,
}

func requireMetricFamily(t *testing.T, mf *promclient.MetricFamily, name string, mType promclient.MetricType) {
	require.Equal(t, name, *mf.Name)
	require.Equal(t, mType, *mf.Type)
}

func requireMetric(t *testing.T, m *promclient.Metric, want string) {
	require.Equal(t, want, m.String())
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
