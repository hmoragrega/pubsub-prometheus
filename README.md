# Pubsub Prometheus

Prometheus plugin for pubsub

[![godoc][godoc-badge]][godoc-url]
[![ci][ci-badge]][ci-url]
[![coverage][coverage-badge]][coverage-url]
[![goreport][goreport-badge]][goreport-url]

## TLDR;

```go
import (
    "github.com/hmoragrega/pubsub"
    pubsubprom "github.com/hmoragrega/pubsub-prometheus"
)

// Initialize a router will default metrics.
router := pubsubprom.MustInstrument(pubsub.Router{})
```

## Configuration

Metrics can be tweaked using a custom monitor
```go
import (
    "github.com/hmoragrega/pubsub"
    pubsubprom "github.com/hmoragrega/pubsub-prometheus"
)

var router pubsub.Router
err := pubsubprom.InstrumentWithMonitor(&pubsubprom.Monitor{
    // tweak options
}, &router)
```

## Provided Metrics

### Common labels:

All metrics come with these labels

* `consumer`: name of the consumer.
* `msg_name`: name of the message (if any).
* `msg_version`: version of the message.
* `error`: `true|false` if the monitored operation reported an error.

### Processed message histogram

The amount of time it took to process a message in the consumer.

* Name: `pubsub_message_processed`

### Checkpoint counters

For every executed checkpoint while processing a message

* Name: `pubsub_message_checkpoint`
* Extra Labels:
    * `checkpoint`: the name of the checkpoint

### Acknowledgements counter

The acknowledgements operations.

* Name: `pubsub_message_acknowledgements`
* Extra Labels:
    * `operation`: `ack|nack|re-schedule`

## Customize Metrics

Metrics can be customized using the exported options, for example changing the metric name or adding the namespace and
subsystem information.


[ci-badge]: https://github.com/hmoragrega/pubsub-prometheus/workflows/CI/badge.svg
[ci-url]:   https://github.com/hmoragrega/pubsub-prometheus/actions?query=workflow%3ACI

[coverage-badge]: https://coveralls.io/repos/github/hmoragrega/pubsub-prometheus/badge.svg?branch=main
[coverage-url]:   https://coveralls.io/github/hmoragrega/pubsub-prometheus?branch=main

[godoc-badge]: https://pkg.go.dev/badge/github.com/hmoragrega/pubsub-prometheus.svg
[godoc-url]:   https://pkg.go.dev/github.com/hmoragrega/pubsub-prometheus

[goreport-badge]: https://goreportcard.com/badge/github.com/hmoragrega/pubsub-prometheus
[goreport-url]: https://goreportcard.com/report/github.com/hmoragrega/pubsub-prometheus
