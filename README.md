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

// Initialize a router with default metrics.
router := pubsubprom.MustInstrumentRouter(pubsub.Router{})

// Add the publisher middleware for publishing metrics.
publisher := pubsubprom.InstrumentPublisher(publisher)
```

## List of provided metrics:

* `pubsub_message_processed` (`histogram`)
* `pubsub_message_acknowledgements` (`counter`)
* `pubsub_message_checkpoint` (`counter`)
* `pubsub_message_consumed` (`counter`)
* `pubsub_message_published` (`counter`)
* `pubsub_message_publishing` (`histogram`)

## Configuration

The names of the metrics can be tweaked in multiple ways:

The monitor accepts a namespace and subsystem option that will be applied to all metrics.

You can also tweak the name of a single metric and other parameters by adjusting the prometheus option struct.

## Provided Metrics

Most metrics share these common labels when available in the operation:

* `consumer`: name of the consumer.
* `msg_name`: name of the message.
* `msg_version`: version of the message.
* `error`: `true|false` if the monitored operation reported an error.

### Processed messages histogram

The amount of time it took to process a message in the consumer.

* Default name: `pubsub_message_processed`

### Checkpoint counters

For every executed checkpoint while processing a message

* Default name: `pubsub_message_checkpoint`
* Extra labels:
    * `checkpoint`: the name of the checkpoint

### Message acknowledgements counter

The message acknowledgements operations.

* Default name: `pubsub_message_acknowledgements`
* Extra labels:
    * `operation`: `ack|nack|re-schedule`

### Consumed messages

Number of messages consumed by consumer.

* Default name: `pubsub_message_consumed`

### Published messages

Number of messages published by publisher.

* Default name: `pubsub_message_published`

### Publishing operations

A histogram with the result of publishing operations.

* Default name: `pubsub_message_publishing`

[ci-badge]: https://github.com/hmoragrega/pubsub-prometheus/workflows/CI/badge.svg

[ci-url]:   https://github.com/hmoragrega/pubsub-prometheus/actions?query=workflow%3ACI

[coverage-badge]: https://coveralls.io/repos/github/hmoragrega/pubsub-prometheus/badge.svg?branch=main

[coverage-url]:   https://coveralls.io/github/hmoragrega/pubsub-prometheus?branch=main

[godoc-badge]: https://pkg.go.dev/badge/github.com/hmoragrega/pubsub-prometheus.svg

[godoc-url]:   https://pkg.go.dev/github.com/hmoragrega/pubsub-prometheus

[goreport-badge]: https://goreportcard.com/badge/github.com/hmoragrega/pubsub-prometheus

[goreport-url]: https://goreportcard.com/report/github.com/hmoragrega/pubsub-prometheus
