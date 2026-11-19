# chocolate-platform

[![ci](https://github.com/keshava19/chocolate-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/<your-user>/chocolate-platform/actions/workflows/ci.yml)

A reference implementation of an event-driven microservices platform on .NET 8, Kafka, PostgreSQL and gRPC. It focuses on distributed-systems correctness, resilience and operability, using a deliberately neutral order/inventory/notification domain. The patterns are ones I have applied in production systems, re-implemented here from scratch.

> **Status: design phase.** The design decisions are in [`docs/design.md`](docs/design.md). Nothing below is claimed until it is built and tested.

## Why this repo

<!-- Write this section before coding: what this repo demonstrates and what it deliberately does not. -->

TODO

## Architecture

<!-- Add docs/architecture.png here once the diagram exists. -->

Planned services: Gateway (YARP), Order, Inventory, Notification. See the design doc for responsibilities, data ownership and the order lifecycle.

## Failure modes and guarantees

See section 5 of the [design doc](docs/design.md). This table moves here once each row is demonstrated by a test.

## Quickstart

TODO: `docker compose up` from `deploy/compose`, once the stack exists.

## Operating this system

TODO: dashboards, key metrics, alert thresholds, how to debug a stuck consumer.

## Scaling

TODO: measured k6 results (target scenario: 100 orders per second for 5 minutes), bottleneck analysis, autoscaling notes.

## Repository layout

```
src/            services and shared building blocks
contracts/      .proto files for gRPC and events
tests/          unit, architecture, integration, failure scenarios
deploy/         Docker Compose and Helm chart
observability/  Grafana dashboards and Prometheus rules
loadtest/       k6 scripts and recorded results
docs/           design doc, ADRs, diagrams
```

## Roadmap

- [ ] Order service: domain, state machine, CQRS, idempotency key
- [ ] Outbox, Inventory consumer with inbox, kill-the-consumer test
- [ ] gRPC availability check with resilience policies
- [ ] Notification service, gateway, schema compatibility check
- [ ] Observability, Compose, Helm, k6, chaos scenarios
- [ ] Architecture diagram and ADRs

## License

MIT
