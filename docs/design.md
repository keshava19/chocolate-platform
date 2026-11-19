# Design Decisions: Event-Driven Order Platform

Working design doc. Each decision here becomes a short ADR in `docs/adr/` once confirmed.

## 1. Purpose and positioning

**Goal:** a flagship repo that shows backend/platform depth: distributed-systems correctness, messaging, resilience, and operability. It is not a feature-rich product.

**Demo domain:** a generic order flow (place order, reserve stock, notify customer). It is deliberately neutral, with no code, schemas, or naming taken from client or employer systems. The README frames it as "patterns applied in production, re-implemented on a neutral domain".

**Success criteria (what a reviewer should be able to verify in 10 minutes):**

- `docker compose up` brings up the whole system from a clean clone.
- A scripted failure scenario (kill a consumer mid-message) proves no duplicate side effects.
- Dashboards show consumer lag, p95 latency, and error rate under load.
- Measured k6 numbers sit in the README next to the claims they back.

## 2. Scope

**In scope:** three services, a gateway, Kafka, PostgreSQL, gRPC, observability, Helm chart, CI, load and chaos tests.

**Non-goals (stated in the README so nobody wonders):**

- Payments, real authentication, and a UI. The gateway validates a stub JWT only.
- Event sourcing. It is a different pattern with different costs (see D4).
- Multi-region, exactly-once Kafka transactions across services, and a service mesh.
- Terraform and cloud provisioning, for now. Local environments run on Compose and `kind`.

## 3. System overview

| Component | Responsibility | Talks to |
|---|---|---|
| **Gateway (YARP)** | Routing, rate limiting, JWT stub, correlation IDs | Order (REST) |
| **Order service** | Owns orders, order state machine, publishes order events | Inventory (gRPC, sync), Kafka (async) |
| **Inventory service** | Owns stock levels and reservations, answers availability queries | Kafka, Postgres |
| **Notification service** | Pure consumer; sends (simulated) notifications | Kafka |

**Order lifecycle:** `Pending -> Confirmed | Rejected`, plus `Cancelled` by the customer while Pending.

**Happy path:**

1. Client calls `POST /orders` with an `Idempotency-Key` header.
2. Order calls Inventory over gRPC for an advisory availability check, within a deadline.
3. Order saves the order as `Pending` and writes `OrderPlaced` to its outbox, in one transaction.
4. The outbox publisher sends `OrderPlaced` to Kafka.
5. Inventory consumes it, reserves stock, and writes `StockReserved` or `StockRejected` to its own outbox.
6. Order consumes the result and moves to `Confirmed` or `Rejected`, emitting `OrderConfirmed` or `OrderRejected`.
7. Notification consumes the final event and notifies the customer.

## 4. Decisions

Each decision lists the choice, the alternatives considered, the rationale, and the cost accepted.

### D1. Architecture style: Clean Architecture with DDD tactical patterns, per service

- **Choice:** four projects per service: `Domain`, `Application`, `Infrastructure`, `Api`. Dependencies point inward only, enforced by architecture tests (NetArchTest).
- **Alternatives:** a simple layered N-tier, or a vertical-slice structure.
- **Why:** it is the pattern the target roles expect to see, and domain logic (order state machine, reservation rules) is testable without infrastructure.
- **Cost:** more projects and ceremony than the problem strictly needs. Mitigation: keep Domain small and real. Only Order and Inventory get rich aggregates. Notification stays thin and is documented as intentionally simple.

### D2. Stack: .NET 8, PostgreSQL, Kafka, Protobuf

- **Choice:** ASP.NET Core, MediatR, EF Core (writes), Dapper (reads), Confluent.Kafka, Grpc.AspNetCore, YARP, Serilog, OpenTelemetry.
- **Why:** it matches the author's production background, so every line can be defended in an interview. One stack per repo, with no menu of options.
- **Cost:** none significant. Revisit MediatR only if its licensing or overhead becomes a concern, and keep handlers plain so swapping it out is cheap.

### D3. Service boundaries and data ownership: database per service

- **Choice:** each service has its own PostgreSQL database (separate instances in Compose, separate schemas at minimum). No service reads another's tables. All cross-service data moves through gRPC or events.
- **Why:** this is what makes the services independently deployable, and it forces the consistency problems the repo exists to solve.
- **Cost:** no cross-service joins and no distributed transactions. That is the point, and the README should say so.

### D4. CQRS without event sourcing

- **Choice:** commands and queries are separate handlers in the Application layer. Writes go through the domain model and EF Core. Reads use thin Dapper queries returning DTOs. Same database, no separate read store. Decided: plain Dapper for reads, with no projected read model for now.
- **Alternatives:** full event sourcing, or a separate read model fed by projections.
- **Why:** CQRS is justified here by clean write invariants and cheap reads. Event sourcing adds replay, snapshotting, and versioning complexity that would swamp the messaging story.
- **Cost:** no built-in audit history. The outbox table and Kafka retention provide a partial trail. Say so honestly.

### D5. Workflow coordination: choreography, not orchestration

- **Choice:** services react to each other's events. There is no central saga orchestrator.
- **Alternatives:** an orchestrator (MassTransit saga, Temporal, or a hand-built process manager).
- **Why:** with one workflow and three participants, choreography is simple and easy to trace.
- **Cost:** the end-to-end flow is implicit and harder to see at a glance. Mitigations: a documented sequence diagram, the `OrderId` correlation ID on every event, and traces that follow the whole flow. The ADR should state the threshold where you would switch (roughly more than four participants or branching compensation logic).

### D6. Reliable publishing: transactional outbox with a polling publisher

- **Choice:** the business change and an `outbox_messages` row are written in one local transaction. A background publisher polls unpublished rows using `SELECT ... FOR UPDATE SKIP LOCKED`, publishes to Kafka, then marks them published. Delivery is **at-least-once**, with ordering preserved per aggregate.
- **Alternatives:** CDC with Debezium (lower latency, no polling, but another moving part), or dual-write (rejected: it loses events on a crash between the DB commit and the Kafka send).
- **Why:** polling is simple, needs no extra infrastructure, and is easy to demonstrate under failure.
- **Cost:** added publish latency (target polling interval around 100 ms) and extra DB load. Documented as a known trade-off, with Debezium noted as the scale-up path.

### D7. Reliable consuming: idempotent consumer with inbox table

- **Choice:**
  - Every event carries a unique `EventId` (UUID), set once at creation time and stored in the outbox.
  - Consumers disable auto-commit. Inside one DB transaction they insert `(ConsumerName, EventId)` into `processed_messages` (unique constraint) and apply the business effect.
  - They commit the Kafka offset only after the DB commit.
  - A duplicate hits the unique constraint and is skipped, with the offset still advancing.
- **Why:** outbox plus idempotent consumer gives *exactly-once effects* on top of at-least-once delivery, without relying on Kafka transactions across services. This is the core correctness claim of the repo.
- **Cost:** one extra insert per message, and the table needs retention cleanup (a scheduled purge older than the Kafka retention window plus a margin).
- **Proof:** an integration test using Testcontainers kills the consumer after the DB commit but before the offset commit, restarts it, and asserts a single stock reservation.

### D8. Topics, keys, and ordering

- **Topics:** `orders.events.v1`, `inventory.events.v1`, plus a dead-letter topic per consumer group (`<topic>.<group>.dlt`).
- **Partition key:** `OrderId` on both topics, which guarantees ordering per order.
- **Partitions:** 6 per topic in the demo. This also sets the ceiling for consumer scale-out per group.
- **Replication:** factor 3 in the Helm values, factor 1 in Compose, documented.
- **Why not key by SKU?** Cross-order stock contention is handled in the database (see D9), so ordering by SKU is not needed.

### D9. Inventory concurrency: reservations guarded in the database

- **Choice:** reservation happens in a single transaction using an atomic conditional update (`UPDATE stock SET available = available - @qty WHERE sku = @sku AND available >= @qty`) or optimistic concurrency with a row version. Overselling is impossible regardless of consumer count.
- **Why:** the correctness guarantee lives in the database, not in message ordering. This makes scale-out of Inventory safe.
- **Cost:** hot SKUs serialize on a row. Show this in the load test as a bottleneck and describe the mitigation (stock sharding or a reservation queue) rather than building it.

### D10. Event contracts: Protobuf with Schema Registry, backward-compatible evolution

- **Choice:** event schemas are Protobuf, registered in Confluent Schema Registry with BACKWARD compatibility enforced. Events carry an envelope: `EventId`, `EventType`, `OccurredAt`, `CorrelationId`, `SchemaVersion`.
- **Alternatives:** JSON (easy but unversioned), Avro (equally valid; Protobuf is chosen for tooling reuse with gRPC).
- **Why:** schema evolution is a classic senior-level question. One serialization family for both gRPC and Kafka keeps the repo coherent.
- **Cost:** extra infrastructure and a CI step that checks schema compatibility. Worth it.
- **Demo:** an ADR plus a commit that adds an optional field and shows old consumers still working.

### D11. Synchronous path: gRPC for a single, advisory call

- **Choice:** Gateway to Order is REST. Order to Inventory uses gRPC `CheckAvailability`, with a 300 to 500 ms deadline, retry with jitter, and a circuit breaker (Polly via `Microsoft.Extensions.Http.Resilience`).
- **What it means:** the check is *advisory* and does not reserve stock. The authoritative decision is still the async reservation. This keeps the sync path from becoming a hidden distributed transaction.
- **When Inventory is down (circuit open):** decided: graceful degradation. The order is accepted as `Pending` and the saga decides. The check is advisory, so an Inventory outage should not block order intake. The cost is that a customer may see a later rejection, and the README says so. Failing with a 503 is documented as the alternative that was not chosen.
- **Honesty note:** do not invent extra hops just to claim a deep call chain. If you want to demonstrate a longer synchronous chain, add it deliberately in a separate ADR with its latency and failure-amplification analysis.

### D12. API idempotency

- **Choice:** `POST /orders` requires an `Idempotency-Key`. Order stores the key with the resulting order ID under a unique constraint. A retry with the same key returns the original response.
- **Why:** clients retry. Without this the exactly-once story ends at the edge.

### D13. Failure handling in consumers: bounded retry, then dead-letter

- **Choice:** transient errors (DB timeout, deadlock) retry in-process with exponential backoff and jitter, capped at a small number of attempts. Poison messages (deserialization failure, validation failure) go straight to the DLT with failure metadata in headers. A small admin endpoint or script lists and replays DLT messages.
- **Alternatives:** retry topics (non-blocking retries, more complexity). Mentioned as a scale-up path, since in-process retry blocks the partition.
- **Why:** blocking a partition on a transient error is acceptable at this scale, and it preserves ordering.

### D14. Observability: traces, metrics, logs, correlated

- **Traces:** OpenTelemetry with W3C `traceparent` propagated in Kafka headers, so one trace spans HTTP, gRPC, outbox, and consumers. Exported to Jaeger.
- **Metrics:** Prometheus. Custom metrics: outbox backlog size and age, consumer processing duration, DLT count, circuit breaker state. Consumer lag via a Kafka exporter.
- **Dashboards:** one Grafana dashboard in the repo, provisioned as code: request rate and p95 latency, consumer lag, outbox age, error rate, DLT count.
- **Logs:** structured (Serilog) with `CorrelationId`, `OrderId`, and `EventId` on every line.
- **Alert thresholds** are listed in the README even if only as Prometheus rule examples.

### D15. Runtime: Compose for local, Helm for Kubernetes

- **Compose:** one command runs services, Postgres instances, a single Kafka broker (KRaft mode), Schema Registry, and the observability stack. Decided: single broker, to keep the local stack light. Broker restarts are demonstrated; replication and leader failover are not.
- **Helm chart:** Deployments, Services, ConfigMaps/Secrets, readiness and liveness probes, resource requests/limits, HPA, and PodDisruptionBudgets. Tested on `kind` in CI or by script.
- **Scaling rules:** HPA on CPU as the baseline. Consumer replicas must not exceed partition count. KEDA on consumer lag is documented as the better trigger and kept as a stretch goal.
- **Terraform:** skipped for now. Environments are Compose locally and `kind` for Kubernetes, and nothing cloud-provisioned is claimed. Revisit later if time allows.

### D16. Testing strategy

| Level | What | Tooling |
|---|---|---|
| Unit | Domain invariants, order state machine, reservation rules | xUnit, FluentAssertions |
| Architecture | Layer dependency rules | NetArchTest |
| Integration | Outbox, idempotent consumer, DB concurrency | Testcontainers (Postgres, Kafka) |
| Contract | Schema compatibility on every PR | Schema Registry check in CI |
| Failure | Kill consumer mid-message, broker restart, DB restart | Scripted scenarios, results documented |
| Load | Order placement throughput and latency at 100 orders per second for 5 minutes | k6, results committed to the README |

### D17. CI/CD

GitHub Actions: build, test (unit plus Testcontainers integration), lint/format, schema compatibility check, Docker image build, push to GHCR, and a `kind` smoke test of the Helm chart on main. No secrets in the repo, only `.env.example`.

## 5. Failure modes and guarantees

| Failure | What happens | Why the system stays correct |
|---|---|---|
| Order service crashes after DB commit, before Kafka publish | Event is still in the outbox | Publisher resumes and sends it on restart |
| Publisher sends, then crashes before marking published | Event is published twice | Consumers dedupe by `EventId` |
| Inventory consumer crashes after DB commit, before offset commit | Message redelivered | Processed-message unique constraint skips the duplicate |
| Two Inventory replicas reserve the same SKU concurrently | One update wins | Conditional update or row version prevents overselling |
| Kafka broker unavailable | Publishing stalls | Orders still commit locally. Outbox backlog grows, and the alert fires on its age |
| Inventory down during `POST /orders` | gRPC calls fail | Circuit opens. Order is accepted as Pending (graceful degradation) |
| Client retries `POST /orders` | Second request arrives | Idempotency key returns the original result |
| Malformed or incompatible event | Deserialization fails | Goes to the DLT and the partition keeps moving. Schema checks in CI make this rare |
| Consumer slower than producer | Lag grows | Lag alert, then scale consumers up to the partition count. Beyond that, repartition |
| Duplicate or out-of-order events for one order | Possible if keying is wrong | Same key, same partition, so ordering holds. State machine rejects invalid transitions |

## 6. Repository layout

```
chocolate-platform/
├── src/
│   ├── Gateway/
│   ├── Order/{Domain,Application,Infrastructure,Api}/
│   ├── Inventory/{Domain,Application,Infrastructure,Api}/
│   ├── Notification/
│   └── BuildingBlocks/        # outbox, inbox, envelope, telemetry
├── contracts/                 # .proto files for gRPC and events
├── tests/{Unit,Architecture,Integration,Failure}/
├── deploy/{compose,helm}/
├── observability/             # Grafana dashboards, Prometheus rules
├── loadtest/                  # k6 scripts and recorded results
├── docs/{architecture.png,sequence.md,adr/}
├── .github/workflows/
└── README.md
```

## 7. ADR list to write

1. Clean Architecture and CQRS without event sourcing (D1, D4)
2. Choreography over orchestration, and when to switch (D5)
3. Transactional outbox with polling publisher (D6)
4. Idempotent consumer and exactly-once effects (D7)
5. Protobuf schemas and compatibility policy (D10)
6. Advisory gRPC check and degradation behavior (D11)

## 8. Resolved decisions

| Question | Decision | Effect on the design |
| --- | --- | --- |
| Degradation default (D11) | Graceful degradation: accept the order as Pending | An Inventory outage does not block order intake. A later rejection is possible and documented |
| Kafka in Compose | Single broker | Light local stack. Broker restart is demonstrated, replication failover is not |
| Read models (D4) | Plain Dapper | No projected read model for now |
| Terraform (D15) | Skipped for now | Compose plus `kind` only. The layout has no `terraform/` folder |
| k6 target (D16) | 100 orders per second for 5 minutes | Steady-rate scenario, sized to show behavior and the first bottleneck rather than a headline figure. Measured results go in the README as they come out |

## 9. Build order that follows from these decisions

1. Order service: domain model, state machine, CQRS handlers, EF Core, idempotency key.
2. Outbox plus publisher, and the Inventory consumer with the inbox. Write the kill-the-consumer test as soon as this works.
3. Inventory reservation concurrency test, gRPC availability check, Polly policies.
4. Notification service, gateway, schema registry and compatibility CI check.
5. Observability, Compose polish, Helm, k6, chaos scenarios, diagram, README.
