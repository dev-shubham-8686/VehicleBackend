# CVP Architecture

A simplified Connected Vehicle Platform, modeled on the [AWS Connected Vehicle guidance](https://aws.amazon.com/solutions/guidance/connected-vehicles-on-aws/).

## Services

| Service | Kind | Responsibility |
|---|---|---|
| `DeviceGateway` | Web API | MQTT *client* of the standalone VerneMQ broker; bridges inbound telemetry/command-acks onto Kafka |
| `VehicleSimulator` | Worker | Concurrent virtual fleet publishing synthetic telemetry over MQTT, for load/integration testing |
| `TelemetryProcessor` | Worker | Consumes raw telemetry from Kafka, persists to TimescaleDB, evaluates alert rules |
| `VehicleShadow` | Web API | Redis-backed reported/desired state per vehicle (the "digital twin") |
| `CommandService` | Web API | Issues remote commands, tracks delivery/ack status in Postgres |
| `AlertingService` | Web API | Rule-triggered alerts; SignalR hub pushes them to connected dashboards |
| `OtaService` | Web API | Update-campaign management: targeting, staged rollout, rollback |
| `FleetApi` | Web API | Backend-for-frontend aggregating Shadow/Command/Alerting for dashboard & mobile clients |
| `IdentityService` | Web API | Device identity: issues/validates per-vehicle MQTT credentials, answers VerneMQ's webhook-auth calls; OAuth2/OIDC for human users is still unbuilt |

## Shared libraries

- `Cvp.Contracts` — message/event DTOs shared across services (telemetry, commands, shadow state, alerts) and Kafka topic name constants.
- `Cvp.Common` — cross-cutting service wiring: Serilog + OpenTelemetry setup (`AddCvpServiceDefaults`), health checks, a thin Kafka producer/consumer wrapper, and `StartupRetry` (retries a startup dependency check with backoff instead of crashing — needed because Compose starts containers concurrently, so e.g. Postgres may still be starting when a service's first query fires).

## MQTT broker: VerneMQ (not embedded)

The vehicle-facing MQTT broker is a standalone [VerneMQ](https://vernemq.com/) container (`deploy/docker-compose.yml`), not code running inside DeviceGateway — matching how real OEM platforms separate "a broker built to hold millions of persistent connections" from "the service that does something with the messages." DeviceGateway connects to it as an ordinary MQTT *client* (`MqttBridgeHostedService`), subscribed to `vehicles/+/telemetry` and `vehicles/+/commands/ack`, and bridges whatever it receives onto Kafka.

**Reconnect is a manual poll loop, not MQTTnet's managed client.** No release of `MQTTnet.Extensions.ManagedClient` targets MQTTnet 5.x yet (it tops out at 4.3.7, incompatible with the 5.2 core package used here) — `MqttBridgeHostedService` polls `IsConnected` every 5s and reconnects/resubscribes itself instead.

## Device identity & VerneMQ auth

`DOCKER_VERNEMQ_ALLOW_ANONYMOUS` is `off`. VerneMQ's [`vmq_webhooks`](https://docs.vernemq.com/plugin-development/webhookplugins) plugin calls out to **IdentityService** over HTTP on every connect, publish, and subscribe — policy lives in C#, VerneMQ stays a dumb broker that just enforces whatever IdentityService decides. This was the deliberate choice over `vmq_diversity` (Lua + a DB VerneMQ queries directly): keeping authorization logic in the same language and test suite as everything else, rather than splitting it across SQL/Lua and C#.

- **Device identity, not full PKI.** Each vehicle calls `POST /devices/register {vehicleId}` once at startup (`VehicleSimulator.RegisterDeviceAsync`) and gets back a random 256-bit secret (idempotent — re-registering returns the same one), stored in IdentityService's `devices` table. The vehicle then connects to VerneMQ with `username=vehicleId, password=deviceSecret`. This is the same shape as AWS IoT's custom-auth or Azure IoT Hub's SAS tokens without going as far as mutual-TLS device certs — a deliberately smaller scope than a real CA/cert-rotation setup, which would be the next step if this went further (`IdentityService.DeviceRepository`, `CryptographicOperations.FixedTimeEquals` for the comparison).
- **DeviceGateway authenticates as a service account**, not a device: a fixed `username=device-gateway` + shared secret (`Mqtt:Username`/`Mqtt:SharedSecret` on DeviceGateway, `Gateway:Username`/`Gateway:SharedSecret` on IdentityService — same literal value in both, set via `deploy/docker-compose.yml`).
- **Per-vehicle topic isolation (`auth_on_publish`/`auth_on_subscribe`).** A vehicle client (`vehicle-{id}`) can only publish to its own `vehicles/{id}/telemetry` and `vehicles/{id}/commands/ack`, and only subscribe to its own `vehicles/{id}/commands` — not the wildcards DeviceGateway uses. This is the actual security property that matters here: without it, connect-time auth alone would still let any authenticated vehicle snoop on or spoof *any other* vehicle's topics. DeviceGateway (`device-gateway-*`) is allowed the wildcards since it's a trusted internal service, not something a compromised vehicle could impersonate (different client-id prefix, different credential).
- **MQTT5 vs MQTT3.1.1 hook variants.** MQTTnet 5.x clients negotiate MQTT5 by default, so VerneMQ invokes `auth_on_register_m5` / `auth_on_publish_m5` / `auth_on_subscribe_m5`, not the plain names — missing this is a real way to silently break auth (every connection fails with `plugin_chain_exhausted` and no hint about *why*, since nothing errors, it's just that no plugin in the hook chain ever returns `ok`). Both the `_m5` and plain hook names are registered in `docker-compose.yml`, pointing at the same endpoints; the request payloads differ only in a few fields (`clean_start` vs `clean_session`, an extra `properties` object) that `IdentityService`'s webhook DTOs don't read, so one handler serves both.
- **Plaintext, not TLS.** Credentials and all MQTT traffic travel unencrypted between containers. Fine on an isolated Docker network for local dev; real TLS termination on VerneMQ plus a trusted cert chain is Phase 7 hardening, not done here.
- **No redelivery/rotation.** A revoked or rotated device secret doesn't disconnect an already-connected session, and there's no admin endpoint to revoke one yet.

## Data flow (steady state)

```
Vehicle --MQTT--> VerneMQ <--MQTT(client)-- DeviceGateway --Kafka(cvp.telemetry.raw)--> TelemetryProcessor --> TimescaleDB
                                                                                                |
                                                                                                +--Kafka(cvp.alerts)--> AlertingService --SignalR--> Dashboard

FleetApi/CommandService --Kafka(cvp.commands.dispatch)--> DeviceGateway --MQTT--> VerneMQ --MQTT--> Vehicle
Vehicle --MQTT(ack)--> VerneMQ --MQTT(client)--> DeviceGateway --Kafka(cvp.commands.ack)--> CommandService
```

Both legs are now wired up end-to-end: `CommandDispatchConsumer` consumes `cvp.commands.dispatch` and hands each command to `MqttBridgeHostedService.PublishCommandAsync`, which publishes it to `vehicles/{id}/commands`. VehicleSimulator subscribes to its own `vehicles/{id}/commands` topic and auto-acks (`FleetSimulatorWorker.HandleCommandAsync`), standing in for real vehicle firmware. Verified live: `POST /vehicles/{id}/commands` went `Queued` → `Acknowledged` in ~160ms.

`MqttBridgeHostedService` is registered as both a plain singleton and the hosted service (see `DeviceGateway/Program.cs`) so `CommandDispatchConsumer` can share the one connected MQTT client rather than opening a second connection. Delivery here is best-effort, not at-least-once: if the bridge isn't currently connected to VerneMQ when a command arrives from Kafka, it's logged and dropped — CommandService's Postgres record stays `Queued` either way, so nothing is silently lost from the system's point of view, but there's no redelivery-on-reconnect yet. Worth revisiting once a vehicle can plausibly be offline for a while (that's realistic — cars lose signal) rather than just briefly reconnecting, as it does today.

## Observability

Every service already emits traces/metrics via OpenTelemetry (`AddCvpServiceDefaults`), but until now nothing collected them — and the auto-instrumentation only covered inbound HTTP requests, leaving the thing that actually matters here (Kafka produce/consume, which every worker service lives and dies by) completely invisible. Two changes close that gap:

- **Manual Kafka instrumentation** (`Cvp.Common.Observability.CvpTelemetry`) — `KafkaProducer.PublishAsync` and `KafkaConsumerBackgroundService<T>` each wrap their work in an `Activity` (`{topic} publish` / `{topic} consume`, tagged with `messaging.system`/`messaging.destination.name`/`messaging.kafka.message.key`) and record `cvp.kafka.messages_produced`, `cvp.kafka.messages_consumed`, and `cvp.kafka.consume_duration` on a shared `Meter`. Confluent.Kafka has no official OTel instrumentation, so this had to be written by hand — there was nothing to just "turn on."
- **Npgsql's built-in tracing** (`Npgsql.OpenTelemetry`, `.AddNpgsql()`/`.AddNpgsqlInstrumentation()`) is wired up too, for free. The payoff: a Kafka-consume span now correctly parents the Postgres-insert span it triggers, so e.g. TelemetryProcessor's "received a telemetry message" → "wrote it to TimescaleDB" shows up as one trace, not two disconnected ones. Verified live in Jaeger.

**Infra** (`deploy/docker-compose.yml`, `deploy/observability/`): every service points `Otel:Endpoint` at an **OpenTelemetry Collector**, which fans out to **Jaeger** (traces) and a **Prometheus**-scrapeable endpoint (metrics); **Grafana** sits on top with both pre-provisioned as datasources plus one dashboard (`cvp-overview.json`) with real, verified PromQL — Kafka produce/consume rate by topic, p95 consume duration, p95 HTTP request duration by service. Exact metric names (`cvp_kafka_messages_produced_total`, `cvp_kafka_consume_duration_milliseconds_bucket`, `http_server_request_duration_seconds_bucket`) were confirmed against the live collector output before being baked into the dashboard JSON, not guessed.

- Jaeger UI: http://localhost:16686
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (anonymous admin access, dev-only — `GF_AUTH_ANONYMOUS_ENABLED`)

### Cross-process trace propagation — a full request traced end to end

A single trace now follows one message across every hop, across process boundaries, across two transports (MQTT and Kafka) and HTTP. The mechanism (`CvpTelemetry.TraceParentPropertyName`, `"traceparent"`): the W3C trace-context string is `Activity.Id` — every produce/publish call writes it onto the outgoing message (a Kafka header via `Headers.Add`, an MQTT5 user property via `WithUserProperty`), and every consume/receive call reads it back out and passes it as the `parentId` to `ActivitySource.StartActivity(name, kind, parentId)` (`CvpTelemetry.StartActivity`) instead of starting a disconnected root span. Same-process hops (e.g. DeviceGateway's MQTT-receive handler calling straight into a Kafka publish) need no explicit wiring at all — `Activity.Current` already covers it as the ambient parent.

Verified live in Jaeger — the full telemetry-ingestion trace, one trace ID, 3 separate containers:

```
vehicles/{id}/telemetry publish        (VehicleSimulator — root span)
  └─ vehicles/{id}/telemetry receive   (DeviceGateway, MQTT)
       └─ cvp.telemetry.raw publish    (DeviceGateway, Kafka)
            ├─ cvp.telemetry.raw consume → postgresql   (TelemetryProcessor)
            └─ cvp.telemetry.raw consume                (VehicleShadow — correct fan-out, same parent)
```

And the full command round-trip — one trace, 11 spans, 3 containers, HTTP → Kafka → MQTT → vehicle → MQTT → Kafka → Postgres:

```
POST /vehicles/{id}/commands                              (CommandService — root)
  ├─ postgresql                                            (insert Queued)
  └─ cvp.commands.dispatch publish
       └─ cvp.commands.dispatch consume                    (DeviceGateway)
            └─ vehicles/{id}/commands publish               (DeviceGateway, MQTT)
                 └─ vehicles/{id}/commands receive           (VehicleSimulator, MQTT)
                      └─ vehicles/{id}/commands/ack publish        (VehicleSimulator, MQTT)
                           └─ vehicles/{id}/commands/ack receive    (DeviceGateway, MQTT)
                                └─ cvp.commands.ack publish
                                     └─ cvp.commands.ack consume    (CommandService)
                                          └─ postgresql              (update to Acknowledged)
```

Known gap: **logs aren't centralized** — Serilog still only writes to each container's stdout (`docker logs`), no Loki/aggregated log search tied to trace IDs. Natural next step if this goes further, not done in this pass.

## Kafka topics (`Cvp.Contracts.KafkaTopics`)

- `cvp.telemetry.raw` — raw inbound vehicle telemetry
- `cvp.commands.dispatch` — commands to deliver to a vehicle
- `cvp.commands.ack` — delivery/execution acknowledgements from a vehicle
- `cvp.alerts` — rule-evaluated alerts
- `cvp.ota.status` — per-vehicle OTA campaign status

## Delivery phases

0. **Scaffolding — done.** Solution structure, shared contracts, local infra (Kafka/TimescaleDB/Redis via `deploy/docker-compose.yml`), one health-checked skeleton per service.
1. **Ingestion pipeline — done.** VerneMQ is the MQTT broker vehicles connect to; DeviceGateway (`MqttBridgeHostedService`) connects to it as a client and bridges `vehicles/{id}/telemetry` and `vehicles/{id}/commands/ack` onto Kafka (see "MQTT broker: VerneMQ" above — this replaced an initial embedded-MQTTnet-broker version of DeviceGateway). VehicleSimulator (`FleetSimulatorWorker`) runs a configurable concurrent fleet of virtual vehicles (one async MQTT client per vehicle) publishing synthetic telemetry straight to VerneMQ. TelemetryProcessor (`TelemetryIngestConsumer`) consumes `cvp.telemetry.raw` and persists into the `vehicle_telemetry` hypertable. Verified end-to-end against the Docker Compose stack: 20 simulated vehicles, sustained ingestion into TimescaleDB with no errors.
2. **Command/shadow loop — done, fully closed.** VehicleShadow (`TelemetryShadowConsumer` + `ShadowStore`) consumes telemetry into a Redis-backed digital twin, exposed via `GET /vehicles/{id}/shadow` and `PUT /vehicles/{id}/shadow/desired`. CommandService (`CommandRepository` + `CommandAckConsumer`) exposes `POST /vehicles/{id}/commands` (dispatches onto Kafka, persists as `Queued` in Postgres) and `GET /vehicles/{id}/commands/{commandId}`; acks arriving on `cvp.commands.ack` asynchronously update status. DeviceGateway's `CommandDispatchConsumer` + `MqttBridgeHostedService` deliver the command onward to the vehicle over MQTT, and VehicleSimulator acks it — see "MQTT broker: VerneMQ" above for the dispatch-direction detail. Built test-first — see Testing below.
3. Rules engine in TelemetryProcessor + AlertingService + live dashboard push.
4. OTA campaign management.
5. **Partially done.** VerneMQ auth + device identity via IdentityService — done (see "Device identity & VerneMQ auth" above). Still open: FleetApi BFF, OAuth2/OIDC for human users, multi-tenancy.
6. **Partially done.** Observability (OTel Collector → Jaeger + Prometheus → Grafana, custom Kafka+MQTT tracing/metrics, full cross-process trace propagation) — done, see "Observability" above. Still open: load testing (NBomber), resilience/chaos testing, perf tuning, centralized logs.
7. Production hardening — security review, CI/CD, Kubernetes manifests (`deploy/k8s`).

## Local development

```bash
cd deploy
docker compose up -d        # Kafka, TimescaleDB, Redis (+ services once Dockerfiles are exercised)
cd ..
dotnet build VehicleBackend.slnx
```

Each service's `appsettings.json` defaults `Kafka:BootstrapServers` to `localhost:9092` for running outside Docker; the compose file overrides it to `kafka:19092` for service-to-service traffic inside the Docker network.

## Testing

`tests/Cvp.IntegrationTests` exercises real dependencies via [Testcontainers](https://testcontainers.com/) (Kafka, TimescaleDB, Redis, VerneMQ — the last via Testcontainers' generic `ContainerBuilder`, since there's no dedicated VerneMQ module) rather than mocking them — no fakes to drift out of sync with the real client libraries. New endpoint-level features (CommandService, VehicleShadow) are built test-first: write the test against the intended contract, watch it fail (404/wrong status), then implement until it passes. Phase 1's DeviceGateway↔VerneMQ↔Kafka bridge and TelemetryProcessor's Kafka→TimescaleDB consumer are covered by characterization tests (written after the fact, pinning down the behavior that's already running in the compose stack).

Each web service that's tested this way exposes a `public partial class Program;` marker so `WebApplicationFactory<Program>` can boot it in-process; since every service's top-level-statement `Program` lives in the global namespace, the test project references each via `ProjectReference ... Aliases="XAssembly"` + `extern alias` to avoid a `CS0433` type collision.

Test collections run serially (`xunit.runner.json`, `parallelizeTestCollections: false`) rather than xUnit's default parallel-by-class — each class spins up its own Kafka/Postgres/Redis/VerneMQ containers, and running several sets concurrently was overloading the Docker daemon enough to cause real timing-sensitive failures (a message genuinely took longer than a test's wait budget to arrive). Serializing trades total runtime (the full suite takes under a minute) for not depending on how much container churn the host can absorb at once.

```bash
dotnet test tests/Cvp.IntegrationTests
```

Requires Docker (Testcontainers pulls/starts its own Kafka/Postgres/Redis containers, independent of `deploy/docker-compose.yml`).
