# Connected Vehicle Platform (CVP)

A simplified, production-shaped backend for talking to a fleet of connected vehicles — telemetry ingestion, digital twin state, remote commands, rule-based alerting, and OTA campaign management. Built in C# (.NET 10) as a set of independently deployable services. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full design and delivery phases.

## Structure

```
src/
  Shared/
    Cvp.Contracts/     shared message/event DTOs + Kafka topic names
    Cvp.Common/        Serilog/OpenTelemetry wiring, Kafka producer/consumer helpers
  Services/
    DeviceGateway/      MQTT client bridging VerneMQ <-> Kafka
    VehicleSimulator/   synthetic vehicle fleet (load/integration testing)
    TelemetryProcessor/ Kafka -> TimescaleDB, rule evaluation
    VehicleShadow/      digital twin (Redis)
    CommandService/     remote command dispatch/tracking (Postgres)
    AlertingService/    rules -> SignalR push
    OtaService/         update campaign management
    FleetApi/           dashboard/mobile BFF
    IdentityService/    device identity (VerneMQ webhook-auth backend); user auth not built yet
deploy/
  docker-compose.yml    local infra: Kafka (KRaft), TimescaleDB, Redis, VerneMQ, AKHQ, pgAdmin, RedisInsight, OTel Collector, Jaeger, Prometheus, Grafana + all services
  observability/        otel-collector-config.yaml, prometheus.yml, Grafana datasource/dashboard provisioning
  k8s/                  Kubernetes manifests (added in phase 7)
docs/
  ARCHITECTURE.md
tests/
  Cvp.IntegrationTests/  Testcontainers-backed integration tests (real Kafka/Postgres/Redis)
```

## Getting started

```bash
dotnet build VehicleBackend.slnx
```

```bash
cd deploy
docker compose up -d
```

Once up, [AKHQ](http://localhost:8080) gives a browsable UI over the `cvp` Kafka cluster — topics, partitions, consumer group lag, and message contents (handy for confirming `cvp.telemetry.raw` / `cvp.commands.dispatch` / `cvp.commands.ack` are actually flowing without reaching for `kafka-console-consumer`).

[pgAdmin](http://localhost:5050) gives a browsable UI over the `cvp` Postgres/TimescaleDB database — runs in desktop mode (no login), with the `CVP Postgres` server pre-registered (`deploy/pgadmin-servers.json`) pointing at `postgres:5432`/db `cvp`. It'll still prompt once for the database password the first time you connect to that server: `cvp_dev_only`.

[RedisInsight](http://localhost:5540) is pgAdmin's Redis equivalent — official Redis GUI, with the `CVP Redis` connection pre-configured (`RI_REDIS_HOST`/`RI_REDIS_PORT`/`RI_REDIS_ALIAS` env vars in `docker-compose.yml`, no manual setup) pointing at `redis:6379`. Browse the live `shadow:{vehicleId}` keys VehicleShadow writes.

[Grafana](http://localhost:3000) (anonymous access, dev-only) has a pre-built **CVP Overview** dashboard — Kafka produce/consume rate by topic, p95 consume duration, p95 HTTP latency by service — backed by [Prometheus](http://localhost:9090) and [Jaeger](http://localhost:16686) (distributed traces), both fed by an OpenTelemetry Collector every service exports to. A single request traces end to end across process boundaries — e.g. open [Jaeger](http://localhost:16686), pick `CommandService`, and a `POST /vehicles/{id}/commands` trace shows all 11 spans of the full round trip: HTTP → Kafka → MQTT → the (simulated) vehicle → MQTT → Kafka → Postgres, one trace ID throughout. See [Observability in docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#observability) for how (Kafka produce/consume had to be hand-instrumented — no official OTel support for Confluent.Kafka — and the W3C traceparent rides as a Kafka header / MQTT5 user property) and what isn't yet (centralized logs).

## Testing

```bash
dotnet test tests/Cvp.IntegrationTests
```

Runs against real Kafka/Postgres/Redis/VerneMQ via Testcontainers (requires Docker) — see [Testing in docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#testing) for the test-first workflow used for new endpoints.

## Status

- **Phase 0 (scaffolding) — done.** Solution builds, every service exposes `/health`, local infra is defined.
- **Phase 1 (ingestion pipeline) — done.** Vehicle (simulated) → VerneMQ → DeviceGateway (MQTT client) → Kafka → TelemetryProcessor → TimescaleDB, verified end-to-end. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#mqtt-broker-vernemq-not-embedded) for why the broker is standalone VerneMQ rather than code inside DeviceGateway.
- **Phase 2 (command/shadow loop) — done, fully closed.** VehicleShadow (Redis digital twin) and CommandService (command dispatch + ack tracking) built test-first. A queued command is now actually delivered to the vehicle (DeviceGateway → VerneMQ) and the vehicle's ack flows back and updates status — verified live, `Queued` → `Acknowledged` in ~160ms.
- **Phase 5, partial (VerneMQ auth + device identity) — done.** `allow_anonymous` is off; IdentityService issues per-vehicle MQTT credentials and answers VerneMQ's webhook-auth calls, enforcing that a vehicle can only publish/subscribe to its own topics. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#device-identity--vernemq-auth) — including a real gotcha hit and fixed along the way (MQTT5 vs MQTT3.1.1 webhook hook names). OAuth2/OIDC for human users and multi-tenancy are still open.
- **Phase 6, partial (observability) — done.** OpenTelemetry Collector → Jaeger (traces) + Prometheus (metrics) → Grafana, with hand-written Kafka+MQTT instrumentation (nothing exists off-the-shelf for Confluent.Kafka/MQTTnet) and Npgsql's built-in tracing wired in, plus full cross-process trace propagation — a single trace now follows one request from an HTTP call through Kafka and MQTT and back, verified live end-to-end. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#observability). Load testing, resilience/chaos testing, and centralized logs are still open.

Remaining business logic lands phase-by-phase per [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#delivery-phases).

To verify the ingestion pipeline yourself:

```bash
cd deploy
docker compose up -d --build
# wait ~15s, then:
docker exec cvp-postgres psql -U cvp -d cvp -c "select count(*) from vehicle_telemetry;"
curl http://localhost:8082/vehicles/sim-0000/shadow
curl -X POST http://localhost:8083/vehicles/sim-0000/commands -H "Content-Type: application/json" -d "{\"type\":\"LockDoors\"}"
# note the commandId from the response, then (a couple seconds later):
curl http://localhost:8083/vehicles/sim-0000/commands/{commandId}   # status should now be "Acknowledged"
```
