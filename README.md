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
    IdentityService/    device certs + user auth
deploy/
  docker-compose.yml    local infra: Kafka (KRaft), TimescaleDB, Redis, VerneMQ + all services
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

## Testing

```bash
dotnet test tests/Cvp.IntegrationTests
```

Runs against real Kafka/Postgres/Redis/VerneMQ via Testcontainers (requires Docker) — see [Testing in docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#testing) for the test-first workflow used for new endpoints.

## Status

- **Phase 0 (scaffolding) — done.** Solution builds, every service exposes `/health`, local infra is defined.
- **Phase 1 (ingestion pipeline) — done.** Vehicle (simulated) → VerneMQ → DeviceGateway (MQTT client) → Kafka → TelemetryProcessor → TimescaleDB, verified end-to-end. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#mqtt-broker-vernemq-not-embedded) for why the broker is standalone VerneMQ rather than code inside DeviceGateway.
- **Phase 2 (command/shadow loop) — done.** VehicleShadow (Redis digital twin) and CommandService (command dispatch + ack tracking) built test-first; verified against the live compose stack with curl.

Remaining business logic lands phase-by-phase per [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#delivery-phases).

To verify the ingestion pipeline yourself:

```bash
cd deploy
docker compose up -d --build
# wait ~15s, then:
docker exec cvp-postgres psql -U cvp -d cvp -c "select count(*) from vehicle_telemetry;"
curl http://localhost:8082/vehicles/sim-0000/shadow
curl -X POST http://localhost:8083/vehicles/sim-0000/commands -H "Content-Type: application/json" -d "{\"type\":\"LockDoors\"}"
```
