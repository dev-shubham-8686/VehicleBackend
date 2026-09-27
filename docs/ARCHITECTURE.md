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
| `IdentityService` | Web API | Device certificate issuance/validation; OAuth2/OIDC for human users |

## Shared libraries

- `Cvp.Contracts` — message/event DTOs shared across services (telemetry, commands, shadow state, alerts) and Kafka topic name constants.
- `Cvp.Common` — cross-cutting service wiring: Serilog + OpenTelemetry setup (`AddCvpServiceDefaults`), health checks, a thin Kafka producer/consumer wrapper, and `StartupRetry` (retries a startup dependency check with backoff instead of crashing — needed because Compose starts containers concurrently, so e.g. Postgres may still be starting when a service's first query fires).

## MQTT broker: VerneMQ (not embedded)

The vehicle-facing MQTT broker is a standalone [VerneMQ](https://vernemq.com/) container (`deploy/docker-compose.yml`), not code running inside DeviceGateway — matching how real OEM platforms separate "a broker built to hold millions of persistent connections" from "the service that does something with the messages." DeviceGateway connects to it as an ordinary MQTT *client* (`MqttBridgeHostedService`), subscribed to `vehicles/+/telemetry` and `vehicles/+/commands/ack`, and bridges whatever it receives onto Kafka.

Two things worth knowing:
- **Reconnect is a manual poll loop, not MQTTnet's managed client.** No release of `MQTTnet.Extensions.ManagedClient` targets MQTTnet 5.x yet (it tops out at 4.3.7, incompatible with the 5.2 core package used here) — `MqttBridgeHostedService` polls `IsConnected` every 5s and reconnects/resubscribes itself instead.
- **No connection auth yet.** VerneMQ runs with `DOCKER_VERNEMQ_ALLOW_ANONYMOUS=on` for now. The embedded-broker version of this service used to reject client IDs without a `vehicle-` prefix; that check doesn't carry over to VerneMQ automatically. Phase 5 replaces this with a proper VerneMQ auth plugin (e.g. `vmq_diversity`) backed by IdentityService, rather than reimplementing an ad hoc client-ID check.

## Data flow (steady state)

```
Vehicle --MQTT--> VerneMQ <--MQTT(client)-- DeviceGateway --Kafka(cvp.telemetry.raw)--> TelemetryProcessor --> TimescaleDB
                                                                                                |
                                                                                                +--Kafka(cvp.alerts)--> AlertingService --SignalR--> Dashboard

FleetApi/CommandService --Kafka(cvp.commands.dispatch)--> DeviceGateway --MQTT--> VerneMQ --MQTT--> Vehicle
Vehicle --MQTT(ack)--> VerneMQ --MQTT(client)--> DeviceGateway --Kafka(cvp.commands.ack)--> CommandService
```

Note: the `cvp.commands.dispatch` → VerneMQ leg (DeviceGateway delivering a command *to* a vehicle) isn't implemented yet — CommandService publishes to Kafka today, but nothing consumes that topic and republishes over MQTT. That's the next gap to close, likely alongside Phase 3's rules engine work.

## Kafka topics (`Cvp.Contracts.KafkaTopics`)

- `cvp.telemetry.raw` — raw inbound vehicle telemetry
- `cvp.commands.dispatch` — commands to deliver to a vehicle
- `cvp.commands.ack` — delivery/execution acknowledgements from a vehicle
- `cvp.alerts` — rule-evaluated alerts
- `cvp.ota.status` — per-vehicle OTA campaign status

## Delivery phases

0. **Scaffolding — done.** Solution structure, shared contracts, local infra (Kafka/TimescaleDB/Redis via `deploy/docker-compose.yml`), one health-checked skeleton per service.
1. **Ingestion pipeline — done.** VerneMQ is the MQTT broker vehicles connect to; DeviceGateway (`MqttBridgeHostedService`) connects to it as a client and bridges `vehicles/{id}/telemetry` and `vehicles/{id}/commands/ack` onto Kafka (see "MQTT broker: VerneMQ" above — this replaced an initial embedded-MQTTnet-broker version of DeviceGateway). VehicleSimulator (`FleetSimulatorWorker`) runs a configurable concurrent fleet of virtual vehicles (one async MQTT client per vehicle) publishing synthetic telemetry straight to VerneMQ. TelemetryProcessor (`TelemetryIngestConsumer`) consumes `cvp.telemetry.raw` and persists into the `vehicle_telemetry` hypertable. Verified end-to-end against the Docker Compose stack: 20 simulated vehicles, sustained ingestion into TimescaleDB with no errors.
2. **Command/shadow loop — done.** VehicleShadow (`TelemetryShadowConsumer` + `ShadowStore`) consumes telemetry into a Redis-backed digital twin, exposed via `GET /vehicles/{id}/shadow` and `PUT /vehicles/{id}/shadow/desired`. CommandService (`CommandRepository` + `CommandAckConsumer`) exposes `POST /vehicles/{id}/commands` (dispatches onto Kafka, persists as `Queued` in Postgres) and `GET /vehicles/{id}/commands/{commandId}`; acks arriving on `cvp.commands.ack` asynchronously update status. Both built test-first — see Testing below.
3. Rules engine in TelemetryProcessor + AlertingService + live dashboard push.
4. OTA campaign management.
5. FleetApi BFF + IdentityService (device certs + user OAuth2/OIDC) + multi-tenancy.
6. Observability dashboards, load testing (NBomber), resilience/chaos testing, perf tuning.
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

```bash
dotnet test tests/Cvp.IntegrationTests
```

Requires Docker (Testcontainers pulls/starts its own Kafka/Postgres/Redis containers, independent of `deploy/docker-compose.yml`).
