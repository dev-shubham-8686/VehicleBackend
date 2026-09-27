using System.Text.Json;
using Cvp.Contracts.Telemetry;
using Microsoft.Extensions.Options;
using MQTTnet;

namespace VehicleSimulator;

/// <summary>
/// Runs a concurrent fleet of virtual vehicles, one async MQTT client per
/// vehicle, each publishing synthetic telemetry to DeviceGateway. Exists to
/// exercise the ingestion pipeline under load (see docs/ARCHITECTURE.md).
/// </summary>
public sealed class FleetSimulatorWorker(
    IOptions<FleetSimulatorOptions> options,
    ILogger<FleetSimulatorWorker> logger) : BackgroundService
{
    private readonly FleetSimulatorOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation(
            "Starting fleet simulator: {FleetSize} vehicles against {Host}:{Port}",
            _options.FleetSize, _options.BrokerHost, _options.BrokerPort);

        var vehicles = Enumerable.Range(0, _options.FleetSize)
            .Select(index => RunVehicleAsync(index, stoppingToken));

        await Task.WhenAll(vehicles);
    }

    private async Task RunVehicleAsync(int index, CancellationToken stoppingToken)
    {
        var vehicleId = $"sim-{index:D4}";
        var clientId = $"vehicle-{vehicleId}";

        using var mqttClient = new MqttClientFactory().CreateMqttClient();
        var clientOptions = new MqttClientOptionsBuilder()
            .WithClientId(clientId)
            .WithTcpServer(_options.BrokerHost, _options.BrokerPort)
            .Build();

        var random = new Random(index);
        var latitude = 37.7749 + (random.NextDouble() - 0.5) * 0.5;
        var longitude = -122.4194 + (random.NextDouble() - 0.5) * 0.5;
        var batteryPercent = 100.0;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!mqttClient.IsConnected)
                {
                    await mqttClient.ConnectAsync(clientOptions, stoppingToken);
                    logger.LogInformation("Vehicle {VehicleId} connected", vehicleId);
                }

                latitude += (random.NextDouble() - 0.5) * 0.002;
                longitude += (random.NextDouble() - 0.5) * 0.002;
                batteryPercent = Math.Max(0, batteryPercent - random.NextDouble() * 0.05);
                var speedKph = random.NextDouble() * 120;
                var faultCodes = random.NextDouble() < 0.02
                    ? new[] { "P0300" }
                    : Array.Empty<string>();

                var telemetry = new VehicleTelemetry(
                    vehicleId,
                    DateTimeOffset.UtcNow,
                    new GeoLocation(latitude, longitude),
                    speedKph,
                    batteryPercent,
                    FuelPercent: null,
                    faultCodes);

                var payload = JsonSerializer.Serialize(telemetry);
                var message = new MqttApplicationMessageBuilder()
                    .WithTopic($"vehicles/{vehicleId}/telemetry")
                    .WithPayload(payload)
                    .Build();

                await mqttClient.PublishAsync(message, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Vehicle {VehicleId} publish failed, will retry", vehicleId);
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(_options.PublishIntervalSeconds), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        if (mqttClient.IsConnected)
        {
            await mqttClient.DisconnectAsync(cancellationToken: CancellationToken.None);
        }
    }
}
