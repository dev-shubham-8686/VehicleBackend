using System.Diagnostics;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Cvp.Common;
using Cvp.Common.Observability;
using Cvp.Contracts.Commands;
using Cvp.Contracts.Telemetry;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Packets;

namespace VehicleSimulator;

/// <summary>
/// Runs a concurrent fleet of virtual vehicles, one async MQTT client per
/// vehicle, each publishing synthetic telemetry to DeviceGateway. Exists to
/// exercise the ingestion pipeline under load (see docs/ARCHITECTURE.md).
/// </summary>
public sealed class FleetSimulatorWorker(
    IOptions<FleetSimulatorOptions> options,
    IHttpClientFactory httpClientFactory,
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

        var deviceSecret = await RegisterDeviceAsync(vehicleId, stoppingToken);

        using var mqttClient = new MqttClientFactory().CreateMqttClient();
        var clientOptions = new MqttClientOptionsBuilder()
            .WithClientId(clientId)
            .WithTcpServer(_options.BrokerHost, _options.BrokerPort)
            .WithCredentials(vehicleId, deviceSecret)
            .Build();
        var commandSubscribeOptions = new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(f => f.WithTopic($"vehicles/{vehicleId}/commands"))
            .Build();

        mqttClient.ApplicationMessageReceivedAsync += args => HandleCommandAsync(vehicleId, mqttClient, args);

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
                    await mqttClient.SubscribeAsync(commandSubscribeOptions, stoppingToken);
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

                var telemetryTopic = $"vehicles/{vehicleId}/telemetry";
                using (var activity = CvpTelemetry.ActivitySource.StartActivity($"{telemetryTopic} publish", ActivityKind.Producer))
                {
                    activity?.SetTag("messaging.system", "mqtt");
                    activity?.SetTag("messaging.destination.name", telemetryTopic);

                    var messageBuilder = new MqttApplicationMessageBuilder()
                        .WithTopic(telemetryTopic)
                        .WithPayload(JsonSerializer.Serialize(telemetry));

                    if (activity?.Id is { } traceParent)
                    {
                        messageBuilder.WithUserProperty(CvpTelemetry.TraceParentPropertyName, Encoding.UTF8.GetBytes(traceParent));
                    }

                    await mqttClient.PublishAsync(messageBuilder.Build(), stoppingToken);
                }
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

    /// <summary>
    /// Device provisioning: obtains this vehicle's MQTT credentials from
    /// IdentityService before connecting to VerneMQ. Retries with backoff
    /// since IdentityService may still be starting up (see docs/ARCHITECTURE.md).
    /// </summary>
    private async Task<string> RegisterDeviceAsync(string vehicleId, CancellationToken cancellationToken)
    {
        string? deviceSecret = null;

        await StartupRetry.ExecuteAsync(
            async () =>
            {
                using var httpClient = httpClientFactory.CreateClient();
                var response = await httpClient.PostAsJsonAsync(
                    $"{_options.IdentityServiceBaseUrl}/devices/register",
                    new { vehicleId },
                    cancellationToken);
                response.EnsureSuccessStatusCode();

                var body = await response.Content.ReadFromJsonAsync<RegisterDeviceResponse>(JsonSerializerOptions.Web, cancellationToken);
                deviceSecret = body!.DeviceSecret;
            },
            logger);

        return deviceSecret!;
    }

    /// <summary>
    /// Stands in for the vehicle's own firmware acting on a command — here it
    /// just acks immediately rather than actually locking doors etc.
    /// </summary>
    private async Task HandleCommandAsync(string vehicleId, IMqttClient mqttClient, MqttApplicationMessageReceivedEventArgs args)
    {
        var topic = args.ApplicationMessage.Topic;
        var traceParent = args.ApplicationMessage.UserProperties
            ?.FirstOrDefault(p => p.Name == CvpTelemetry.TraceParentPropertyName)?.ReadValueAsString();

        using var receiveActivity = CvpTelemetry.StartActivity($"{topic} receive", ActivityKind.Consumer, traceParent);
        receiveActivity?.SetTag("messaging.system", "mqtt");
        receiveActivity?.SetTag("messaging.destination.name", topic);

        try
        {
            var payloadSequence = args.ApplicationMessage.Payload;
            var payload = Encoding.UTF8.GetString(System.Buffers.BuffersExtensions.ToArray(in payloadSequence));
            var command = JsonSerializer.Deserialize<VehicleCommand>(payload);
            if (command is null)
            {
                return;
            }

            logger.LogInformation(
                "Vehicle {VehicleId} received command {CommandId} ({Type})",
                vehicleId, command.CommandId, command.Type);

            var ack = new VehicleCommandAck(
                command.CommandId,
                vehicleId,
                VehicleCommandStatus.Acknowledged,
                DateTimeOffset.UtcNow,
                FailureReason: null);

            var ackTopic = $"vehicles/{vehicleId}/commands/ack";
            using var publishActivity = CvpTelemetry.ActivitySource.StartActivity($"{ackTopic} publish", ActivityKind.Producer);
            publishActivity?.SetTag("messaging.system", "mqtt");
            publishActivity?.SetTag("messaging.destination.name", ackTopic);

            var ackMessageBuilder = new MqttApplicationMessageBuilder()
                .WithTopic(ackTopic)
                .WithPayload(JsonSerializer.Serialize(ack));

            if (publishActivity?.Id is { } ackTraceParent)
            {
                ackMessageBuilder.WithUserProperty(CvpTelemetry.TraceParentPropertyName, Encoding.UTF8.GetBytes(ackTraceParent));
            }

            await mqttClient.PublishAsync(ackMessageBuilder.Build());
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Vehicle {VehicleId} failed to process an incoming command", vehicleId);
        }
    }
}
