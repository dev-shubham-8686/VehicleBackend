extern alias DeviceGatewayAssembly;

using System.Text.Json;
using Confluent.Kafka;
using Cvp.Common.Kafka;
using Cvp.Contracts;
using Cvp.Contracts.Telemetry;
using Cvp.IntegrationTests.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MQTTnet;

using DeviceGatewayAssembly::DeviceGateway;

namespace Cvp.IntegrationTests;

/// <summary>
/// Characterizes the DeviceGateway <-> VerneMQ <-> Kafka bridge: a message a
/// vehicle publishes to a real VerneMQ broker must arrive, unchanged, as a
/// Kafka record on <c>cvp.telemetry.raw</c>. DeviceGateway is exercised as an
/// MQTT *client* here (<see cref="MqttBridgeHostedService"/>), matching how it
/// actually connects to the standalone VerneMQ broker in production.
/// </summary>
public sealed class DeviceGatewayMqttBridgeTests : IClassFixture<KafkaFixture>, IClassFixture<VerneMqFixture>, IAsyncLifetime
{
    private readonly KafkaFixture _kafka;
    private readonly VerneMqFixture _vernemq;
    private MqttBridgeHostedService _bridge = null!;
    private KafkaProducer _producer = null!;

    public DeviceGatewayMqttBridgeTests(KafkaFixture kafka, VerneMqFixture vernemq)
    {
        _kafka = kafka;
        _vernemq = vernemq;
    }

    public async Task InitializeAsync()
    {
        var kafkaOptions = Options.Create(new KafkaOptions { BootstrapServers = _kafka.BootstrapServers });
        _producer = new KafkaProducer(kafkaOptions);

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Mqtt:BrokerHost"] = _vernemq.Host,
                ["Mqtt:BrokerPort"] = _vernemq.Port.ToString()
            })
            .Build();

        _bridge = new MqttBridgeHostedService(_producer, configuration, NullLogger<MqttBridgeHostedService>.Instance);
        await _bridge.StartAsync(CancellationToken.None);

        // The bridge's reconnect loop polls every 5s; give it a moment to
        // connect and subscribe before a test publishes anything.
        await Task.Delay(TimeSpan.FromSeconds(2));
    }

    public async Task DisposeAsync()
    {
        await _bridge.StopAsync(CancellationToken.None);
        await _producer.DisposeAsync();
    }

    [Fact]
    public async Task TelemetryPublishedToVerneMq_ArrivesOnKafkaTelemetryTopic()
    {
        var telemetry = new VehicleTelemetry(
            "veh-bridge-001",
            DateTimeOffset.UtcNow,
            new GeoLocation(51.5, -0.1),
            SpeedKph: 42.0,
            BatteryPercent: 73.0,
            FuelPercent: null,
            ActiveFaultCodes: Array.Empty<string>());

        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = _kafka.BootstrapServers,
            GroupId = $"bridge-test-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();
        consumer.Subscribe(KafkaTopics.TelemetryRaw);

        using var mqttClient = new MqttClientFactory().CreateMqttClient();
        await mqttClient.ConnectAsync(new MqttClientOptionsBuilder()
            .WithClientId("vehicle-veh-bridge-001")
            .WithTcpServer(_vernemq.Host, _vernemq.Port)
            .Build());

        await mqttClient.PublishAsync(new MqttApplicationMessageBuilder()
            .WithTopic($"vehicles/{telemetry.VehicleId}/telemetry")
            .WithPayload(JsonSerializer.Serialize(telemetry))
            .Build());

        // Generous budget: under a full test-suite run, several other test classes
        // are churning their own Testcontainers through the same Docker daemon, which
        // occasionally slows this one's broker/container startup (see docs/ARCHITECTURE.md#testing).
        ConsumeResult<string, string>? result = null;
        for (var attempt = 0; attempt < 60 && result is null; attempt++)
        {
            try
            {
                result = consumer.Consume(TimeSpan.FromMilliseconds(500));
            }
            catch (ConsumeException)
            {
                // Topic not auto-created yet (race on a fresh broker); keep polling.
            }
        }

        Assert.NotNull(result);
        Assert.Equal(telemetry.VehicleId, result!.Message.Key);

        var received = JsonSerializer.Deserialize<VehicleTelemetry>(result.Message.Value);
        Assert.NotNull(received);
        Assert.Equal(telemetry.VehicleId, received!.VehicleId);
        Assert.Equal(telemetry.SpeedKph, received.SpeedKph);
        Assert.Equal(telemetry.BatteryPercent, received.BatteryPercent);

        await mqttClient.DisconnectAsync();
    }
}
