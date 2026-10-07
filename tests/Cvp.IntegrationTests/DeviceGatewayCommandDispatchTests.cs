extern alias DeviceGatewayAssembly;

using System.Text.Json;
using Cvp.Common.Kafka;
using Cvp.Contracts;
using Cvp.Contracts.Commands;
using Cvp.IntegrationTests.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MQTTnet;

using DeviceGatewayAssembly::DeviceGateway;

namespace Cvp.IntegrationTests;

/// <summary>
/// The other half of the command loop: a command CommandService queues onto
/// Kafka (<c>cvp.commands.dispatch</c>) must reach the vehicle over MQTT.
/// DeviceGateway is exercised the same way as <see cref="DeviceGatewayMqttBridgeTests"/>
/// — as an MQTT client of a real VerneMQ broker — but in the dispatch
/// direction instead of the ingestion direction.
/// </summary>
public sealed class DeviceGatewayCommandDispatchTests : IClassFixture<KafkaFixture>, IClassFixture<VerneMqFixture>, IAsyncLifetime
{
    private readonly KafkaFixture _kafka;
    private readonly VerneMqFixture _vernemq;
    private MqttBridgeHostedService _bridge = null!;
    private CommandDispatchConsumer _dispatchConsumer = null!;
    private KafkaProducer _producer = null!;

    public DeviceGatewayCommandDispatchTests(KafkaFixture kafka, VerneMqFixture vernemq)
    {
        _kafka = kafka;
        _vernemq = vernemq;
    }

    public async Task InitializeAsync()
    {
        var kafkaOptions = Options.Create(new KafkaOptions
        {
            BootstrapServers = _kafka.BootstrapServers,
            ConsumerGroupId = $"dispatch-test-{Guid.NewGuid()}"
        });
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

        _dispatchConsumer = new CommandDispatchConsumer(kafkaOptions, _bridge, NullLogger<CommandDispatchConsumer>.Instance);
        await _dispatchConsumer.StartAsync(CancellationToken.None);

        await Task.Delay(TimeSpan.FromSeconds(2));
    }

    public async Task DisposeAsync()
    {
        await _dispatchConsumer.StopAsync(CancellationToken.None);
        await _bridge.StopAsync(CancellationToken.None);
        await _producer.DisposeAsync();
    }

    [Fact]
    public async Task CommandDispatchedToKafka_ArrivesOnVehicleMqttTopic()
    {
        var command = new VehicleCommand(
            Guid.NewGuid(),
            "veh-dispatch-001",
            VehicleCommandType.LockDoors,
            Parameters: null,
            DateTimeOffset.UtcNow,
            ExpiresAt: null);

        using var mqttClient = new MqttClientFactory().CreateMqttClient();
        var receivedPayload = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        mqttClient.ApplicationMessageReceivedAsync += args =>
        {
            var bytes = args.ApplicationMessage.Payload;
            receivedPayload.TrySetResult(System.Text.Encoding.UTF8.GetString(System.Buffers.BuffersExtensions.ToArray(in bytes)));
            return Task.CompletedTask;
        };

        await mqttClient.ConnectAsync(new MqttClientOptionsBuilder()
            .WithClientId("vehicle-veh-dispatch-001")
            .WithTcpServer(_vernemq.Host, _vernemq.Port)
            .Build());
        await mqttClient.SubscribeAsync(new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(f => f.WithTopic($"vehicles/{command.VehicleId}/commands"))
            .Build());

        await _producer.PublishAsync(KafkaTopics.CommandDispatch, command.VehicleId, command);

        var payload = await receivedPayload.Task.WaitAsync(TimeSpan.FromSeconds(20));
        var received = JsonSerializer.Deserialize<VehicleCommand>(payload);

        Assert.NotNull(received);
        Assert.Equal(command.CommandId, received!.CommandId);
        Assert.Equal(command.VehicleId, received.VehicleId);
        Assert.Equal(command.Type, received.Type);

        await mqttClient.DisconnectAsync();
    }
}
