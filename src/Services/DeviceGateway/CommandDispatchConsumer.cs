using Cvp.Common.Kafka;
using Cvp.Contracts;
using Cvp.Contracts.Commands;
using Microsoft.Extensions.Options;

namespace DeviceGateway;

/// <summary>
/// Consumes commands CommandService queues onto Kafka and hands them to
/// <see cref="MqttBridgeHostedService"/> for delivery to the vehicle over
/// MQTT — the other half of the command loop (see docs/ARCHITECTURE.md).
/// </summary>
public sealed class CommandDispatchConsumer : KafkaConsumerBackgroundService<VehicleCommand>
{
    private readonly MqttBridgeHostedService _mqttBridge;

    public CommandDispatchConsumer(
        IOptions<KafkaOptions> kafkaOptions,
        MqttBridgeHostedService mqttBridge,
        ILogger<CommandDispatchConsumer> logger)
        : base(kafkaOptions, KafkaTopics.CommandDispatch, logger)
    {
        _mqttBridge = mqttBridge;
    }

    protected override Task HandleAsync(string key, VehicleCommand message, CancellationToken cancellationToken)
        => _mqttBridge.PublishCommandAsync(message, cancellationToken);
}
