using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Cvp.Common.Kafka;
using Cvp.Contracts;
using Cvp.Contracts.Commands;
using Cvp.Contracts.Telemetry;
using MQTTnet;

namespace DeviceGateway;

/// <summary>
/// Connects to the standalone VerneMQ broker as an MQTT client and bridges
/// inbound telemetry / command-ack publishes onto Kafka. The broker itself
/// (connections, subscriptions, QoS, persistence) is VerneMQ's job — this
/// service only subscribes to <c>vehicles/+/telemetry</c> and
/// <c>vehicles/+/commands/ack</c> and forwards what it receives. A manual
/// reconnect loop is used instead of MQTTnet's managed client because no
/// release of MQTTnet.Extensions.ManagedClient targets MQTTnet 5.x yet
/// (see docs/ARCHITECTURE.md).
/// </summary>
public sealed partial class MqttBridgeHostedService : BackgroundService
{
    private readonly KafkaProducer _kafkaProducer;
    private readonly ILogger<MqttBridgeHostedService> _logger;
    private readonly string _brokerHost;
    private readonly int _brokerPort;
    private IMqttClient? _client;

    public MqttBridgeHostedService(KafkaProducer kafkaProducer, IConfiguration configuration, ILogger<MqttBridgeHostedService> logger)
    {
        _kafkaProducer = kafkaProducer;
        _logger = logger;
        _brokerHost = configuration["Mqtt:BrokerHost"] ?? "localhost";
        _brokerPort = configuration.GetValue("Mqtt:BrokerPort", 1883);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _client = new MqttClientFactory().CreateMqttClient();
        _client.ApplicationMessageReceivedAsync += OnMessageReceivedAsync;

        var clientOptions = new MqttClientOptionsBuilder()
            .WithClientId($"device-gateway-{Guid.NewGuid():N}")
            .WithTcpServer(_brokerHost, _brokerPort)
            .Build();

        var subscribeOptions = new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(f => f.WithTopic("vehicles/+/telemetry"))
            .WithTopicFilter(f => f.WithTopic("vehicles/+/commands/ack"))
            .Build();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!_client.IsConnected)
                {
                    await _client.ConnectAsync(clientOptions, stoppingToken);
                    await _client.SubscribeAsync(subscribeOptions, stoppingToken);
                    _logger.LogInformation("Connected to MQTT broker {Host}:{Port} and subscribed", _brokerHost, _brokerPort);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to connect to MQTT broker {Host}:{Port}, will retry", _brokerHost, _brokerPort);
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        if (_client.IsConnected)
        {
            await _client.DisconnectAsync(cancellationToken: CancellationToken.None);
        }
    }

    private async Task OnMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs args)
    {
        var topic = args.ApplicationMessage.Topic;
        var payloadSequence = args.ApplicationMessage.Payload;
        var payload = Encoding.UTF8.GetString(System.Buffers.BuffersExtensions.ToArray(in payloadSequence));

        var telemetryMatch = TelemetryTopicRegex().Match(topic);
        if (telemetryMatch.Success)
        {
            await BridgeAsync(topic, payload, KafkaTopics.TelemetryRaw,
                json => JsonSerializer.Deserialize<VehicleTelemetry>(json),
                telemetry => telemetry.VehicleId);
            return;
        }

        var ackMatch = CommandAckTopicRegex().Match(topic);
        if (ackMatch.Success)
        {
            await BridgeAsync(topic, payload, KafkaTopics.CommandAck,
                json => JsonSerializer.Deserialize<VehicleCommandAck>(json),
                ack => ack.VehicleId);
        }
    }

    private async Task BridgeAsync<T>(
        string topic,
        string payload,
        string kafkaTopic,
        Func<string, T?> deserialize,
        Func<T, string> keySelector)
        where T : class
    {
        try
        {
            var message = deserialize(payload);
            if (message is not null)
            {
                await _kafkaProducer.PublishAsync(kafkaTopic, keySelector(message), message);
            }
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Dropped malformed message on topic {Topic}", topic);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to bridge message from topic {Topic} onto Kafka topic {KafkaTopic}", topic, kafkaTopic);
        }
    }

    [GeneratedRegex(@"^vehicles/(?<vehicleId>[^/]+)/telemetry$")]
    private static partial Regex TelemetryTopicRegex();

    [GeneratedRegex(@"^vehicles/(?<vehicleId>[^/]+)/commands/ack$")]
    private static partial Regex CommandAckTopicRegex();
}
