using System.Text.Json;
using Confluent.Kafka;

namespace Cvp.IntegrationTests.Infrastructure;

/// <summary>
/// A raw producer standing in for "some other service" publishing onto a topic
/// (e.g. DeviceGateway publishing a command-ack, or a vehicle publishing telemetry),
/// so tests can drive a service's Kafka consumer without spinning up the upstream service.
/// </summary>
public sealed class TestKafkaPublisher : IAsyncDisposable
{
    private readonly IProducer<string, string> _producer;

    public TestKafkaPublisher(string bootstrapServers)
    {
        _producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build();
    }

    public async Task PublishAsync<T>(string topic, string key, T message)
    {
        var payload = JsonSerializer.Serialize(message);
        await _producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = payload });
        _producer.Flush(TimeSpan.FromSeconds(5));
    }

    public ValueTask DisposeAsync()
    {
        _producer.Dispose();
        return ValueTask.CompletedTask;
    }
}
