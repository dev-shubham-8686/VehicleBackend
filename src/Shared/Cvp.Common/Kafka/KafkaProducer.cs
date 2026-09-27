using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Cvp.Common.Kafka;

/// <summary>
/// Thin, reusable JSON producer. Each service registers this once via
/// <see cref="KafkaServiceCollectionExtensions.AddCvpKafkaProducer"/> and calls
/// <see cref="PublishAsync"/> per message; the underlying Confluent client is
/// safe for concurrent use, which is what lets many request handlers share one instance.
/// </summary>
public sealed class KafkaProducer : IAsyncDisposable
{
    private readonly IProducer<string, string> _producer;

    public KafkaProducer(IOptions<KafkaOptions> options)
    {
        var config = new ProducerConfig { BootstrapServers = options.Value.BootstrapServers };
        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task PublishAsync<T>(string topic, string key, T message, CancellationToken cancellationToken = default)
    {
        var payload = JsonSerializer.Serialize(message);
        await _producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = payload }, cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
        return ValueTask.CompletedTask;
    }
}
