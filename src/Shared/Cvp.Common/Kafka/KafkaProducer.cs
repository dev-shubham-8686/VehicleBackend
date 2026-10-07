using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Cvp.Common.Observability;
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
        using var activity = CvpTelemetry.ActivitySource.StartActivity($"{topic} publish", ActivityKind.Producer);
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination.name", topic);
        activity?.SetTag("messaging.kafka.message.key", key);

        var payload = JsonSerializer.Serialize(message);
        var kafkaMessage = new Message<string, string> { Key = key, Value = payload };

        // Carries the trace across the process boundary: the consumer reads this
        // back out and continues the same trace instead of starting a new one
        // (see KafkaConsumerBackgroundService and docs/ARCHITECTURE.md).
        if (activity?.Id is { } traceParent)
        {
            var headers = new Headers();
            headers.Add(CvpTelemetry.TraceParentPropertyName, Encoding.UTF8.GetBytes(traceParent));
            kafkaMessage.Headers = headers;
        }

        await _producer.ProduceAsync(topic, kafkaMessage, cancellationToken);

        CvpTelemetry.MessagesProduced.Add(1, new KeyValuePair<string, object?>("topic", topic));
    }

    public ValueTask DisposeAsync()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
        return ValueTask.CompletedTask;
    }
}
