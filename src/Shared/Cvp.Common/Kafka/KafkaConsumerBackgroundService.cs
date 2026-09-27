using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Cvp.Common.Kafka;

/// <summary>
/// Base class for services that consume one topic. Subclasses only implement
/// <see cref="HandleAsync"/> — the poll loop, deserialization, and commit/backoff
/// on handler failure are handled here so every consumer behaves consistently.
/// </summary>
public abstract class KafkaConsumerBackgroundService<TMessage> : BackgroundService
{
    private readonly ILogger _logger;
    private readonly IConsumer<string, string> _consumer;
    private readonly string _topic;

    protected KafkaConsumerBackgroundService(IOptions<KafkaOptions> options, string topic, ILogger logger)
    {
        _topic = topic;
        _logger = logger;
        var config = new ConsumerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            GroupId = options.Value.ConsumerGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };
        _consumer = new ConsumerBuilder<string, string>(config).Build();
    }

    protected abstract Task HandleAsync(string key, TMessage message, CancellationToken cancellationToken);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_topic);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = _consumer.Consume(stoppingToken);
                var message = JsonSerializer.Deserialize<TMessage>(result.Message.Value);
                if (message is not null)
                {
                    await HandleAsync(result.Message.Key, message, stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process message from topic {Topic}", _topic);
                try
                {
                    // Avoid a tight retry loop (e.g. broker unreachable, topic not yet created).
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        _consumer.Close();
    }
}
