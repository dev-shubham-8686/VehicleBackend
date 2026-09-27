using Cvp.Common.Kafka;
using Cvp.Contracts;
using Cvp.Contracts.Telemetry;
using Microsoft.Extensions.Options;

namespace VehicleShadow;

/// <summary>
/// Consumes raw telemetry from Kafka and updates each vehicle's reported state
/// (see docs/ARCHITECTURE.md).
/// </summary>
public sealed class TelemetryShadowConsumer : KafkaConsumerBackgroundService<VehicleTelemetry>
{
    private readonly ShadowStore _store;

    public TelemetryShadowConsumer(
        IOptions<KafkaOptions> kafkaOptions,
        ShadowStore store,
        ILogger<TelemetryShadowConsumer> logger)
        : base(kafkaOptions, KafkaTopics.TelemetryRaw, logger)
    {
        _store = store;
    }

    protected override Task HandleAsync(string key, VehicleTelemetry message, CancellationToken cancellationToken)
        => _store.ApplyTelemetryAsync(message);
}
