using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Cvp.Common.Observability;

/// <summary>
/// Shared ActivitySource/Meter for the messaging hops ASP.NET Core/HttpClient
/// auto-instrumentation can't see: Kafka produce/consume and MQTT publish/receive
/// (neither Confluent.Kafka nor MQTTnet ship official OTel instrumentation).
/// Registered once in <c>AddCvpServiceDefaults</c> so every service's traces
/// and metrics include this activity for free (see docs/ARCHITECTURE.md).
///
/// Cross-process propagation rides the W3C traceparent string (<see cref="Activity.Id"/>)
/// as a Kafka message header / MQTT5 user property named "traceparent" — see
/// <see cref="StartActivity"/> and each call site that reads/writes it.
/// </summary>
public static class CvpTelemetry
{
    public const string SourceName = "Cvp.Messaging";
    public const string TraceParentPropertyName = "traceparent";

    public static readonly ActivitySource ActivitySource = new(SourceName);

    private static readonly Meter Meter = new(SourceName);

    public static readonly Counter<long> MessagesProduced =
        Meter.CreateCounter<long>("cvp.kafka.messages_produced", unit: "{message}");

    public static readonly Counter<long> MessagesConsumed =
        Meter.CreateCounter<long>("cvp.kafka.messages_consumed", unit: "{message}");

    public static readonly Histogram<double> ConsumeDuration =
        Meter.CreateHistogram<double>("cvp.kafka.consume_duration", unit: "ms");

    /// <summary>
    /// Starts an activity continuing a trace received from another process
    /// (e.g. a Kafka header or MQTT user property), falling back to the normal
    /// ambient-parent behavior when <paramref name="parentTraceParent"/> is null
    /// (same-process hop, e.g. DeviceGateway bridging an MQTT receive straight
    /// into a Kafka publish — <see cref="Activity.Current"/> already covers that).
    /// </summary>
    public static Activity? StartActivity(string name, ActivityKind kind, string? parentTraceParent)
    {
        return string.IsNullOrEmpty(parentTraceParent)
            ? ActivitySource.StartActivity(name, kind)
            : ActivitySource.StartActivity(name, kind, parentTraceParent);
    }
}
