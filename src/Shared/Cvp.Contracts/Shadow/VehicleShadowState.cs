using Cvp.Contracts.Telemetry;

namespace Cvp.Contracts.Shadow;

public sealed record VehicleShadowState(
    string VehicleId,
    VehicleTelemetry? LastReported,
    bool IsOnline,
    DateTimeOffset LastSeenAt,
    IReadOnlyDictionary<string, string>? DesiredState);
