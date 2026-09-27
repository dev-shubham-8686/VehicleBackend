using System.Text.Json;
using Cvp.Contracts.Shadow;
using Cvp.Contracts.Telemetry;
using StackExchange.Redis;

namespace VehicleShadow;

/// <summary>
/// Redis-backed digital twin: one JSON blob per vehicle under
/// <c>shadow:{vehicleId}</c>. Read-modify-write is not compare-and-swapped —
/// acceptable while only one writer (this service) updates reported state and
/// one (FleetApi/CommandService, later) updates desired state; revisit with a
/// Lua script or optimistic version if that changes (see docs/ARCHITECTURE.md).
/// </summary>
public sealed class ShadowStore(IConnectionMultiplexer redis)
{
    private static string KeyFor(string vehicleId) => $"shadow:{vehicleId}";

    public async Task<VehicleShadowState?> GetAsync(string vehicleId)
    {
        var db = redis.GetDatabase();
        var json = await db.StringGetAsync(KeyFor(vehicleId));
        return json.IsNullOrEmpty ? null : JsonSerializer.Deserialize<VehicleShadowState>((string)json!);
    }

    public async Task ApplyTelemetryAsync(VehicleTelemetry telemetry)
    {
        var existing = await GetAsync(telemetry.VehicleId);
        var updated = existing is null
            ? new VehicleShadowState(telemetry.VehicleId, telemetry, IsOnline: true, telemetry.Timestamp, DesiredState: null)
            : existing with { LastReported = telemetry, IsOnline = true, LastSeenAt = telemetry.Timestamp };

        await redis.GetDatabase().StringSetAsync(KeyFor(telemetry.VehicleId), JsonSerializer.Serialize(updated));
    }

    public async Task<VehicleShadowState> SetDesiredStateAsync(string vehicleId, IReadOnlyDictionary<string, string> desiredState)
    {
        var existing = await GetAsync(vehicleId);
        var updated = existing is null
            ? new VehicleShadowState(vehicleId, LastReported: null, IsOnline: false, DateTimeOffset.UtcNow, desiredState)
            : existing with { DesiredState = desiredState };

        await redis.GetDatabase().StringSetAsync(KeyFor(vehicleId), JsonSerializer.Serialize(updated));
        return updated;
    }
}
