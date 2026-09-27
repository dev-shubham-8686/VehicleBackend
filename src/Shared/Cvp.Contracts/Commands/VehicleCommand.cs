namespace Cvp.Contracts.Commands;

public enum VehicleCommandType
{
    LockDoors,
    UnlockDoors,
    StartClimate,
    StopClimate,
    FlashLights,
    InstallUpdate
}

public enum VehicleCommandStatus
{
    Queued,
    Sent,
    Acknowledged,
    Failed,
    Expired
}

public sealed record VehicleCommand(
    Guid CommandId,
    string VehicleId,
    VehicleCommandType Type,
    IReadOnlyDictionary<string, string>? Parameters,
    DateTimeOffset IssuedAt,
    DateTimeOffset? ExpiresAt);

public sealed record VehicleCommandAck(
    Guid CommandId,
    string VehicleId,
    VehicleCommandStatus Status,
    DateTimeOffset AcknowledgedAt,
    string? FailureReason);

/// <summary>CommandService's POST /vehicles/{vehicleId}/commands request body.</summary>
public sealed record CreateCommandRequest(
    VehicleCommandType Type,
    IReadOnlyDictionary<string, string>? Parameters,
    DateTimeOffset? ExpiresAt);

/// <summary>The persisted, queryable view of a command — CommandService's API response shape.</summary>
public sealed record VehicleCommandRecord(
    Guid CommandId,
    string VehicleId,
    VehicleCommandType Type,
    VehicleCommandStatus Status,
    DateTimeOffset IssuedAt,
    DateTimeOffset? AcknowledgedAt,
    string? FailureReason);
