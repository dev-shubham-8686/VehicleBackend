namespace Cvp.Contracts.Alerts;

public enum AlertSeverity
{
    Info,
    Warning,
    Critical
}

public enum AlertType
{
    GeofenceBreach,
    Speeding,
    LowBattery,
    HarshBraking,
    FaultCodeRaised
}

public sealed record VehicleAlert(
    Guid AlertId,
    string VehicleId,
    AlertType Type,
    AlertSeverity Severity,
    string Message,
    DateTimeOffset RaisedAt);
