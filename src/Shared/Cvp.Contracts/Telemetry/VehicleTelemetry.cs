namespace Cvp.Contracts.Telemetry;

public sealed record VehicleTelemetry(
    string VehicleId,
    DateTimeOffset Timestamp,
    GeoLocation Location,
    double SpeedKph,
    double BatteryPercent,
    double? FuelPercent,
    IReadOnlyList<string> ActiveFaultCodes);

public sealed record GeoLocation(double Latitude, double Longitude);
