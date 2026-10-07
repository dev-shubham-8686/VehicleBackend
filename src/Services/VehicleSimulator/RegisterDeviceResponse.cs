namespace VehicleSimulator;

/// <summary>Mirrors IdentityService's POST /devices/register response shape.</summary>
public sealed record RegisterDeviceResponse(string VehicleId, string DeviceSecret);
