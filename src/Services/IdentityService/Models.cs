namespace IdentityService;

public sealed record RegisterDeviceRequest(string VehicleId);

public sealed record RegisterDeviceResponse(string VehicleId, string DeviceSecret);
