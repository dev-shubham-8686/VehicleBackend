namespace IdentityService;

/// <summary>
/// Parses the client-id naming convention DeviceGateway and VehicleSimulator
/// use into "is this the gateway, or a specific vehicle" for the three
/// VerneMQ auth webhooks (see docs/ARCHITECTURE.md).
/// </summary>
public static class ClientIdentity
{
    private const string VehiclePrefix = "vehicle-";
    private const string GatewayPrefix = "device-gateway-";

    public static bool IsGateway(string clientId) => clientId.StartsWith(GatewayPrefix, StringComparison.Ordinal);

    public static string? TryGetVehicleId(string clientId) =>
        clientId.StartsWith(VehiclePrefix, StringComparison.Ordinal)
            ? clientId[VehiclePrefix.Length..]
            : null;
}
