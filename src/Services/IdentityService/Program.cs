using System.Security.Cryptography;
using System.Text;
using Cvp.Common;
using IdentityService;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);
builder.AddCvpServiceDefaults("IdentityService");

var connectionString = builder.Configuration.GetConnectionString("Postgres")
    ?? "Host=localhost;Database=cvp;Username=cvp;Password=cvp_dev_only";
builder.Services.AddSingleton(NpgsqlDataSource.Create(connectionString));
builder.Services.AddSingleton<DeviceRepository>();

var gatewayUsername = builder.Configuration["Gateway:Username"] ?? "device-gateway";
var gatewaySharedSecret = builder.Configuration["Gateway:SharedSecret"] ?? "dev-gateway-shared-secret-change-me";

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var repository = scope.ServiceProvider.GetRequiredService<DeviceRepository>();
    await StartupRetry.ExecuteAsync(() => repository.EnsureSchemaAsync(), scope.ServiceProvider.GetRequiredService<ILogger<Program>>());
}

app.MapHealthChecks("/health");
app.MapGet("/", () => "IdentityService is running");

// Device provisioning: a vehicle calls this once at startup to obtain its
// MQTT credentials (see VehicleSimulator's FleetSimulatorWorker).
app.MapPost("/devices/register", async (RegisterDeviceRequest request, DeviceRepository repository) =>
{
    var secret = await repository.RegisterAsync(request.VehicleId);
    return Results.Ok(new RegisterDeviceResponse(request.VehicleId, secret));
});

// VerneMQ webhook-auth plugin (vmq_webhooks) calls these three on every
// connect/publish/subscribe — see docs/ARCHITECTURE.md for why webhooks
// rather than vmq_diversity: policy stays in C#, VerneMQ stays a dumb broker.
app.MapPost("/webhooks/vernemq/auth-on-register", async (AuthOnRegisterRequest request, DeviceRepository repository) =>
{
    if (ClientIdentity.IsGateway(request.ClientId))
    {
        return FixedTimeEquals(request.Username, gatewayUsername) && FixedTimeEquals(request.Password, gatewaySharedSecret)
            ? Allow()
            : Deny();
    }

    var vehicleId = ClientIdentity.TryGetVehicleId(request.ClientId);
    if (vehicleId is null || request.Password is null || request.Username != vehicleId)
    {
        return Deny();
    }

    return await repository.ValidateAsync(vehicleId, request.Password) ? Allow() : Deny();
});

app.MapPost("/webhooks/vernemq/auth-on-publish", (AuthOnPublishRequest request) =>
{
    if (ClientIdentity.IsGateway(request.ClientId))
    {
        return Allow();
    }

    var vehicleId = ClientIdentity.TryGetVehicleId(request.ClientId);
    if (vehicleId is null)
    {
        return Deny();
    }

    var ownTopics = new[] { $"vehicles/{vehicleId}/telemetry", $"vehicles/{vehicleId}/commands/ack" };
    return ownTopics.Contains(request.Topic) ? Allow() : Deny();
});

app.MapPost("/webhooks/vernemq/auth-on-subscribe", (AuthOnSubscribeRequest request) =>
{
    if (ClientIdentity.IsGateway(request.ClientId))
    {
        return Allow();
    }

    var vehicleId = ClientIdentity.TryGetVehicleId(request.ClientId);
    if (vehicleId is null)
    {
        return Deny();
    }

    var ownTopic = $"vehicles/{vehicleId}/commands";
    return request.Topics.All(t => t.Topic == ownTopic) ? Allow() : Deny();
});

app.Run();

static bool FixedTimeEquals(string? actual, string expected) =>
    actual is not null && CryptographicOperations.FixedTimeEquals(
        Encoding.UTF8.GetBytes(actual),
        Encoding.UTF8.GetBytes(expected));

static IResult Allow() => Results.Ok(new { result = "ok" });

static IResult Deny() => Results.Ok(new { result = new { error = "not_allowed" } });

// Makes the implicit top-level Program class visible to Cvp.IntegrationTests via WebApplicationFactory<Program>.
public partial class Program;
