using Cvp.Common;
using Cvp.Common.Kafka;
using StackExchange.Redis;
using VehicleShadow;

var builder = WebApplication.CreateBuilder(args);
builder.AddCvpServiceDefaults("VehicleShadow");
builder.Services.AddCvpKafkaOptions(builder.Configuration);

var redisConnectionString = builder.Configuration.GetConnectionString("Redis") ?? "localhost:6379";
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConnectionString));
builder.Services.AddSingleton<ShadowStore>();
builder.Services.AddHostedService<TelemetryShadowConsumer>();

var app = builder.Build();
app.MapHealthChecks("/health");
app.MapGet("/", () => "VehicleShadow is running");

app.MapGet("/vehicles/{vehicleId}/shadow", async (string vehicleId, ShadowStore store) =>
{
    var shadow = await store.GetAsync(vehicleId);
    return shadow is null ? Results.NotFound() : Results.Ok(shadow);
});

app.MapPut("/vehicles/{vehicleId}/shadow/desired", async (
    string vehicleId,
    Dictionary<string, string> desiredState,
    ShadowStore store) =>
{
    var updated = await store.SetDesiredStateAsync(vehicleId, desiredState);
    return Results.Ok(updated);
});

app.Run();

// Makes the implicit top-level Program class visible to Cvp.IntegrationTests via WebApplicationFactory<Program>.
public partial class Program;
