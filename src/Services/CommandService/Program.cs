using System.Text.Json.Serialization;
using CommandService;
using Cvp.Common;
using Cvp.Common.Kafka;
using Cvp.Contracts;
using Cvp.Contracts.Commands;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);
builder.AddCvpServiceDefaults("CommandService");
builder.Services.AddCvpKafkaProducer(builder.Configuration);
// Enums as strings on the public HTTP contract (e.g. "LockDoors", not 0) —
// the internal Kafka wire format (KafkaProducer/Consumer) is unaffected.
builder.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));

var connectionString = builder.Configuration.GetConnectionString("Postgres")
    ?? "Host=localhost;Database=cvp;Username=cvp;Password=cvp_dev_only";
builder.Services.AddSingleton(NpgsqlDataSource.Create(connectionString));
builder.Services.AddSingleton<CommandRepository>();
builder.Services.AddHostedService<CommandAckConsumer>();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var repository = scope.ServiceProvider.GetRequiredService<CommandRepository>();
    await StartupRetry.ExecuteAsync(() => repository.EnsureSchemaAsync(), scope.ServiceProvider.GetRequiredService<ILogger<Program>>());
}

app.MapHealthChecks("/health");
app.MapGet("/", () => "CommandService is running");

app.MapPost("/vehicles/{vehicleId}/commands", async (
    string vehicleId,
    CreateCommandRequest request,
    CommandRepository repository,
    KafkaProducer kafkaProducer) =>
{
    var command = new VehicleCommand(
        Guid.NewGuid(),
        vehicleId,
        request.Type,
        request.Parameters,
        DateTimeOffset.UtcNow,
        request.ExpiresAt);

    await repository.InsertAsync(command);
    await kafkaProducer.PublishAsync(KafkaTopics.CommandDispatch, vehicleId, command);

    var record = new VehicleCommandRecord(
        command.CommandId,
        command.VehicleId,
        command.Type,
        VehicleCommandStatus.Queued,
        command.IssuedAt,
        AcknowledgedAt: null,
        FailureReason: null);

    return Results.Created($"/vehicles/{vehicleId}/commands/{command.CommandId}", record);
});

app.MapGet("/vehicles/{vehicleId}/commands/{commandId:guid}", async (
    string vehicleId,
    Guid commandId,
    CommandRepository repository) =>
{
    var record = await repository.GetAsync(commandId);
    if (record is null || !string.Equals(record.VehicleId, vehicleId, StringComparison.Ordinal))
    {
        return Results.NotFound();
    }

    return Results.Ok(record);
});

app.Run();

// Makes the implicit top-level Program class visible to Cvp.IntegrationTests via WebApplicationFactory<Program>.
public partial class Program;
