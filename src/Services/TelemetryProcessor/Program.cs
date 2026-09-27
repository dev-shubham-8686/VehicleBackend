using Cvp.Common;
using Cvp.Common.Kafka;
using Npgsql;
using TelemetryProcessor;

var builder = Host.CreateApplicationBuilder(args);
builder.AddCvpServiceDefaults("TelemetryProcessor");
builder.Services.AddCvpKafkaOptions(builder.Configuration);

var connectionString = builder.Configuration.GetConnectionString("Postgres")
    ?? "Host=localhost;Database=cvp;Username=cvp;Password=cvp_dev_only";
var dataSource = NpgsqlDataSource.Create(connectionString);
builder.Services.AddSingleton(dataSource);

builder.Services.AddHostedService<TelemetryIngestConsumer>();

var host = builder.Build();
await StartupRetry.ExecuteAsync(
    () => SchemaBootstrap.EnsureSchemaAsync(dataSource),
    host.Services.GetRequiredService<ILogger<Program>>());
host.Run();
