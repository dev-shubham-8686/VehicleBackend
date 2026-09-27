using Cvp.Common;

var builder = WebApplication.CreateBuilder(args);
builder.AddCvpServiceDefaults("AlertingService");

// Phase 3 adds: a rules engine consuming telemetry/alerts from Kafka and a
// SignalR hub pushing alerts to connected dashboards (see docs/ARCHITECTURE.md).

var app = builder.Build();
app.MapHealthChecks("/health");
app.MapGet("/", () => "AlertingService is running");

app.Run();
