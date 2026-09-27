using Cvp.Common;

var builder = WebApplication.CreateBuilder(args);
builder.AddCvpServiceDefaults("OtaService");

// Phase 4 adds: update-campaign management (targeting, staged rollout,
// rollback) with per-vehicle status tracked via CommandService (see docs/ARCHITECTURE.md).

var app = builder.Build();
app.MapHealthChecks("/health");
app.MapGet("/", () => "OtaService is running");

app.Run();
