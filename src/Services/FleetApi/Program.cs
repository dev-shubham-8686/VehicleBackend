using Cvp.Common;

var builder = WebApplication.CreateBuilder(args);
builder.AddCvpServiceDefaults("FleetApi");

// Phase 5 adds: the aggregated backend-for-frontend API for dashboard/mobile
// clients, composing VehicleShadow, CommandService, and AlertingService (see docs/ARCHITECTURE.md).

var app = builder.Build();
app.MapHealthChecks("/health");
app.MapGet("/", () => "FleetApi is running");

app.Run();
