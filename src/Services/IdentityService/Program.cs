using Cvp.Common;

var builder = WebApplication.CreateBuilder(args);
builder.AddCvpServiceDefaults("IdentityService");

// Phase 5 adds: device certificate issuance/validation and OAuth2/OIDC user
// auth for dashboard and dealer-portal clients (see docs/ARCHITECTURE.md).

var app = builder.Build();
app.MapHealthChecks("/health");
app.MapGet("/", () => "IdentityService is running");

app.Run();
