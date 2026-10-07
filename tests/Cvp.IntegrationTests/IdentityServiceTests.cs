extern alias IdentityServiceAssembly;

using System.Net.Http.Json;
using Cvp.IntegrationTests.Infrastructure;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;

using IdentityServiceProgram = IdentityServiceAssembly::Program;
using RegisterDeviceRequest = IdentityServiceAssembly::IdentityService.RegisterDeviceRequest;
using RegisterDeviceResponse = IdentityServiceAssembly::IdentityService.RegisterDeviceResponse;

namespace Cvp.IntegrationTests;

/// <summary>
/// IdentityService owns device identity (so VerneMQ doesn't have to): it issues
/// a per-vehicle secret on registration, and answers VerneMQ's webhook-auth
/// calls for connect/publish/subscribe so vehicles can only touch their own
/// topics (see docs/ARCHITECTURE.md). Built test-first, exercised against a
/// real Postgres via Testcontainers.
/// </summary>
public sealed class IdentityServiceTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private const string GatewayUsername = "device-gateway";
    private const string GatewaySharedSecret = "test-gateway-secret";

    private readonly PostgresFixture _postgres;
    private WebApplicationFactory<IdentityServiceProgram> _factory = null!;
    private HttpClient _client = null!;

    public IdentityServiceTests(PostgresFixture postgres)
    {
        _postgres = postgres;
    }

    public Task InitializeAsync()
    {
        _factory = new WebApplicationFactory<IdentityServiceProgram>().WithWebHostBuilder(builder =>
        {
            builder.UseSetting("ConnectionStrings:Postgres", _postgres.ConnectionString);
            builder.UseSetting("Gateway:Username", GatewayUsername);
            builder.UseSetting("Gateway:SharedSecret", GatewaySharedSecret);
        });
        _client = _factory.CreateClient();
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        _client.Dispose();
        await _factory.DisposeAsync();
    }

    [Fact]
    public async Task RegisteringDevice_Twice_ReturnsSameSecret()
    {
        var first = await (await _client.PostAsJsonAsync("/devices/register", new RegisterDeviceRequest("veh-id-001")))
            .Content.ReadFromJsonAsync<RegisterDeviceResponse>();
        var second = await (await _client.PostAsJsonAsync("/devices/register", new RegisterDeviceRequest("veh-id-001")))
            .Content.ReadFromJsonAsync<RegisterDeviceResponse>();

        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.Equal(first!.DeviceSecret, second!.DeviceSecret);
    }

    [Fact]
    public async Task AuthOnRegister_CorrectVehicleCredentials_Allows()
    {
        var registered = await (await _client.PostAsJsonAsync("/devices/register", new RegisterDeviceRequest("veh-id-002")))
            .Content.ReadFromJsonAsync<RegisterDeviceResponse>();

        var response = await _client.PostAsJsonAsync("/webhooks/vernemq/auth-on-register", new
        {
            client_id = "vehicle-veh-id-002",
            username = "veh-id-002",
            password = registered!.DeviceSecret,
            clean_session = true
        });

        var json = await response.Content.ReadAsStringAsync();
        Assert.Contains("\"ok\"", json);
    }

    [Fact]
    public async Task AuthOnRegister_WrongPassword_Denies()
    {
        await _client.PostAsJsonAsync("/devices/register", new RegisterDeviceRequest("veh-id-003"));

        var response = await _client.PostAsJsonAsync("/webhooks/vernemq/auth-on-register", new
        {
            client_id = "vehicle-veh-id-003",
            username = "veh-id-003",
            password = "wrong-secret",
            clean_session = true
        });

        var json = await response.Content.ReadAsStringAsync();
        Assert.Contains("error", json);
    }

    [Fact]
    public async Task AuthOnRegister_GatewayCredentials_Allows()
    {
        var response = await _client.PostAsJsonAsync("/webhooks/vernemq/auth-on-register", new
        {
            client_id = "device-gateway-abc123",
            username = GatewayUsername,
            password = GatewaySharedSecret,
            clean_session = true
        });

        var json = await response.Content.ReadAsStringAsync();
        Assert.Contains("\"ok\"", json);
    }

    [Fact]
    public async Task AuthOnPublish_VehiclePublishingToOwnTopic_Allows()
    {
        var response = await _client.PostAsJsonAsync("/webhooks/vernemq/auth-on-publish", new
        {
            username = "veh-id-004",
            client_id = "vehicle-veh-id-004",
            qos = 0,
            topic = "vehicles/veh-id-004/telemetry",
            payload = "{}",
            retain = false
        });

        var json = await response.Content.ReadAsStringAsync();
        Assert.Contains("\"ok\"", json);
    }

    [Fact]
    public async Task AuthOnPublish_VehiclePublishingToAnotherVehiclesTopic_Denies()
    {
        var response = await _client.PostAsJsonAsync("/webhooks/vernemq/auth-on-publish", new
        {
            username = "veh-id-005",
            client_id = "vehicle-veh-id-005",
            qos = 0,
            topic = "vehicles/veh-id-999/telemetry",
            payload = "{}",
            retain = false
        });

        var json = await response.Content.ReadAsStringAsync();
        Assert.Contains("error", json);
    }

    [Fact]
    public async Task AuthOnSubscribe_VehicleSubscribingToWildcard_Denies()
    {
        var response = await _client.PostAsJsonAsync("/webhooks/vernemq/auth-on-subscribe", new
        {
            username = "veh-id-006",
            client_id = "vehicle-veh-id-006",
            topics = new[] { new { topic = "vehicles/+/telemetry", qos = 0 } }
        });

        var json = await response.Content.ReadAsStringAsync();
        Assert.Contains("error", json);
    }

    [Fact]
    public async Task AuthOnSubscribe_GatewaySubscribingToWildcard_Allows()
    {
        var response = await _client.PostAsJsonAsync("/webhooks/vernemq/auth-on-subscribe", new
        {
            username = GatewayUsername,
            client_id = "device-gateway-abc123",
            topics = new[] { new { topic = "vehicles/+/telemetry", qos = 0 } }
        });

        var json = await response.Content.ReadAsStringAsync();
        Assert.Contains("\"ok\"", json);
    }
}
