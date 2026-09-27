extern alias VehicleShadowAssembly;

using System.Net;
using System.Net.Http.Json;
using Cvp.Contracts;
using Cvp.Contracts.Shadow;
using Cvp.Contracts.Telemetry;
using Cvp.IntegrationTests.Infrastructure;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;

using VehicleShadowProgram = VehicleShadowAssembly::Program;

namespace Cvp.IntegrationTests;

/// <summary>
/// VehicleShadow's contract, exercised against real Redis and Kafka containers:
/// telemetry published on cvp.telemetry.raw asynchronously updates the reported
/// state; desired-state updates go through a REST PUT and are read back via GET.
/// </summary>
public sealed class VehicleShadowTests : IClassFixture<RedisFixture>, IClassFixture<KafkaFixture>, IAsyncLifetime
{
    private readonly RedisFixture _redis;
    private readonly KafkaFixture _kafka;
    private WebApplicationFactory<VehicleShadowProgram> _factory = null!;
    private HttpClient _client = null!;
    private TestKafkaPublisher _publisher = null!;

    public VehicleShadowTests(RedisFixture redis, KafkaFixture kafka)
    {
        _redis = redis;
        _kafka = kafka;
    }

    public Task InitializeAsync()
    {
        _factory = new WebApplicationFactory<VehicleShadowProgram>().WithWebHostBuilder(builder =>
        {
            builder.UseSetting("ConnectionStrings:Redis", _redis.ConnectionString);
            builder.UseSetting("Kafka:BootstrapServers", _kafka.BootstrapServers);
            builder.UseSetting("Kafka:ConsumerGroupId", $"vehicle-shadow-test-{Guid.NewGuid()}");
        });
        _client = _factory.CreateClient();
        _publisher = new TestKafkaPublisher(_kafka.BootstrapServers);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        _client.Dispose();
        await _publisher.DisposeAsync();
        await _factory.DisposeAsync();
    }

    [Fact]
    public async Task GetShadow_UnknownVehicle_ReturnsNotFound()
    {
        var response = await _client.GetAsync("/vehicles/unknown-vehicle/shadow");
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task TelemetryFromKafka_UpdatesReportedStateAndMarksVehicleOnline()
    {
        var telemetry = new VehicleTelemetry(
            "veh-shadow-001",
            DateTimeOffset.UtcNow,
            new GeoLocation(37.77, -122.42),
            SpeedKph: 55.5,
            BatteryPercent: 87.0,
            FuelPercent: null,
            ActiveFaultCodes: Array.Empty<string>());

        await _publisher.PublishAsync(KafkaTopics.TelemetryRaw, telemetry.VehicleId, telemetry);

        var shadow = await Polling.UntilAsync(
            () => _client.GetFromJsonAsync<VehicleShadowState>($"/vehicles/{telemetry.VehicleId}/shadow"),
            state => state is not null,
            TimeSpan.FromSeconds(20));

        Assert.NotNull(shadow);
        Assert.True(shadow!.IsOnline);
        Assert.NotNull(shadow.LastReported);
        Assert.Equal(telemetry.SpeedKph, shadow.LastReported!.SpeedKph);
        Assert.Equal(telemetry.BatteryPercent, shadow.LastReported.BatteryPercent);
    }

    [Fact]
    public async Task PutDesiredState_IsReflectedOnSubsequentGet()
    {
        const string vehicleId = "veh-shadow-002";
        await _publisher.PublishAsync(KafkaTopics.TelemetryRaw, vehicleId, new VehicleTelemetry(
            vehicleId, DateTimeOffset.UtcNow, new GeoLocation(0, 0), 0, 100, null, Array.Empty<string>()));

        await Polling.UntilAsync(
            () => _client.GetFromJsonAsync<VehicleShadowState>($"/vehicles/{vehicleId}/shadow"),
            state => state is not null,
            TimeSpan.FromSeconds(20));

        var desired = new Dictionary<string, string> { ["climate"] = "on" };
        var putResponse = await _client.PutAsJsonAsync($"/vehicles/{vehicleId}/shadow/desired", desired);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        var shadow = await _client.GetFromJsonAsync<VehicleShadowState>($"/vehicles/{vehicleId}/shadow");
        Assert.NotNull(shadow);
        Assert.NotNull(shadow!.DesiredState);
        Assert.Equal("on", shadow.DesiredState!["climate"]);
    }
}
