extern alias CommandServiceAssembly;

using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Cvp.Contracts;
using Cvp.Contracts.Commands;
using Cvp.IntegrationTests.Infrastructure;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;

using CommandServiceProgram = CommandServiceAssembly::Program;

namespace Cvp.IntegrationTests;

/// <summary>
/// CommandService's contract, exercised against real Postgres and Kafka containers
/// (no mocks): issuing a command persists it as Queued and dispatches it onto Kafka;
/// a command-ack arriving on cvp.commands.ack asynchronously updates its status.
/// </summary>
public sealed class CommandServiceTests : IClassFixture<PostgresFixture>, IClassFixture<KafkaFixture>, IAsyncLifetime
{
    // Matches CommandService's ConfigureHttpJsonOptions: enums as strings on the wire.
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }
    };

    private readonly PostgresFixture _postgres;
    private readonly KafkaFixture _kafka;
    private WebApplicationFactory<CommandServiceProgram> _factory = null!;
    private HttpClient _client = null!;
    private TestKafkaPublisher _publisher = null!;

    public CommandServiceTests(PostgresFixture postgres, KafkaFixture kafka)
    {
        _postgres = postgres;
        _kafka = kafka;
    }

    public Task InitializeAsync()
    {
        _factory = new WebApplicationFactory<CommandServiceProgram>().WithWebHostBuilder(builder =>
        {
            builder.UseSetting("ConnectionStrings:Postgres", _postgres.ConnectionString);
            builder.UseSetting("Kafka:BootstrapServers", _kafka.BootstrapServers);
            builder.UseSetting("Kafka:ConsumerGroupId", $"command-service-test-{Guid.NewGuid()}");
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
    public async Task IssuingCommand_PersistsItAsQueued()
    {
        var response = await _client.PostAsJsonAsync(
            "/vehicles/veh-001/commands",
            new CreateCommandRequest(VehicleCommandType.LockDoors, Parameters: null, ExpiresAt: null),
            JsonOptions);

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);

        var created = await response.Content.ReadFromJsonAsync<VehicleCommandRecord>(JsonOptions);
        Assert.NotNull(created);
        Assert.Equal("veh-001", created!.VehicleId);
        Assert.Equal(VehicleCommandType.LockDoors, created.Type);
        Assert.Equal(VehicleCommandStatus.Queued, created.Status);

        var fetched = await _client.GetFromJsonAsync<VehicleCommandRecord>($"/vehicles/veh-001/commands/{created.CommandId}", JsonOptions);
        Assert.NotNull(fetched);
        Assert.Equal(VehicleCommandStatus.Queued, fetched!.Status);
    }

    [Fact]
    public async Task GetCommand_UnknownId_ReturnsNotFound()
    {
        var response = await _client.GetAsync($"/vehicles/veh-999/commands/{Guid.NewGuid()}");
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task CommandAckFromKafka_UpdatesStoredStatusToAcknowledged()
    {
        var createResponse = await _client.PostAsJsonAsync(
            "/vehicles/veh-002/commands",
            new CreateCommandRequest(VehicleCommandType.UnlockDoors, Parameters: null, ExpiresAt: null),
            JsonOptions);
        var created = await createResponse.Content.ReadFromJsonAsync<VehicleCommandRecord>(JsonOptions);
        Assert.NotNull(created);

        var ack = new VehicleCommandAck(
            created!.CommandId,
            "veh-002",
            VehicleCommandStatus.Acknowledged,
            DateTimeOffset.UtcNow,
            FailureReason: null);
        await _publisher.PublishAsync(KafkaTopics.CommandAck, ack.VehicleId, ack);

        var updated = await Polling.UntilAsync(
            () => _client.GetFromJsonAsync<VehicleCommandRecord>($"/vehicles/veh-002/commands/{created.CommandId}", JsonOptions),
            record => record?.Status == VehicleCommandStatus.Acknowledged,
            TimeSpan.FromSeconds(20));

        Assert.NotNull(updated);
        Assert.Equal(VehicleCommandStatus.Acknowledged, updated!.Status);
        Assert.NotNull(updated.AcknowledgedAt);
    }
}
