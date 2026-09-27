extern alias TelemetryProcessorAssembly;

using Cvp.Common.Kafka;
using Cvp.Contracts;
using Cvp.Contracts.Telemetry;
using Cvp.IntegrationTests.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;

using TelemetryProcessorAssembly::TelemetryProcessor;

namespace Cvp.IntegrationTests;

/// <summary>
/// Characterizes the Phase 1 ingestion sink already running in production:
/// a telemetry message on Kafka must be persisted into the
/// <c>vehicle_telemetry</c> hypertable by <see cref="TelemetryIngestConsumer"/>.
/// </summary>
public sealed class TelemetryIngestConsumerTests : IClassFixture<PostgresFixture>, IClassFixture<KafkaFixture>, IAsyncLifetime
{
    private readonly PostgresFixture _postgres;
    private readonly KafkaFixture _kafka;
    private NpgsqlDataSource _dataSource = null!;
    private TelemetryIngestConsumer _consumer = null!;
    private TestKafkaPublisher _publisher = null!;

    public TelemetryIngestConsumerTests(PostgresFixture postgres, KafkaFixture kafka)
    {
        _postgres = postgres;
        _kafka = kafka;
    }

    public async Task InitializeAsync()
    {
        _dataSource = NpgsqlDataSource.Create(_postgres.ConnectionString);
        await SchemaBootstrap.EnsureSchemaAsync(_dataSource);

        var kafkaOptions = Options.Create(new KafkaOptions
        {
            BootstrapServers = _kafka.BootstrapServers,
            ConsumerGroupId = $"telemetry-processor-test-{Guid.NewGuid()}"
        });
        _consumer = new TelemetryIngestConsumer(kafkaOptions, _dataSource, NullLogger<TelemetryIngestConsumer>.Instance);
        await _consumer.StartAsync(CancellationToken.None);

        _publisher = new TestKafkaPublisher(_kafka.BootstrapServers);
    }

    public async Task DisposeAsync()
    {
        await _consumer.StopAsync(CancellationToken.None);
        await _publisher.DisposeAsync();
        await _dataSource.DisposeAsync();
    }

    [Fact]
    public async Task TelemetryPublishedToKafka_IsPersistedToTimescaleDb()
    {
        var telemetry = new VehicleTelemetry(
            "veh-ingest-001",
            DateTimeOffset.UtcNow,
            new GeoLocation(48.85, 2.35),
            SpeedKph: 88.0,
            BatteryPercent: 61.5,
            FuelPercent: null,
            ActiveFaultCodes: new[] { "P0420" });

        await _publisher.PublishAsync(KafkaTopics.TelemetryRaw, telemetry.VehicleId, telemetry);

        var row = await Polling.UntilAsync(
            () => FetchLatestAsync(telemetry.VehicleId),
            r => r is not null,
            TimeSpan.FromSeconds(20));

        Assert.NotNull(row);
        Assert.Equal(telemetry.SpeedKph, row!.Value.SpeedKph);
        Assert.Equal(telemetry.BatteryPercent, row.Value.BatteryPercent);
        Assert.Equal(telemetry.ActiveFaultCodes, row.Value.FaultCodes);
    }

    private async Task<(double SpeedKph, double BatteryPercent, string[] FaultCodes)?> FetchLatestAsync(string vehicleId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select speed_kph, battery_percent, fault_codes
            from vehicle_telemetry
            where vehicle_id = $1
            order by ts desc
            limit 1
            """;
        command.Parameters.Add(new NpgsqlParameter { Value = vehicleId });

        await using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return null;
        }

        return (reader.GetDouble(0), reader.GetDouble(1), reader.GetFieldValue<string[]>(2));
    }
}
