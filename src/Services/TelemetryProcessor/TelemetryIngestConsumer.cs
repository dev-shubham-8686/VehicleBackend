using Cvp.Common.Kafka;
using Cvp.Contracts;
using Cvp.Contracts.Telemetry;
using Microsoft.Extensions.Options;
using Npgsql;

namespace TelemetryProcessor;

/// <summary>
/// Consumes raw telemetry from Kafka and persists it into the TimescaleDB
/// hypertable created by deploy/postgres-init/001_telemetry.sql. Alert-rule
/// evaluation lands here in Phase 3 (see docs/ARCHITECTURE.md).
/// </summary>
public sealed class TelemetryIngestConsumer : KafkaConsumerBackgroundService<VehicleTelemetry>
{
    private readonly NpgsqlDataSource _dataSource;

    public TelemetryIngestConsumer(
        IOptions<KafkaOptions> kafkaOptions,
        NpgsqlDataSource dataSource,
        ILogger<TelemetryIngestConsumer> logger)
        : base(kafkaOptions, KafkaTopics.TelemetryRaw, logger)
    {
        _dataSource = dataSource;
    }

    protected override async Task HandleAsync(string key, VehicleTelemetry message, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            insert into vehicle_telemetry
                (vehicle_id, ts, latitude, longitude, speed_kph, battery_percent, fuel_percent, fault_codes)
            values
                ($1, $2, $3, $4, $5, $6, $7, $8)
            """;
        command.Parameters.Add(new NpgsqlParameter { Value = message.VehicleId });
        command.Parameters.Add(new NpgsqlParameter { Value = message.Timestamp });
        command.Parameters.Add(new NpgsqlParameter { Value = message.Location.Latitude });
        command.Parameters.Add(new NpgsqlParameter { Value = message.Location.Longitude });
        command.Parameters.Add(new NpgsqlParameter { Value = message.SpeedKph });
        command.Parameters.Add(new NpgsqlParameter { Value = message.BatteryPercent });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)message.FuelPercent ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { Value = message.ActiveFaultCodes.ToArray() });

        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
