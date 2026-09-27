using Npgsql;

namespace TelemetryProcessor;

/// <summary>
/// Idempotent schema bootstrap so the service also works against a bare
/// Postgres (e.g. local `dotnet run` outside Docker Compose), not only the
/// `deploy/postgres-init` script, which only runs on a brand-new volume.
/// </summary>
public static class SchemaBootstrap
{
    public static async Task EnsureSchemaAsync(NpgsqlDataSource dataSource, CancellationToken cancellationToken = default)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            create extension if not exists timescaledb;
            create table if not exists vehicle_telemetry (
                vehicle_id text not null,
                ts timestamptz not null,
                latitude double precision,
                longitude double precision,
                speed_kph double precision,
                battery_percent double precision,
                fuel_percent double precision,
                fault_codes text[]
            );
            select create_hypertable('vehicle_telemetry', 'ts', if_not_exists => true);
            create index if not exists idx_vehicle_telemetry_vehicle_id_ts
                on vehicle_telemetry (vehicle_id, ts desc);
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
