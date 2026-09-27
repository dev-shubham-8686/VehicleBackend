using Cvp.Contracts.Commands;
using Npgsql;

namespace CommandService;

/// <summary>
/// Postgres-backed store for issued commands. No ORM/migration framework yet
/// (see docs/ARCHITECTURE.md) — <see cref="EnsureSchemaAsync"/> is an idempotent
/// bootstrap suitable for this phase's scope, run once at startup.
/// </summary>
public sealed class CommandRepository(NpgsqlDataSource dataSource)
{
    public async Task EnsureSchemaAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            create table if not exists vehicle_commands (
                command_id uuid primary key,
                vehicle_id text not null,
                type integer not null,
                issued_at timestamptz not null,
                expires_at timestamptz null,
                status integer not null,
                acknowledged_at timestamptz null,
                failure_reason text null
            );
            create index if not exists idx_vehicle_commands_vehicle_id on vehicle_commands (vehicle_id);
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task InsertAsync(VehicleCommand command, CancellationToken cancellationToken = default)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = """
            insert into vehicle_commands (command_id, vehicle_id, type, issued_at, expires_at, status)
            values ($1, $2, $3, $4, $5, $6)
            """;
        dbCommand.Parameters.Add(new NpgsqlParameter { Value = command.CommandId });
        dbCommand.Parameters.Add(new NpgsqlParameter { Value = command.VehicleId });
        dbCommand.Parameters.Add(new NpgsqlParameter { Value = (int)command.Type });
        dbCommand.Parameters.Add(new NpgsqlParameter { Value = command.IssuedAt });
        dbCommand.Parameters.Add(new NpgsqlParameter { Value = (object?)command.ExpiresAt ?? DBNull.Value });
        dbCommand.Parameters.Add(new NpgsqlParameter { Value = (int)VehicleCommandStatus.Queued });

        await dbCommand.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<VehicleCommandRecord?> GetAsync(Guid commandId, CancellationToken cancellationToken = default)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = """
            select command_id, vehicle_id, type, status, issued_at, acknowledged_at, failure_reason
            from vehicle_commands
            where command_id = $1
            """;
        dbCommand.Parameters.Add(new NpgsqlParameter { Value = commandId });

        await using var reader = await dbCommand.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new VehicleCommandRecord(
            reader.GetGuid(0),
            reader.GetString(1),
            (VehicleCommandType)reader.GetInt32(2),
            (VehicleCommandStatus)reader.GetInt32(3),
            reader.GetFieldValue<DateTimeOffset>(4),
            reader.IsDBNull(5) ? null : reader.GetFieldValue<DateTimeOffset>(5),
            reader.IsDBNull(6) ? null : reader.GetString(6));
    }

    public async Task UpdateStatusAsync(VehicleCommandAck ack, CancellationToken cancellationToken = default)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = """
            update vehicle_commands
            set status = $1, acknowledged_at = $2, failure_reason = $3
            where command_id = $4
            """;
        dbCommand.Parameters.Add(new NpgsqlParameter { Value = (int)ack.Status });
        dbCommand.Parameters.Add(new NpgsqlParameter { Value = ack.AcknowledgedAt });
        dbCommand.Parameters.Add(new NpgsqlParameter { Value = (object?)ack.FailureReason ?? DBNull.Value });
        dbCommand.Parameters.Add(new NpgsqlParameter { Value = ack.CommandId });

        await dbCommand.ExecuteNonQueryAsync(cancellationToken);
    }
}
