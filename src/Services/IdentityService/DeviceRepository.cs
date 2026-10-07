using System.Security.Cryptography;
using System.Text;
using Npgsql;

namespace IdentityService;

/// <summary>
/// Owns device (vehicle) identity: a per-vehicle secret, issued once on
/// registration and validated on every VerneMQ connect. Not a full PKI/mTLS
/// setup — username/password over MQTT CONNECT, the same shape AWS IoT's
/// custom-auth and Azure IoT Hub SAS tokens use without full device certs
/// (see docs/ARCHITECTURE.md).
/// </summary>
public sealed class DeviceRepository(NpgsqlDataSource dataSource)
{
    public async Task EnsureSchemaAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            create table if not exists devices (
                vehicle_id text primary key,
                device_secret text not null,
                created_at timestamptz not null default now()
            );
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>Idempotent: a vehicle re-registering (e.g. after a restart) gets back the same secret.</summary>
    public async Task<string> RegisterAsync(string vehicleId, CancellationToken cancellationToken = default)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);

        var existing = await GetSecretAsync(connection, vehicleId, cancellationToken);
        if (existing is not null)
        {
            return existing;
        }

        var secret = RandomNumberGenerator.GetHexString(32);

        await using (var insertCommand = connection.CreateCommand())
        {
            insertCommand.CommandText = """
                insert into devices (vehicle_id, device_secret)
                values ($1, $2)
                on conflict (vehicle_id) do nothing
                """;
            insertCommand.Parameters.Add(new NpgsqlParameter { Value = vehicleId });
            insertCommand.Parameters.Add(new NpgsqlParameter { Value = secret });
            await insertCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        // Handles the race where two concurrent registrations both missed the first SELECT.
        return (await GetSecretAsync(connection, vehicleId, cancellationToken))!;
    }

    public async Task<bool> ValidateAsync(string vehicleId, string secret, CancellationToken cancellationToken = default)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        var stored = await GetSecretAsync(connection, vehicleId, cancellationToken);
        if (stored is null)
        {
            return false;
        }

        return CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(stored),
            Encoding.UTF8.GetBytes(secret));
    }

    private static async Task<string?> GetSecretAsync(NpgsqlConnection connection, string vehicleId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "select device_secret from devices where vehicle_id = $1";
        command.Parameters.Add(new NpgsqlParameter { Value = vehicleId });
        return (string?)await command.ExecuteScalarAsync(cancellationToken);
    }
}
