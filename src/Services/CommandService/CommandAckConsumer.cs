using Cvp.Common.Kafka;
using Cvp.Contracts;
using Cvp.Contracts.Commands;
using Microsoft.Extensions.Options;

namespace CommandService;

/// <summary>
/// Consumes delivery/execution acknowledgements DeviceGateway bridges from
/// vehicles and applies them to the stored command (see docs/ARCHITECTURE.md).
/// </summary>
public sealed class CommandAckConsumer : KafkaConsumerBackgroundService<VehicleCommandAck>
{
    private readonly CommandRepository _repository;

    public CommandAckConsumer(
        IOptions<KafkaOptions> kafkaOptions,
        CommandRepository repository,
        ILogger<CommandAckConsumer> logger)
        : base(kafkaOptions, KafkaTopics.CommandAck, logger)
    {
        _repository = repository;
    }

    protected override Task HandleAsync(string key, VehicleCommandAck message, CancellationToken cancellationToken)
        => _repository.UpdateStatusAsync(message, cancellationToken);
}
