namespace Cvp.Contracts;

public static class KafkaTopics
{
    public const string TelemetryRaw = "cvp.telemetry.raw";
    public const string CommandDispatch = "cvp.commands.dispatch";
    public const string CommandAck = "cvp.commands.ack";
    public const string Alerts = "cvp.alerts";
    public const string OtaStatus = "cvp.ota.status";
}
