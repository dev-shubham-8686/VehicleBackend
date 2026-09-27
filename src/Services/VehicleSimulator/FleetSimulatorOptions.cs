namespace VehicleSimulator;

public sealed class FleetSimulatorOptions
{
    public const string SectionName = "Simulator";

    public int FleetSize { get; set; } = 20;
    public string BrokerHost { get; set; } = "localhost";
    public int BrokerPort { get; set; } = 1883;
    public int PublishIntervalSeconds { get; set; } = 5;
}
