using Cvp.Common;
using VehicleSimulator;

var builder = Host.CreateApplicationBuilder(args);
builder.AddCvpServiceDefaults("VehicleSimulator");
builder.Services.Configure<FleetSimulatorOptions>(builder.Configuration.GetSection(FleetSimulatorOptions.SectionName));
builder.Services.AddHostedService<FleetSimulatorWorker>();

var host = builder.Build();
host.Run();
