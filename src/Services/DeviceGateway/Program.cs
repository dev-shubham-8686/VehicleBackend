using Cvp.Common;
using Cvp.Common.Kafka;
using DeviceGateway;

var builder = WebApplication.CreateBuilder(args);
builder.AddCvpServiceDefaults("DeviceGateway");
builder.Services.AddCvpKafkaProducer(builder.Configuration);

// Registered as its own singleton (not just via AddHostedService) so
// CommandDispatchConsumer can inject the same connected MQTT client instance.
builder.Services.AddSingleton<MqttBridgeHostedService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<MqttBridgeHostedService>());
builder.Services.AddHostedService<CommandDispatchConsumer>();

var app = builder.Build();
app.MapHealthChecks("/health");
app.MapGet("/", () => "DeviceGateway is running");

app.Run();
