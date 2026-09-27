using Cvp.Common;
using Cvp.Common.Kafka;
using DeviceGateway;

var builder = WebApplication.CreateBuilder(args);
builder.AddCvpServiceDefaults("DeviceGateway");
builder.Services.AddCvpKafkaProducer(builder.Configuration);
builder.Services.AddHostedService<MqttBridgeHostedService>();

var app = builder.Build();
app.MapHealthChecks("/health");
app.MapGet("/", () => "DeviceGateway is running");

app.Run();
