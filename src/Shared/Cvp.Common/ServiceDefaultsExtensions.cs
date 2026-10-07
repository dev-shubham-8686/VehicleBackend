using Cvp.Common.Observability;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Serilog;

namespace Cvp.Common;

/// <summary>
/// Cross-cutting wiring (logging, tracing, health checks) shared by every CVP service
/// so each service's Program.cs only declares what makes it different.
/// </summary>
public static class ServiceDefaultsExtensions
{
    public static IHostApplicationBuilder AddCvpServiceDefaults(this IHostApplicationBuilder builder, string serviceName)
    {
        builder.Services.AddSerilog((_, loggerConfig) => loggerConfig
            .ReadFrom.Configuration(builder.Configuration)
            .Enrich.WithProperty("Service", serviceName)
            .WriteTo.Console());

        var otlpEndpoint = builder.Configuration["Otel:Endpoint"];

        builder.Services.AddOpenTelemetry()
            .ConfigureResource(resource => resource.AddService(serviceName))
            .WithTracing(tracing =>
            {
                tracing.AddAspNetCoreInstrumentation().AddHttpClientInstrumentation();
                // Confluent.Kafka and MQTTnet have no official OTel instrumentation,
                // so Kafka produce/consume is traced manually (see CvpTelemetry);
                // Npgsql ships its own ActivitySource, just needs registering.
                tracing.AddSource(CvpTelemetry.SourceName);
                tracing.AddNpgsql();
                if (!string.IsNullOrWhiteSpace(otlpEndpoint))
                {
                    tracing.AddOtlpExporter(o => o.Endpoint = new Uri(otlpEndpoint));
                }
            })
            .WithMetrics(metrics =>
            {
                metrics.AddAspNetCoreInstrumentation().AddHttpClientInstrumentation().AddRuntimeInstrumentation();
                metrics.AddMeter(CvpTelemetry.SourceName);
                metrics.AddNpgsqlInstrumentation();
                if (!string.IsNullOrWhiteSpace(otlpEndpoint))
                {
                    metrics.AddOtlpExporter(o => o.Endpoint = new Uri(otlpEndpoint));
                }
            });

        builder.Services.AddHealthChecks();

        return builder;
    }
}
