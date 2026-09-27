using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Cvp.Common.Kafka;

public static class KafkaServiceCollectionExtensions
{
    public static IServiceCollection AddCvpKafkaOptions(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<KafkaOptions>(configuration.GetSection(KafkaOptions.SectionName));
        return services;
    }

    public static IServiceCollection AddCvpKafkaProducer(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddCvpKafkaOptions(configuration);
        services.AddSingleton<KafkaProducer>();
        return services;
    }
}
