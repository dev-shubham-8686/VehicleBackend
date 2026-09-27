using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace Cvp.IntegrationTests.Infrastructure;

public sealed class VerneMqFixture : IAsyncLifetime
{
    private readonly IContainer _container = new ContainerBuilder("vernemq/vernemq:latest")
        .WithEnvironment("DOCKER_VERNEMQ_ACCEPT_EULA", "yes")
        .WithEnvironment("DOCKER_VERNEMQ_ALLOW_ANONYMOUS", "on")
        .WithEnvironment("DOCKER_VERNEMQ_LOG__CONSOLE__LEVEL", "warning")
        .WithPortBinding(1883, true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(1883))
        .Build();

    public string Host => _container.Hostname;
    public int Port => _container.GetMappedPublicPort(1883);

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}
