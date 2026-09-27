using System.Net;
using System.Net.Sockets;

namespace Cvp.IntegrationTests.Infrastructure;

public static class FreePort
{
    public static int GetTcpPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
