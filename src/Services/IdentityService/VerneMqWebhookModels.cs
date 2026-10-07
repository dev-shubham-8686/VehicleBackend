using System.Text.Json.Serialization;

namespace IdentityService;

// Field names/shapes match VerneMQ's webhook-plugin JSON payloads exactly
// (snake_case, VerneMQ's own vocabulary) — see
// https://docs.vernemq.com/plugin-development/webhookplugins and
// docs/ARCHITECTURE.md.

public sealed record AuthOnRegisterRequest(
    [property: JsonPropertyName("client_id")] string ClientId,
    [property: JsonPropertyName("username")] string? Username,
    [property: JsonPropertyName("password")] string? Password);

public sealed record TopicSubscription(
    [property: JsonPropertyName("topic")] string Topic,
    [property: JsonPropertyName("qos")] int Qos);

public sealed record AuthOnSubscribeRequest(
    [property: JsonPropertyName("client_id")] string ClientId,
    [property: JsonPropertyName("username")] string? Username,
    [property: JsonPropertyName("topics")] IReadOnlyList<TopicSubscription> Topics);

public sealed record AuthOnPublishRequest(
    [property: JsonPropertyName("client_id")] string ClientId,
    [property: JsonPropertyName("username")] string? Username,
    [property: JsonPropertyName("topic")] string Topic);
