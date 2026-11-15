using System.Text.Json;

namespace Shared.Events;

/// <summary>
/// Единые правила сериализации протокола Pub/Sub: одно сообщение = одна строка JSON.
/// Тот же стиль, что в ЛР №1…ЛР №5 для сообщений клиент → сервер.
/// </summary>
public static class PubSubJson
{
    private static readonly JsonSerializerOptions WriteOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private static readonly JsonSerializerOptions ReadOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public static string Serialize(object message) => JsonSerializer.Serialize(message, WriteOptions);

    /// Бросает JsonException при повреждённой строке — вызывающий обязан её перехватить.
    public static T? Deserialize<T>(string json) => JsonSerializer.Deserialize<T>(json, ReadOptions);

    public static PubSubMessage Message(string operation, object data) => new()
    {
        Operation = operation,
        Data = JsonSerializer.SerializeToElement(data, WriteOptions)
    };

    public static JsonElement ToElement(object data) => JsonSerializer.SerializeToElement(data, WriteOptions);

    public static T? Payload<T>(PubSubMessage message) =>
        message.Data.ValueKind == JsonValueKind.Undefined || message.Data.ValueKind == JsonValueKind.Null
            ? default
            : message.Data.Deserialize<T>(ReadOptions);
}
