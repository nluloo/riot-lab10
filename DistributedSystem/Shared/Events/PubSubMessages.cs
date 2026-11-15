using System.Text.Json;

namespace Shared.Events;

/// <summary>
/// Сообщение протокола Pub/Sub: одна строка JSON с операцией и данными.
/// Тот же стиль, что Request в ЛР №1…ЛР №5, поэтому на одном соединении достаточно одного читателя.
/// </summary>
public class PubSubMessage
{
    public string Operation { get; set; } = string.Empty;
    public JsonElement Data { get; set; }
}

/// <summary>Операции протокола Pub/Sub.</summary>
public static class PubSubOperations
{
    public const string Hello = "hello";
    public const string Subscribe = "subscribe";
    public const string Unsubscribe = "unsubscribe";
    public const string Info = "info";
    public const string Publish = "publish";

    /// Broker → подписчик; ответ подписчика — всегда Result.
    public const string Deliver = "deliver";

    public const string Result = "result";
}

/// <summary>Первое сообщение соединения: роль и имя.</summary>
public class HelloMessage
{
    /// publisher или subscriber
    public string Kind { get; set; } = "subscriber";

    public string Name { get; set; } = string.Empty;

    /// true — Broker сохраняет подписки и копит события в очереди, пока подписчика нет на связи.
    public bool Reconnectable { get; set; } = true;
}

public class SubscribeMessage
{
    public string Name { get; set; } = string.Empty;
    public List<string> EventTypes { get; set; } = new();
}

public class UnsubscribeMessage
{
    public string Name { get; set; } = string.Empty;
}

public class DeliveryMessage
{
    public DomainEvent? Event { get; set; }

    /// Больше 1 — повторная попытка.
    public int Attempt { get; set; } = 1;
}

public class DeliveryResultMessage
{
    public bool Success { get; set; }

    /// OK, UNAVAILABLE, ERROR, TIMEOUT, CONNECTION_LOST
    public string Status { get; set; } = string.Empty;

    public string Message { get; set; } = string.Empty;

    /// Время обработки подписчиком, мс.
    public int ProcessedMs { get; set; }

    /// Без него Broker принял бы запоздалый ответ за ответ на следующую доставку.
    public string EventId { get; set; } = string.Empty;
}

public class SubscribeResultMessage
{
    public bool Success { get; set; }

    /// OK, PARTIAL, UNKNOWN_SUBSCRIBER, UNKNOWN_EVENT_TYPE, NO_EVENT_TYPES, NO_CONNECTION
    public string Status { get; set; } = string.Empty;

    public string Message { get; set; } = string.Empty;
    public List<string> Types { get; set; } = new();
}

/// <summary>Результат публикации от одного Broker. Отказ брокера не отменяет уже сохранённую аренду.</summary>
public class PublishResultMessage
{
    public bool Success { get; set; }

    /// ACCEPTED, NO_SUBSCRIBERS, UNKNOWN_EVENT_TYPE, INVALID_EVENT, BROKER_UNAVAILABLE
    public string Status { get; set; } = string.Empty;

    public string Message { get; set; } = string.Empty;
    public string BrokerId { get; set; } = string.Empty;
    public int SubscribersForEvent { get; set; }
    public int DeliveredCount { get; set; }
    public int FailedCount { get; set; }
    public int QueuedCount { get; set; }
    public int PublishMs { get; set; }
}

public class BrokerInfoResultMessage
{
    public bool Success { get; set; }
    public string BrokerId { get; set; } = string.Empty;

    /// READY или BROKER_UNAVAILABLE
    public string Status { get; set; } = string.Empty;

    public string Message { get; set; } = string.Empty;
    public int OnlineSubscribers { get; set; }
    public int QueuedEvents { get; set; }

    /// «Logger: RentalCreated, CarReturned»
    public List<string> Subscriptions { get; set; } = new();

    /// «CarService: ONLINE, очередь»
    public List<string> Statuses { get; set; } = new();
}
