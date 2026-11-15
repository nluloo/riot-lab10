using Broker;
using Shared.Events;

namespace Tests;

/// <summary>
/// Подписчик-заглушка: тот же SubscriberChannel, что получает Broker, но без TCP.
/// Даёт проверить маршрутизацию, отказ и очередь, не поднимая процесс подписчика.
/// </summary>
internal sealed class FakeSubscriber
{
    private readonly List<DomainEvent> _received = new();
    private readonly object _sync = new();
    private int _failuresLeft;

    public FakeSubscriber(string name)
    {
        Name = name;
        Channel = new SubscriberChannel(name) { Online = true, Reconnectable = true };
        Channel.Send = (message, _) => Task.FromResult(Answer(message));
    }

    public string Name { get; }

    public SubscriberChannel Channel { get; }

    /// false — подписчик отвечает UNAVAILABLE, как переведённый в режим отказа.
    public bool Available { get; set; } = true;

    /// бросить транспортное исключение — так проверяется CONNECTION_LOST.
    public bool DropsConnection { get; set; }

    public IReadOnlyList<DomainEvent> Received
    {
        get
        {
            lock (_sync)
            {
                return _received.ToList();
            }
        }
    }

    public int Processed { get; private set; }

    public int Failed { get; private set; }

    public void FailTimes(int times) => _failuresLeft = times;

    public void Subscribe(params string[] eventTypes) => Channel.EventTypes.AddRange(eventTypes);

    private DeliveryResultMessage Answer(DeliveryMessage message)
    {
        DomainEvent? evt = message.Event;

        if (evt == null)
        {
            return new DeliveryResultMessage { Status = "ERROR", Message = "подписчик получил пустое событие" };
        }

        if (!Available)
        {
            Failed++;

            return new DeliveryResultMessage
            {
                Status = "UNAVAILABLE",
                Message = "подписчик недоступен"
            };
        }

        if (DropsConnection)
        {
            Failed++;
            throw new PubSubTransportException("соединение с подписчиком оборвано");
        }

        if (_failuresLeft > 0)
        {
            _failuresLeft--;
            Failed++;

            return new DeliveryResultMessage { Status = "ERROR", Message = "ошибка обработки события" };
        }

        lock (_sync)
        {
            _received.Add(evt);
        }

        Processed++;

        return new DeliveryResultMessage { Success = true, Status = "OK", Message = "обработано", ProcessedMs = 10 };
    }
}

/// <summary>Пара Broker + подписчики для тестов механизма Pub/Sub без сети и без PostgreSQL.</summary>
internal sealed class PubSubRig
{
    public PubSubRig(int retryDelayMs = 0)
    {
        Journal = new EventJournal();
        Broker = new EventBroker("Broker-test", Journal, retryDelayMs);
    }

    public EventJournal Journal { get; }

    public EventBroker Broker { get; }

    public FakeSubscriber Add(string name, params string[] eventTypes)
    {
        var subscriber = new FakeSubscriber(name);
        subscriber.Subscribe(eventTypes);
        Broker.Subscriptions.Add(subscriber.Channel);
        return subscriber;
    }

    public DomainEvent Event(string eventType, int entityId = 1, int clientId = 7) => new()
    {
        EventType = eventType,
        EntityId = entityId,
        ClientId = clientId,
        Data = new Dictionary<string, object?> { ["registrationNumber"] = "1234 AB-7" }
    };

    public async Task<PublishResultMessage> PublishAsync(string eventType, int entityId = 1) =>
        await Broker.PublishAsync(Event(eventType, entityId), awaitDelivery: true);
}
