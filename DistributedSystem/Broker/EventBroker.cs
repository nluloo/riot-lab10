using Shared.Events;

namespace Broker;

/// <summary>
/// Ядро Broker: хранит подписки, определяет получателей события, доставляет асинхронно,
/// копит события для недоступных подписчиков и продолжает работу при отказе отдельного Subscriber.
/// </summary>
public class EventBroker
{
    private readonly EventJournal _journal;
    private readonly DeliveryManager _delivery;

    public EventBroker(string brokerId, EventJournal journal, int retryDelayMs = 1500, int deliveryTimeoutMs = 10000)
    {
        BrokerId = brokerId;
        Subscriptions = new SubscriptionRegistry();
        Queue = new EventQueue();
        _journal = journal;
        _delivery = new DeliveryManager(brokerId, journal, retryDelayMs, deliveryTimeoutMs);
    }

    public string BrokerId { get; }

    public SubscriptionRegistry Subscriptions { get; }

    public EventQueue Queue { get; }

    /// Переподключение с прежним именем сохраняет подписки — на этом держится повторная доставка.
    public SubscriberChannel AcceptConnection(string name, bool reconnectable)
    {
        SubscriberChannel? previous = Subscriptions.Find(name);
        var channel = new SubscriberChannel(name)
        {
            Online = true,
            Reconnectable = previous?.Reconnectable ?? reconnectable
        };

        foreach (string eventType in previous?.EventTypes ?? new List<string>())
        {
            channel.EventTypes.Add(eventType);
        }

        Subscriptions.Add(channel);
        _journal.Write(BrokerId, channel.EventTypes.Count > 0
            ? $"SUBSCRIBER CONNECTED {name}, восстановлены подписки: {string.Join(", ", channel.EventTypes)}"
            : $"SUBSCRIBER CONNECTED {name}, подписок пока нет");

        return channel;
    }

    /// Соединение с подписчиком оборвалось. Broker при этом не завершается и других не трогает.
    public void CloseConnection(SubscriberChannel channel)
    {
        channel.Online = false;
        channel.Send = null;

        if (channel.Reconnectable)
        {
            _journal.Write(BrokerId,
                $"SUBSCRIBER LOST {channel.Name}: подписки сохранены, в очереди {Queue.Count(channel.Name)} событий");
        }
        else
        {
            Subscriptions.Remove(channel.Name);
            Queue.Clear(channel.Name);
            _journal.Write(BrokerId, $"SUBSCRIBER REMOVED {channel.Name}: подписки удалены");
        }
    }

    /// Регистрация подписки. Если для подписчика уже что-то лежит в очереди — сразу отдаём накопленное.
    public Task<SubscriptionResult> SubscribeAsync(SubscriberChannel channel, IEnumerable<string> eventTypes)
    {
        List<string> requested = eventTypes.ToList();
        SubscriptionResult result = Subscriptions.Subscribe(channel.Name, requested);

        _journal.Write(BrokerId, $"SUBSCRIBE {channel.Name} [{(requested.Count > 0 ? string.Join(", ", requested) : "")}] -> {result.Status}: {result.Message}");

        // Очередь сливаем отдельной задачей: доставка уходит по тому же соединению, а читатель
        // на нём один. Ждать ACK внутри обработчика команды нельзя — прочесть его было бы некому.
        if (result.Success)
        {
            _ = Task.Run(() => FlushQueueAsync(channel));
        }

        return Task.FromResult(result);
    }

    public IReadOnlyList<string> Unsubscribe(SubscriberChannel channel)
    {
        IReadOnlyList<string> removed = Subscriptions.UnsubscribeAll(channel.Name);
        _journal.Write(BrokerId, $"UNSUBSCRIBE {channel.Name}: снято {(removed.Count > 0 ? string.Join(", ", removed) : "ничего (подписок не было)")}");
        return removed;
    }

    /// Публикация события. Publisher получает результат маршрутизации сразу,
    /// а доставка каждому подписчику идёт отдельной задачей — это и есть асинхронная обработка.
    public Task<PublishResultMessage> PublishAsync(DomainEvent evt) => PublishAsync(evt, awaitDelivery: false);

    /// Тот же путь, но с ожиданием всех подписчиков — нужно тестам и замеру полного времени обработки.
    public async Task<PublishResultMessage> PublishAsync(DomainEvent evt, bool awaitDelivery)
    {
        var result = new PublishResultMessage { BrokerId = BrokerId };

        if (evt == null || string.IsNullOrWhiteSpace(evt.EventType) || evt.EntityId <= 0)
        {
            _journal.Write(BrokerId, $"INVALID EVENT: тип '{evt?.EventType}' или идентификатор '{evt?.EntityId}' некорректны");
            result.Status = "INVALID_EVENT";
            result.Message = "Некорректное событие: не задан тип или идентификатор объекта";
            return result;
        }

        _journal.Write(BrokerId, $"EVENT RECEIVED {evt.EventType} #{evt.EntityId} (event {evt.EventId}, client {evt.ClientId})");

        if (!EventTypes.IsKnown(evt.EventType))
        {
            _journal.Write(BrokerId, $"UNKNOWN EVENT TYPE {evt.EventType}");
            result.Status = "UNKNOWN_EVENT_TYPE";
            result.Message = $"Неизвестный тип события: {evt.EventType}";
            return result;
        }

        List<SubscriberChannel> targets = Subscriptions.GetTargets(evt.EventType);
        result.SubscribersForEvent = targets.Count;

        if (targets.Count == 0)
        {
            _journal.Write(BrokerId, $"NO SUBSCRIBERS для {evt.EventType}: публиковать некому, событие отброшено");
            result.Success = true;
            result.Status = "NO_SUBSCRIBERS";
            result.Message = $"Подписчиков на {evt.EventType} нет";
            return result;
        }

        _journal.Write(BrokerId, $"ROUTING {evt.EventType} -> {string.Join(", ", targets.Select(channel => channel.Name))}");

        if (awaitDelivery)
        {
            bool[] delivered = await Task.WhenAll(targets.Select(channel => DeliverOneAsync(channel, evt)));
            result.DeliveredCount = delivered.Count(flag => flag);
            result.FailedCount = targets.Count - result.DeliveredCount;
            result.QueuedCount = targets.Sum(channel => Queue.Count(channel.Name));
        }
        else
        {
            foreach (SubscriberChannel channel in targets)
            {
                _ = Task.Run(() => DeliverOneAsync(channel, evt));
            }
        }

        result.Success = true;
        result.Status = "ACCEPTED";
        result.Message = $"Событие принято, получателей: {targets.Count}";
        return result;
    }

    /// Повторная доставка накопленного: подписчик снова на связи — отдаём ему очередь по порядку.
    public async Task<int> FlushQueueAsync(SubscriberChannel channel)
    {
        int waiting = Queue.Count(channel.Name);

        if (waiting == 0 || !channel.Online)
        {
            return 0;
        }

        _journal.Write(BrokerId, $"FLUSH {channel.Name}: в очереди {waiting} событий");
        return await _delivery.FlushQueueAsync(channel, Queue);
    }

    public BrokerInfoResultMessage GetInfo()
    {
        return new BrokerInfoResultMessage
        {
            Success = true,
            BrokerId = BrokerId,
            Status = "READY",
            Message = "Broker на связи",
            OnlineSubscribers = Subscriptions.Names().Count(name => Subscriptions.Find(name)?.Online == true),
            QueuedEvents = Queue.TotalCount,
            Subscriptions = Subscriptions.DescribeSubscriptions(),
            Statuses = Subscriptions.DescribeStatuses()
        };
    }

    private async Task<bool> DeliverOneAsync(SubscriberChannel channel, DomainEvent evt)
    {
        DeliveryOutcome outcome = await _delivery.DeliverWithRetriesAsync(channel, evt);

        if (outcome.Success)
        {
            return true;
        }

        if (channel.Reconnectable)
        {
            if (Queue.Append(channel.Name, evt))
            {
                _journal.Write(BrokerId,
                    $"QUEUED {evt.EventType} -> {channel.Name}: сохранено в очереди ({Queue.Count(channel.Name)}), ждёт восстановления подписчика");
            }
            else
            {
                _journal.Write(BrokerId,
                    $"QUEUE FULL {evt.EventType} -> {channel.Name}: очередь {EventQueue.PerSubscriberLimit} переполнена, событие отброшено");
            }
        }
        else
        {
            _journal.Write(BrokerId, $"DROPPED {evt.EventType} -> {channel.Name}: подписчик событие не принял");
        }

        return false;
    }
}
